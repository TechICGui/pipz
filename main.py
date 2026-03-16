import requests
import json
import time
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import re

# Configurações de ambiente
PIPZ_KEY = os.getenv("PIPZ_TOKEN")
PIPZ_SECRET = os.getenv("PIPZ_SECRET")
DB_URL = os.getenv("DB_URL")

def normalize_key(key):
    """Limpeza profunda de chaves: sem acento, sem espaços extras, minúsculo"""
    if not key: return ""
    key = str(key).lower().strip()
    key = re.sub(r'[àáâãäå]', 'a', key)
    key = re.sub(r'[èéêë]', 'e', key)
    key = re.sub(r'[ìíîï]', 'i', key)
    key = re.sub(r'[òóôõö]', 'o', key)
    key = re.sub(r'[ùúûü]', 'u', key)
    key = re.sub(r'[ç]', 'c', key)
    key = re.sub(r'[^a-z0-9]', ' ', key) # Substitui símbolos por espaço
    return " ".join(key.split()) # Remove espaços duplos

def normalize_genero(val):
    """Tratamento rigoroso de gênero"""
    if not val or str(val).lower() in ["none", "null", ""]:
        return "Não informado"
    
    txt = str(val).lower().strip()
    if txt.startswith(('h', 'mas')): return "Masculino"
    if txt.startswith(('m', 'fem', 'mu')): return "Feminino"
    return "Outros"

def normalize_etnia(f_dict):
    """Busca etnia em múltiplos campos e padroniza"""
    # Combina os valores de todos os campos prováveis de etnia
    campos = [
        "gc 2026 lp2 etnia", "gc 2026 lp2 qual etnia", "etnia", 
        "2025 etnia", "qual etnia", "gc 2026 etnia"
    ]
    vals = [str(f_dict.get(normalize_key(c)) or "") for c in campos]
    texto = " ".join(vals).lower()
    
    if "bran" in texto: return "Branca"
    if "pard" in texto: return "Parda"
    if "pret" in texto or "negr" in texto: return "Preta"
    if "amar" in texto: return "Amarela"
    if "indi" in texto: return "Indígena"
    return None

def format_date_to_db(date_str):
    if not date_str or str(date_str).lower() in ["none", "null", ""]: return None
    clean = str(date_str).replace("T", " ").replace("Z", "").split(" ")[0].replace("-", "/")
    for fmt in ("%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(clean, fmt).strftime("%Y-%m-%d")
        except: continue
    return None

def extract_all_fields(contact):
    """Extrai campos da raiz e dos fieldsets usando chaves normalizadas"""
    data = {}
    # 1. Raiz
    for k, v in contact.items():
        if not isinstance(v, (dict, list)):
            data[k] = v
            data[normalize_key(k)] = v
    # 2. Fieldsets
    fs_data = contact.get('fieldsets', {})
    fs_list = fs_data.values() if isinstance(fs_data, dict) else fs_data if isinstance(fs_data, list) else []
    for fs in fs_list:
        if isinstance(fs, dict):
            for field in fs.get('fields', []):
                label = field.get('label', '')
                name = field.get('name', '')
                val = field.get('value')
                if label: data[normalize_key(label)] = val
                if name: data[normalize_key(name)] = val
                if name: data[name] = val # Mantém o nome técnico original
    return data

def fetch_pipz_page(list_id, offset=0):
    url = "https://campuscaldeira.pipz.io/api/v1/contact/"
    params = {
        "list_id": list_id, "limit": 100, "offset": offset,
        "extra_fields": 1, "include_fieldsets": 1,
        "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET
    }
    try:
        res = requests.get(url, params=params, timeout=30)
        if res.status_code == 200: return res.json().get('objects', [])
    except: pass
    return []

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    
    with engine.connect() as conn:
        print("--- INICIANDO CONEXÃO ---")
        
        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            offset = 0
            while offset < 1000: # Limite de 1000 contatos por execução para segurança
                contacts = fetch_pipz_page(list_id, offset)
                if not contacts: break
                
                print(f"Lista {list_id}: Processando {len(contacts)} contatos (Offset: {offset})")
                
                for c in contacts:
                    f = extract_all_fields(c)
                    
                    # --- PESSOA ---
                    raw_cpf = f.get(normalize_key("CPF")) or f.get(normalize_key("[2025] CPF")) or f.get("gc_2026_lp1_cpf")
                    final_cpf = re.sub(r'\D', '', str(raw_cpf)) if raw_cpf else None
                    if not final_cpf or len(final_cpf) < 11: final_cpf = f"ID_{c.get('id')}"
                    
                    birth = format_date_to_db(c.get('birthdate') or f.get(normalize_key('revisar data de nascimento')))
                    tel = c.get('mobile_phone') or c.get('phone') or f.get(normalize_key('telefone'))

                    trans = conn.begin()
                    try:
                        conn.execute(text("""
                            INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                            VALUES (:cpf, :email, :nome, :birth, :tel)
                            ON CONFLICT (cpf) DO UPDATE SET 
                                email = COALESCE(EXCLUDED.email, form_gc.pessoas.email),
                                nome = COALESCE(EXCLUDED.nome, form_gc.pessoas.nome)
                        """), {"cpf": final_cpf, "email": c.get("email"), "nome": c.get("name"), "birth": birth, "tel": str(tel)[:20] if tel else None})
                        
                        # --- LP1 ---
                        if handler == "lp1":
                            sabendo = (f.get(normalize_key("[GC 2026 LP1] Origem")) or 
                                       f.get(normalize_key("gc 2026 lp1 origem")) or 
                                       f.get(normalize_key("[2025] Como ficou sabendo do Geração Caldeira?")) or
                                       f.get(normalize_key("como ficou sabendo")))
                            
                            alumni = f.get(normalize_key("gc 2026 codigo alumni")) or f.get(normalize_key("[2025] CUPOM GC 2025"))
                            
                            conn.execute(text("""
                                INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_resposta)
                                VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :est, :cid, :sab, :cod, NOW())
                                ON CONFLICT DO NOTHING
                            """), {
                                "cpf": final_cpf, "est": c.get("state"), "cid": c.get("city_name"), 
                                "sab": sabendo, "cod": alumni
                            })

                        # --- LP2 ---
                        if handler == "lp2":
                            # Gênero com fallback para root e "Não informado"
                            gen_val = f.get(normalize_key("[GC 2026 LP2] Genero")) or f.get(normalize_key("[GC 2026] Genero")) or c.get('gender')
                            genero = normalize_genero(gen_val)
                            
                            # Etnia Padronizada
                            etnia = normalize_etnia(f)

                            conn.execute(text("""
                                INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha, data_cadastro)
                                VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :tri, :esc, :gen, :etn, :tra, :dt)
                                ON CONFLICT DO NOTHING
                            """), {
                                "cpf": final_cpf, 
                                "tri": f.get(normalize_key("[GC 2026 LP2] trilha educacional")),
                                "esc": f.get(normalize_key("[GC 2026 LP2] qual escola")) or f.get(normalize_key("Nome da escola")),
                                "gen": genero, "etn": etnia, "tra": f.get(normalize_key("[GC 2026 LP2] voce trabalha")),
                                "dt": format_date_to_db(c.get('creation_date'))
                            })
                        trans.commit()
                    except Exception as e:
                        trans.rollback()
                        print(f"Erro ID {c.get('id')}: {e}")
                
                if len(contacts) < 100: break
                offset += 100
                time.sleep(1)
        print("--- PROCESSO FINALIZADO ---")

if __name__ == "__main__":
    process()