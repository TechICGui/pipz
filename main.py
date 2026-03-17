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

def format_date_to_db(date_str):
    if not date_str or str(date_str).lower() in ["none", "null", ""]: return None
    clean = str(date_str)[:10].replace("/", "-")
    try:
        return datetime.strptime(clean, "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        try: return datetime.strptime(clean, "%d-%m-%Y").strftime("%Y-%m-%d")
        except: return None

def format_timestamp(ts_str):
    if not ts_str: return None
    try: return datetime.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S")
    except: return None

def normalize_genero(lp2, g2026, root):
    val = lp2 or g2026 or root
    if not val: return "Não informado"
    txt = str(val).lower().strip()
    if txt.startswith(('m', 'f', 'mu')) or "mulher" in txt or "fem" in txt:
        return "Feminino"
    if txt.startswith(('h', 'mas')) or "homem" in txt:
        return "Masculino"
    return "Outros"

def normalize_etnia(etnia, qual_etnia):
    texto = (str(etnia or "") + " " + str(qual_etnia or "")).lower()
    if "bran" in texto: return "Branca"
    if "pard" in texto: return "Parda"
    if "pret" in texto or "negr" in texto: return "Preta"
    if "amar" in texto: return "Amarela"
    if "indi" in texto: return "Indígena"
    return "Outra" if texto.strip() else None

def extract_fields_logic(contact_full):
    if not contact_full: return {}
    data = {}
    # Lógica antiga de pessoas: captura raiz e depois fieldsets
    for k, v in contact_full.items():
        if not isinstance(v, (dict, list)):
            data[k] = v
    for fs in contact_full.get('fieldsets', []):
        for field in fs.get('fields', []):
            name, label, val = field.get('name'), field.get('label'), field.get('value')
            if name: data[name] = val
            if label: data[label] = val
    return data

def get_contact_detail(contact_id):
    url = f"https://campuscaldeira.pipz.io/api/v1/contact/{contact_id}/"
    params = {"extra_fields": "1", "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET}
    for _ in range(3):
        res = requests.get(url, params=params, headers={"Accept": "application/json"})
        if res.status_code == 200: return res.json()
        elif res.status_code == 429:
            print("Pipz ocupado... esperando 5 segundos")
            time.sleep(5)
    return None

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        print(f"--- INICIANDO SINCRONIZAÇÃO COMPLETA: {datetime.now()} ---")
        
        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            offset = 0
            limit = 100
            
            while True: # LOOP ATÉ ACABAREM OS DADOS
                print(f"Buscando lista {handler} (Offset: {offset})...")
                url = "https://campuscaldeira.pipz.io/api/v1/contact/"
                params = {
                    "list_id": list_id, 
                    "limit": limit, 
                    "offset": offset,
                    "api_key": PIPZ_KEY, 
                    "api_secret": PIPZ_SECRET
                }
                
                res = requests.get(url, params=params)
                if res.status_code != 200: break
                
                batch = res.json().get('objects', [])
                if not batch: break # Se a página vier vazia, para o loop
                
                for summary in batch:
                    detail = get_contact_detail(summary['id'])
                    f = extract_fields_logic(detail)
                    if not f: continue

                    # PESSOA (Código antigo estável)
                    raw_cpf = f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or f.get("CPF") or f.get("cpf")
                    nums_cpf = re.sub(r'\D', '', str(raw_cpf)) if raw_cpf else None
                    final_cpf = nums_cpf if nums_cpf and len(nums_cpf) >= 11 else f"ID_{f.get('id', 'N/A')}"
                    birth = format_date_to_db(f.get('birthdate') or f.get('birthday'))
                    tel = f.get('mobile_phone') or f.get('phone') or f.get('telefone')
                    dt_cad = format_timestamp(f.get('creation_date'))

                    with conn.begin():
                        try:
                            # UPSERT PESSOA
                            conn.execute(text("""
                                INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                                VALUES (:cpf, :email, :nome, :birth, :tel)
                                ON CONFLICT (cpf) DO UPDATE SET 
                                    email = COALESCE(EXCLUDED.email, form_gc.pessoas.email),
                                    nome = COALESCE(EXCLUDED.nome, form_gc.pessoas.nome),
                                    data_nascimento = COALESCE(EXCLUDED.data_nascimento, form_gc.pessoas.data_nascimento),
                                    telefone = COALESCE(EXCLUDED.telefone, form_gc.pessoas.telefone)
                            """), {"cpf": final_cpf, "email": f.get('email'), "nome": f.get('name'), "birth": birth, "tel": tel})

                            if handler == "lp1":
                                sabendo = f.get("gc_2026_lp1_origem") or f.get("[GC 2026 LP1] Origem") or f.get("[2025] Como ficou sabendo do Geração Caldeira?")
                                alumni = f.get("gc_2026_codigo_alumni") or f.get("gc2026_codigo_alumni") or f.get("[GC2026] codigo alumni")
                                conn.execute(text("""
                                    INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_cadastro, data_resposta)
                                    VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :est, :cid, :sab, :cod, :dt, NOW())
                                    ON CONFLICT (pessoa_id, edicao) DO UPDATE SET como_ficou_sabendo = EXCLUDED.como_ficou_sabendo, codigo_indicacao = EXCLUDED.codigo_indicacao
                                """), {"cpf": final_cpf, "est": f.get('state'), "cid": f.get('city_name'), "sab": sabendo, "cod": alumni, "dt": dt_cad})

                            if handler == "lp2":
                                gen = normalize_genero(f.get("gc_2026_lp2_genero"), f.get("gc_2026_genero"), f.get('gender'))
                                etn = normalize_etnia(f.get("gc_2026_lp2_etnia"), f.get("gc_2026_lp2_qual_etnia"))
                                trab_lp2 = str(f.get("gc_2026_lp2_voce_trabalha") or "").lower()
                                trab_emp = str(f.get("_gc_2026_lp2_voc_trabalha_em_alguma_empresa") or "").lower()
                                tra = "Sim" if "sim" in trab_lp2 else "Não" if "n" in trab_lp2 else ("Não" if "n" in trab_emp or trab_emp == "" else "Sim")

                                conn.execute(text("""
                                    INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha, data_cadastro)
                                    VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :tri, :esc, :gen, :etn, :tra, :dt)
                                    ON CONFLICT (pessoa_id, edicao) DO UPDATE SET trilha = EXCLUDED.trilha, genero = EXCLUDED.genero, etnia = EXCLUDED.etnia, trabalha = EXCLUDED.trabalha
                                """), {
                                    "cpf": final_cpf, "tri": f.get("gc_2026_lp2_trilha_educacional") or f.get("[GC 2026 LP2] trilha educacional"),
                                    "esc": f.get("gc_2026_lp2_qual_escola") or f.get("Nome da escola"),
                                    "gen": gen, "etn": etn, "tra": tra, "dt": dt_cad
                                })
                        except Exception as e:
                            print(f"Erro ID {f.get('id', 'N/A')}: {e}")
                
                offset += limit # Vai para a próxima página
                time.sleep(1) # Pausa de 1s para não sobrecarregar a API
                
        print("--- SYNC FINALIZADO ---")

if __name__ == "__main__":
    process()