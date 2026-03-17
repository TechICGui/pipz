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
    try: return datetime.strptime(clean, "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        try: return datetime.strptime(clean, "%d-%m-%Y").strftime("%Y-%m-%d")
        except: return None

def format_timestamp(ts_str):
    if not ts_str: return None
    try: return datetime.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S")
    except: return None

def get_deep_field(data_dict, search_term):
    """Procura em todas as chaves por um termo (ex: alumni)"""
    search_term = search_term.lower()
    for k, v in data_dict.items():
        if search_term in k.lower() and v:
            return v
    return None

def extract_fields(obj):
    """Extrai campos da raiz e achata os fieldsets se existirem"""
    res = {}
    # 1. Campos raiz
    for k, v in obj.items():
        if not isinstance(v, (dict, list)): res[k] = v
    
    # 2. Fieldsets (Podem vir como lista ou dict)
    fs_raw = obj.get('fieldsets', [])
    fs_list = fs_raw.values() if isinstance(fs_raw, dict) else fs_raw
    for fs in fs_list:
        for field in fs.get('fields', []):
            name, label, val = field.get('name'), field.get('label'), field.get('value')
            if name: res[name] = val
            if label: res[label] = val
    return res

def get_detail_with_retry(contact_id):
    """Busca detalhe com espera inteligente para não ser bloqueado"""
    url = f"https://campuscaldeira.pipz.io/api/v1/contact/{contact_id}/"
    params = {"extra_fields": "1", "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET}
    
    wait = 5
    for i in range(4): # 4 tentativas
        res = requests.get(url, params=params, timeout=20)
        if res.status_code == 200: return res.json()
        if res.status_code == 429:
            print(f"  Rate Limit! Dormindo {wait}s...")
            time.sleep(wait)
            wait *= 2 # Dobra o tempo
        else: break
    return None

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        print(f"--- INICIANDO SYNC TURBO: {datetime.now().strftime('%H:%M:%S')} ---")
        
        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            offset = 0
            # Aumentamos o limite da lista para o máximo (500) para fazer menos pedidos de lista
            limit = 500 
            
            while True:
                url = "https://campuscaldeira.pipz.io/api/v1/contact/"
                params = {"list_id": list_id, "limit": limit, "offset": offset, "include_fieldsets": "1", "extra_fields": "1", "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET}
                
                resp = requests.get(url, params=params)
                if resp.status_code != 200: break
                
                batch = resp.json().get('objects', [])
                if not batch: break
                
                print(f"[{handler}] Offset {offset}: Processando {len(batch)} contatos...")
                
                for item in batch:
                    # Tenta extrair da lista primeiro
                    f = extract_fields(item)
                    
                    # Se faltar o essencial (CPF ou código), chama o detalhe
                    # Isso economiza MUITA API se o Pipz mandar o campo na lista
                    if not f.get('gc_2026_lp1_cpf') and not f.get('CPF'):
                        detail = get_detail_with_retry(item['id'])
                        if detail: f = extract_fields(detail)

                    # --- LÓGICA DE DADOS ---
                    raw_cpf = f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or f.get("CPF") or f.get("cpf")
                    final_cpf = re.sub(r'\D', '', str(raw_cpf)) if raw_cpf else None
                    if not final_cpf or len(final_cpf) < 11: final_cpf = f"ID_{item['id']}"

                    with conn.begin():
                        try:
                            # PESSOAS
                            conn.execute(text("""
                                INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                                VALUES (:cpf, :email, :nome, :birth, :tel)
                                ON CONFLICT (cpf) DO UPDATE SET 
                                    email = EXCLUDED.email, nome = EXCLUDED.nome,
                                    data_nascimento = COALESCE(EXCLUDED.data_nascimento, form_gc.pessoas.data_nascimento),
                                    telefone = COALESCE(EXCLUDED.telefone, form_gc.pessoas.telefone)
                            """), {"cpf": final_cpf, "email": f.get('email'), "nome": f.get('name'), 
                                   "birth": format_date_to_db(f.get('birthdate') or f.get('birthday')), 
                                   "tel": f.get('mobile_phone') or f.get('phone') or f.get('telefone')})

                            if handler == "lp1":
                                # BUSCA DINÂMICA DE ALUMNI
                                alumni = f.get("gc2026_codigo_alumni") or f.get("gc_2026_codigo_alumni") or get_deep_field(f, "alumni")
                                sabendo = f.get("gc_2026_lp1_origem") or f.get("[GC 2026 LP1] Origem") or get_deep_field(f, "sabendo")
                                
                                conn.execute(text("""
                                    INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_cadastro, data_resposta)
                                    VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :est, :cid, :sab, :cod, :dt, NOW())
                                    ON CONFLICT (pessoa_id, edicao) DO UPDATE SET 
                                        como_ficou_sabendo = COALESCE(EXCLUDED.como_ficou_sabendo, form_gc.lp1_respostas.como_ficou_sabendo),
                                        codigo_indicacao = COALESCE(EXCLUDED.codigo_indicacao, form_gc.lp1_respostas.codigo_indicacao)
                                """), {"cpf": final_cpf, "est": f.get('state'), "cid": f.get('city_name'), "sab": sabendo, "cod": alumni, "dt": format_timestamp(f.get('creation_date'))})

                            if handler == "lp2":
                                trab = "Sim" if "sim" in str(f.get("gc_2026_lp2_voce_trabalha") or "").lower() else "Não"
                                conn.execute(text("""
                                    INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha, data_cadastro)
                                    VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :tri, :esc, :gen, :etn, :tra, :dt)
                                    ON CONFLICT (pessoa_id, edicao) DO UPDATE SET 
                                        trilha = COALESCE(EXCLUDED.trilha, form_gc.lp2_respostas.trilha),
                                        etnia = COALESCE(EXCLUDED.etnia, form_gc.lp2_respostas.etnia)
                                """), {
                                    "cpf": final_cpf, "tri": f.get("gc_2026_lp2_trilha_educacional") or f.get("[GC 2026 LP2] trilha educacional"),
                                    "esc": f.get("gc_2026_lp2_qual_escola") or f.get("Nome da escola"),
                                    "gen": f.get("genero") or "Não informado", "etn": f.get("etnia"), "tra": trab, "dt": format_timestamp(f.get('creation_date'))
                                })
                        except Exception as e:
                            print(f"  Erro no ID {item['id']}: {str(e)[:50]}")
                
                offset += limit
                time.sleep(0.5) # Pausa mínima para respirar entre batches
                
        print("--- SYNC FINALIZADO ---")

if __name__ == "__main__":
    process()