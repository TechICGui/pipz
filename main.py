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
    if txt.startswith(('m', 'f', 'mu')) or "mulher" in txt or "fem" in txt: return "Feminino"
    if txt.startswith(('h', 'mas')) or "homem" in txt: return "Masculino"
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
    for k, v in contact_full.items():
        if not isinstance(v, (dict, list)): data[k] = v
    
    # O Pipz às vezes envia fieldsets como lista ou dicionário
    fs_raw = contact_full.get('fieldsets', [])
    fs_list = fs_raw.values() if isinstance(fs_raw, dict) else fs_raw
    
    for fs in fs_list:
        for field in fs.get('fields', []):
            name, label, val = field.get('name'), field.get('label'), field.get('value')
            if name: data[name] = val
            if label: data[label] = val
    return data

def get_contact_detail(contact_id):
    """Busca detalhe com espera progressiva em caso de erro 429"""
    url = f"https://campuscaldeira.pipz.io/api/v1/contact/{contact_id}/"
    params = {"extra_fields": "1", "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET}
    
    wait_time = 5
    for attempt in range(5):
        res = requests.get(url, params=params, headers={"Accept": "application/json"})
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 429:
            print(f"Pipz saturado (429). Tentativa {attempt+1}. Esperando {wait_time}s...")
            time.sleep(wait_time)
            wait_time *= 2 # Espera dobrada a cada falha
        else:
            return None
    return None

def check_if_exists(conn, table, p_id):
    """Verifica se o registro já existe para pular a chamada de API pesada"""
    query = text(f"SELECT 1 FROM form_gc.{table} WHERE pessoa_id = :p_id AND edicao = '2026' LIMIT 1")
    return conn.execute(query, {"p_id": p_id}).fetchone() is not None

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        print(f"--- SINCRONIZAÇÃO INICIADA: {datetime.now()} ---")
        
        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            offset = 0
            limit = 250 # Máximo permitido pela maioria das APIs
            table = "lp1_respostas" if handler == "lp1" else "lp2_respostas"
            
            while True:
                print(f"[{handler}] Lendo página (Offset {offset})...")
                url = "https://campuscaldeira.pipz.io/api/v1/contact/"
                params = {"list_id": list_id, "limit": limit, "offset": offset, "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET}
                
                res = requests.get(url, params=params)
                if res.status_code != 200: break
                
                batch = res.json().get('objects', [])
                if not batch: break
                
                for summary in batch:
                    # LÓGICA INCREMENTAL: Se já temos a resposta dessa pessoa no banco,
                    # e estamos em uma carga total, podemos pular a chamada de detalhe (ID)
                    # No entanto, se for a atualização de 20min, queremos processar apenas os primeiros.
                    
                    # Passo 1: Pegar ID Interno do Banco (ou criar pessoa)
                    # Para isso precisamos do detalhe se o CPF não estiver no summary
                    detail = get_contact_detail(summary['id'])
                    if not detail: continue
                    f = extract_fields_logic(detail)

                    raw_cpf = f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or f.get("CPF") or f.get("cpf")
                    nums_cpf = re.sub(r'\D', '', str(raw_cpf)) if raw_cpf else None
                    final_cpf = nums_cpf if nums_cpf and len(nums_cpf) >= 11 else f"ID_{f['id']}"
                    
                    with conn.begin():
                        try:
                            # Upsert Pessoa
                            p_res = conn.execute(text("""
                                INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                                VALUES (:cpf, :email, :nome, :birth, :tel)
                                ON CONFLICT (cpf) DO UPDATE SET 
                                    email = EXCLUDED.email, nome = EXCLUDED.nome,
                                    data_nascimento = COALESCE(EXCLUDED.data_nascimento, form_gc.pessoas.data_nascimento),
                                    telefone = COALESCE(EXCLUDED.telefone, form_gc.pessoas.telefone)
                                RETURNING id
                            """), {
                                "cpf": final_cpf, "email": f['email'], "nome": f['name'], 
                                "birth": format_date_to_db(f.get('birthdate') or f.get('birthday')), 
                                "tel": f.get('mobile_phone') or f.get('phone')
                            })
                            pessoa_id = p_res.fetchone()[0]

                            # Upsert Respostas
                            if handler == "lp1":
                                sabendo = f.get("[2025] Como ficou sabendo do Geração Caldeira?") or f.get("gc_2026_lp1_origem")
                                alumni = f.get("gc_2026_codigo_alumni") or f.get("[GC2026] codigo alumni")
                                conn.execute(text("""
                                    INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_cadastro, data_resposta)
                                    VALUES (:p_id, '2026', :est, :cid, :sab, :cod, :dt, NOW())
                                    ON CONFLICT (pessoa_id, edicao) DO UPDATE SET como_ficou_sabendo = EXCLUDED.como_ficou_sabendo
                                """), {"p_id": pessoa_id, "est": f.get('state'), "cid": f.get('city_name'), "sab": sabendo, "cod": alumni, "dt": format_timestamp(f.get('creation_date'))})

                            if handler == "lp2":
                                gen = normalize_genero(f.get("gc_2026_lp2_genero"), f.get("gc_2026_genero"), f.get('gender'))
                                etn = normalize_etnia(f.get("gc_2026_lp2_etnia"), f.get("gc_2026_lp2_qual_etnia"))
                                conn.execute(text("""
                                    INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha, data_cadastro)
                                    VALUES (:p_id, '2026', :tri, :esc, :gen, :etn, :tra, :dt)
                                    ON CONFLICT (pessoa_id, edicao) DO UPDATE SET trilha = EXCLUDED.trilha, genero = EXCLUDED.genero, etnia = EXCLUDED.etnia
                                """), {
                                    "p_id": pessoa_id, "tri": f.get("gc_2026_lp2_trilha_educacional"),
                                    "esc": f.get("gc_2026_lp2_qual_escola") or f.get("Nome da escola"),
                                    "gen": gen, "etn": etn, "tra": "Sim" if "sim" in str(f.get("gc_2026_lp2_voce_trabalha") or "").lower() else "Não",
                                    "dt": format_timestamp(f.get('creation_date'))
                                })
                        except Exception as e:
                            print(f"Erro no contato {f.get('id')}: {e}")
                
                offset += limit
                # Se estivermos atualizando a cada 20 min, não precisamos ler 40k registros toda vez.
                # Podemos parar se o offset for maior que 500 (pegando as últimas 500 mudanças).
                # Para a CARGA INICIAL, comente as duas linhas abaixo:
                # if offset >= 1000:
                #    break

        print("--- SYNC FINALIZADO COM SUCESSO ---")

if __name__ == "__main__":
    process()