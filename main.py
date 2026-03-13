import requests
import pandas as pd
import time
import os
from sqlalchemy import create_engine, text
from datetime import datetime

# Captura de variáveis de ambiente
PIPZ_KEY = os.getenv("PIPZ_TOKEN")
PIPZ_SECRET = os.getenv("PIPZ_SECRET")
DB_URL = os.getenv("DB_URL")

def format_date_to_db(date_str):
    """Converte DD/MM/YYYY para YYYY-MM-DD"""
    if not date_str or str(date_str).lower() in ["none", "null", ""]: return None
    try:
        # Se vier no formato brasileiro
        return datetime.strptime(str(date_str).split(" ")[0], "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        try:
            # Se já vier no formato ISO
            return datetime.strptime(str(date_str).split(" ")[0], "%Y-%m-%d").strftime("%Y-%m-%d")
        except:
            return None

def fetch_pipz(list_id):
    contacts = []
    # Teste de 20 pessoas conforme solicitado
    params = {
        "list_id": list_id, 
        "limit": "20", 
        "extra_fields": "1", 
        "api_key": PIPZ_KEY, 
        "api_secret": PIPZ_SECRET
    }
    url = "https://campuscaldeira.pipz.io/api/v1/contact/"
    res = requests.get(url, params=params, headers={"Accept": "application/json"})
    if res.status_code == 200:
        return res.json().get('objects', [])
    return []

def get_fields(contact):
    """Extrai campos usando os NOMES TÉCNICOS (Slugs) da API"""
    f_dict = {}
    for fs in contact.get('fieldsets', []):
        for f in fs.get('fields', []):
            # O Pipz usa o campo 'name' para o ID técnico (ex: gc_2026_lp1_cpf)
            name = f.get('name', '')
            f_dict[name] = f.get('value')
    return f_dict

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    
    with engine.begin() as conn: # engine.begin gerencia a transação e evita Deadlocks
        print("Conexão estabelecida com sucesso.")
        
        for list_id in ["141", "144"]:
            contacts = fetch_pipz(list_id)
            print(f"Processando {len(contacts)} contatos da lista {list_id}...")
            
            for c in contacts:
                f = get_fields(c)
                
                # --- MAPEAMENTO PESSOAS (Usando nomes técnicos do Power BI) ---
                # Tentamos todos os nomes de CPF possíveis encontrados nos fieldsets
                cpf = f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or f.get("cpf") or f.get("CPF")
                
                # Se ainda assim for nulo, usamos o ID
                final_cpf = str(cpf) if cpf and str(cpf).lower() != "none" else f"ID_{c.get('id')}"
                
                # Formatação de Data de Nascimento (Postgres precisa de YYYY-MM-DD)
                birth_raw = c.get('birthdate') or f.get('birthdate') or f.get('revisar_data_de_nascimento')
                birth_final = format_date_to_db(birth_raw)

                # Telefone
                tel = c.get('mobile_phone') or c.get('phone') or f.get('telefone') or f.get('gc_labs_fone')

                # 1. UPSERT PESSOA
                p_res = conn.execute(text("""
                    INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                    VALUES (:cpf, :email, :nome, :birth, :tel)
                    ON CONFLICT (cpf) DO UPDATE SET 
                        email = EXCLUDED.email, 
                        nome = EXCLUDED.nome, 
                        telefone = EXCLUDED.telefone,
                        data_nascimento = EXCLUDED.data_nascimento
                    RETURNING id
                """), {
                    "cpf": final_cpf[:14], 
                    "email": c.get("email"), 
                    "nome": c.get("name"),
                    "birth": birth_final, 
                    "tel": str(tel)[:20] if tel else None
                })
                pessoa_id = p_res.fetchone()[0]

                # 2. LP1 (Lista 141)
                if list_id == "141":
                    # Usando os slugs de origem e sabendo
                    sabendo = f.get("gc_2026_lp1_origem") or f.get("gc_2026_lp1_como_ficou_sabendo")
                    conn.execute(text("""
                        INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, data_resposta)
                        VALUES (:p_id, '2026', :est, :cid, :sabendo, NOW())
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": pessoa_id, 
                        "est": c.get("state") or f.get("gc_2026_lp1_estado"),
                        "cid": c.get("city") or f.get("gc2026_lp1_cidades"),
                        "sabendo": sabendo
                    })

                # 3. LP2 (Lista 144)
                if list_id == "144":
                    # Lógica de Gênero igual ao Power BI
                    g_raw = str(f.get('gc_2026_lp2_genero') or f.get('gc_2026_genero') or c.get('gender') or "").lower()
                    if "homem" in g_raw or "masc" in g_raw or g_raw == "h": genero = "Masculino"
                    elif "mulher" in g_raw or "fem" in g_raw or g_raw == "f": genero = "Feminino"
                    else: genero = "Outros"

                    conn.execute(text("""
                        INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha)
                        VALUES (:p_id, '2026', :trilha, :escola, :genero, :etnia, :trabalha)
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": pessoa_id, 
                        "trilha": f.get("gc_2026_lp2_trilha_educacional"),
                        "escola": f.get("gc_2026_lp2_qual_escola") or f.get("nome_da_escola"),
                        "genero": genero, 
                        "etnia": f.get("gc_2026_lp2_etnia") or f.get("gc_2026_lp2_qual_etnia"),
                        "trabalha": f.get("gc_2026_lp2_voce_trabalha") or f.get("_gc_2026_lp2_voc_trabalha_em_alguma_empresa")
                    })

if __name__ == "__main__":
    process()