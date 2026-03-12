import requests
import pandas as pd
import time
import os
from sqlalchemy import create_engine, text
from datetime import datetime

# Credenciais dos Secrets
PIPZ_TOKEN = os.getenv("PIPZ_TOKEN")
DB_URL = os.getenv("DB_URL") 
engine = create_engine(DB_URL)

def fetch_pipz(list_id):
    contacts = []
    offset = 0
    limit = 1000
    while True:
        url = f"https://campuscaldeira.pipz.io/api/v1/contact/?list_id={list_id}&limit={limit}&offset={offset}&extra_fields=1"
        res = requests.get(url, headers={"Authorization": f"Bearer {PIPZ_TOKEN}", "Accept": "application/json"})
        if res.status_code == 429:
            time.sleep(15)
            continue
        data = res.json()
        objs = data.get('objects', [])
        if not objs: break
        contacts.extend(objs)
        offset += limit
        if len(objs) < limit: break
    return contacts

def get_fields(c):
    f = {}
    for fs in c.get('fieldsets', []):
        for field in fs.get('fields', []):
            f[field['name']] = field.get('value')
    return f

def process():
    print("Conectando ao Pipz...")
    for list_id in ["141", "144"]:
        contacts = fetch_pipz(list_id)
        print(f"Processando {len(contacts)} contatos da lista {list_id}...")
        
        with engine.begin() as conn:
            for c in contacts:
                f = get_fields(c)
                
                # Regra do CPF (Sendo o seu UNIQUE NOT NULL)
                # Se não existir, geramos um ID fake baseado no ID do Pipz para não travar o banco
                cpf = f.get("gc_2026_lp2_cpf") or f.get("gc_2026_lp1_cpf") or f.get("cpf")
                if not cpf: cpf = f"ID_{c.get('id')}"

                # 1. UPSERT Pessoa
                p_res = conn.execute(text("""
                    INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                    VALUES (:cpf, :email, :nome, :birth, :tel)
                    ON CONFLICT (cpf) DO UPDATE SET email = EXCLUDED.email, nome = EXCLUDED.nome
                    RETURNING id
                    """), {
                    "cpf": str(cpf)[:14],
                    "email": c.get("email"),
                    "nome": c.get("name"),
                    "birth": c.get("birthday") or f.get("birthdate"),
                    "tel": c.get("phone")
                })
                p_id = p_res.fetchone()[0]

                # 2. LP1 Respostas
                if list_id == "141":
                    conn.execute(text("""
                        INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, data_resposta)
                        VALUES (:p_id, '2026', :est, :cid, :sabendo, NOW())
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": p_id, "est": c.get("state"), "cid": c.get("city_name"),
                        "sabendo": f.get("gc_2026_lp1_como_ficou_sabendo")
                    })

                # 3. LP2 Respostas (Tratamento completo)
                if list_id == "144":
                    # Gênero
                    g_raw = str(f.get('gc_2026_lp2_genero') or c.get('gender') or "").lower()
                    genero = "Masculino" if g_raw.startswith(('h', 'mas')) else "Feminino" if g_raw.startswith(('mu', 'f')) else "Outros"
                    
                    # Etnia
                    e_raw = str(f.get('gc_2026_lp2_etnia') or "").lower()
                    etnia = "Branca" if "bran" in e_raw else "Parda" if "pard" in e_raw else "Preta" if "pret" in e_raw else "Outra"

                    # Trabalho
                    trab_emp = str(f.get('_gc_2026_lp2_voc_trabalha_em_alguma_empresa') or "").lower()
                    trabalha = "Sim" if (f.get('gc_2026_lp2_voce_trabalha') == "Sim" or (trab_emp != "" and not trab_emp.startswith('n') and trab_emp != "null")) else "Não"

                    conn.execute(text("""
                        INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha)
                        VALUES (:p_id, '2026', :trilha, :escola, :genero, :etnia, :trabalha)
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": p_id, "trilha": f.get("gc_2026_lp2_trilha_educacional"),
                        "escola": f.get("gc_2026_lp2_qual_escola"), "genero": genero, 
                        "etnia": etnia, "trabalha": trabalha
                    })

if __name__ == "__main__":
    process()