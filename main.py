import requests
import pandas as pd
import time
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import re

# Captura de variáveis de ambiente
PIPZ_KEY = os.getenv("PIPZ_TOKEN")
PIPZ_SECRET = os.getenv("PIPZ_SECRET")
DB_URL = os.getenv("DB_URL")

def format_date_to_db(date_str):
    if not date_str or str(date_str).lower() in ["none", "null", ""]: return None
    date_str = str(date_str).split(" ")[0].replace("-", "/") # Normaliza separadores
    for fmt in ("%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except: continue
    return None

def clean_cpf(cpf_str):
    if not cpf_str: return None
    return re.sub(r'\D', '', str(cpf_str)) # Remove tudo que não é número

def fetch_pipz(list_id):
    url = f"https://campuscaldeira.pipz.io/api/v1/contact/"
    params = {"list_id": list_id, "limit": "20", "extra_fields": "1", "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET}
    res = requests.get(url, params=params, headers={"Accept": "application/json"})
    return res.json().get('objects', []) if res.status_code == 200 else []

def get_flexible_fields(contact):
    """Cria um dicionário onde a chave é o NOME TÉCNICO e também o LABEL do campo"""
    mapping = {}
    for fs in contact.get('fieldsets', []):
        for f in fs.get('fields', []):
            val = f.get('value')
            if val is not None:
                mapping[f.get('name')] = val
                mapping[f.get('label')] = val # Permite buscar pelo nome bonito que você vê no site
    return mapping

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    
    with engine.begin() as conn:
        print("--- INICIANDO CONEXÃO ---")
        
        for list_id in ["141", "144"]:
            contacts = fetch_pipz(list_id)
            print(f"Lista {list_id}: {len(contacts)} contatos encontrados.")
            
            for c in contacts:
                f = get_flexible_fields(c)
                
                # --- LOG DE DEBUG (Apenas para o primeiro contato de cada lista) ---
                if contacts.index(c) == 0:
                    print(f"\n--- DEBUG DE CAMPOS DISPONÍVEIS (Lista {list_id}) ---")
                    print(list(f.keys()))
                
                # --- EXTRAÇÃO COM MÚLTIPLAS TENTATIVAS (Fallback) ---
                
                # 1. CPF (Tenta labels comuns e slugs)
                raw_cpf = f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or f.get("CPF") or f.get("cpf") or f.get("[2025] CPF")
                cpf_limpo = clean_cpf(raw_cpf)
                final_cpf = cpf_limpo if cpf_limpo and len(cpf_limpo) >= 11 else f"ID_{c.get('id')}"

                # 2. TELEFONE
                tel = c.get('mobile_phone') or c.get('phone') or f.get('telefone') or f.get('Mobile phone') or f.get('Phone')

                # 3. DATA NASCIMENTO
                nasc_raw = c.get('birthdate') or f.get('birthdate') or f.get('Birthdate') or f.get('revisar_data_de_nascimento')
                nasc_final = format_date_to_db(nasc_raw)

                # --- UPSERT PESSOA ---
                p_res = conn.execute(text("""
                    INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                    VALUES (:cpf, :email, :nome, :birth, :tel)
                    ON CONFLICT (cpf) DO UPDATE SET 
                        email = EXCLUDED.email, nome = EXCLUDED.nome, 
                        telefone = EXCLUDED.telefone, data_nascimento = EXCLUDED.data_nascimento
                    RETURNING id
                """), {
                    "cpf": final_cpf, "email": c.get("email"), "nome": c.get("name"),
                    "birth": nasc_final, "tel": str(tel)[:20] if tel else None
                })
                pessoa_id = p_res.fetchone()[0]

                # --- RESPOSTAS LP1 (141) ---
                if list_id == "141":
                    sabendo = f.get("gc_2026_lp1_origem") or f.get("[2025] Como ficou sabendo do Geração Caldeira?") or f.get("Como ficou sabendo do Geração Caldeira?")
                    indicacao = f.get("gc_2026_lp1_codigo_indicacao") or f.get("[2025] CUPOM GC 2025")
                    
                    conn.execute(text("""
                        INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_resposta)
                        VALUES (:p_id, '2026', :est, :cid, :sab, :cod, NOW())
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": pessoa_id, "est": c.get("state"), "cid": c.get("city"),
                        "sab": sabendo, "cod": indicacao
                    })

                # --- RESPOSTAS LP2 (144) ---
                if list_id == "144":
                    g_raw = str(f.get('gc_2026_lp2_genero') or f.get('[2025] GÊNERO') or c.get('gender') or "").lower()
                    if "homem" in g_raw or "masc" in g_raw or "male" in g_raw: genero = "Masculino"
                    elif "mulher" in g_raw or "fem" in g_raw or "female" in g_raw: genero = "Feminino"
                    else: genero = "Outros"

                    conn.execute(text("""
                        INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha)
                        VALUES (:p_id, '2026', :trilha, :esc, :gen, :etn, :trab)
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": pessoa_id, 
                        "trilha": f.get("gc_2026_lp2_trilha_educacional") or f.get("[2025] TRILHAS 2025"),
                        "esc": f.get("gc_2026_lp2_qual_escola") or f.get("Nome da escola") or f.get("[2025] ESCOLA/FACULDADE"),
                        "gen": genero,
                        "etn": f.get("gc_2026_lp2_etnia") or f.get("[2025] ETNIA"),
                        "trab": f.get("gc_2026_lp2_voce_trabalha") or f.get("[2025] VOCÊ TRABALHA?")
                    })
        print("--- PROCESSO FINALIZADO ---")

if __name__ == "__main__":
    process()