import requests
import json
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
    """Garante que a data vá como YYYY-MM-DD para o Postgres"""
    if not date_str or str(date_str).lower() in ["none", "null", ""]: return None
    # Limpa a string (remove horas se houver)
    date_clean = str(date_str).split(" ")[0].replace("-", "/")
    # Tenta vários formatos
    for fmt in ("%d/%m/%Y", "%Y/%m/%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_clean, fmt).strftime("%Y-%m-%d")
        except: continue
    return None

def clean_cpf(cpf_str):
    """Remove pontos e traços. Retorna apenas os 11 dígitos."""
    if not cpf_str or str(cpf_str).lower() in ["none", "null"]: return None
    nums = re.sub(r'\D', '', str(cpf_str))
    return nums if len(nums) >= 11 else None

def get_flexible_fields(contact):
    """Mapeia TUDO o que vier do Pipz (Raiz e Fieldsets)"""
    mapping = {}
    # 1. Campos da Raiz
    for k, v in contact.items():
        if not isinstance(v, (dict, list)):
            mapping[k] = v
            
    # 2. Fieldsets (Suporta se vier como Lista ou Dicionário)
    fs_data = contact.get('fieldsets', {})
    fs_list = fs_data.values() if isinstance(fs_data, dict) else fs_data if isinstance(fs_data, list) else []
    
    for fs in fs_list:
        if isinstance(fs, dict):
            for field in fs.get('fields', []):
                name = field.get('name')
                label = field.get('label')
                val = field.get('value')
                if name: mapping[name] = val
                if label: mapping[label] = val
                # Adiciona versão sem espaços e minúscula para garantir
                if label: mapping[label.lower().strip()] = val
    return mapping

def fetch_pipz(list_id):
    """Busca contatos forçando extra_fields=1 (inteiro)"""
    # Mudamos 'true' para 1, que é o padrão mais aceito pela API v1 do Pipz
    params = {
        "list_id": list_id, 
        "limit": "20", 
        "extra_fields": 1, 
        "api_key": PIPZ_KEY, 
        "api_secret": PIPZ_SECRET
    }
    url = "https://campuscaldeira.pipz.io/api/v1/contact/"
    res = requests.get(url, params=params, headers={"Accept": "application/json"})
    if res.status_code != 200:
        print(f"Erro Pipz {res.status_code}: {res.text}")
        return []
    return res.json().get('objects', [])

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    
    with engine.begin() as conn:
        print("--- CONEXÃO ESTABELECIDA ---")
        
        for list_id in ["141", "144"]:
            contacts = fetch_pipz(list_id)
            print(f"Lista {list_id}: {len(contacts)} contatos.")
            
            for c in contacts:
                f = get_flexible_fields(c)
                
                # DEBUG: No primeiro contato, imprime o JSON completo para vermos onde estão os campos
                if contacts.index(c) == 0:
                    print(f"\n--- ESTRUTURA DO CONTATO (DEBUG LISTA {list_id}) ---")
                    # Isso vai nos mostrar se o CPF está vindo ou não
                    print(json.dumps(c, indent=2)[:1000] + "...") 

                # --- EXTRAÇÃO CPF ---
                # Tentamos todos os nomes técnicos e labels que você forneceu
                raw_cpf = (f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or 
                           f.get("CPF") or f.get("cpf") or f.get("[2025] CPF"))
                cpf_limpo = clean_cpf(raw_cpf)
                final_cpf = cpf_limpo if cpf_limpo else f"ID_{c.get('id')}"
                
                # --- EXTRAÇÃO GÊNERO ---
                # Pega do campo customizado ou do nativo 'gender'
                g_raw = str(f.get('gc_2026_lp2_genero') or f.get('[2025] GÊNERO') or 
                            f.get('[gc 2026] genero') or c.get('gender') or "").lower().strip()
                
                if any(x in g_raw for x in ["homem", "masc", "male"]) or g_raw == "m":
                    genero_final = "Masculino"
                elif any(x in g_raw for x in ["mulher", "fem", "female"]) or g_raw == "f":
                    genero_final = "Feminino"
                else:
                    genero_final = "Outros"

                # --- EXTRAÇÃO DATA E TELEFONE ---
                birth = format_date_to_db(c.get('birthdate') or f.get('Birthdate') or f.get('revisar_data_de_nascimento'))
                tel = c.get('mobile_phone') or c.get('phone') or f.get('telefone')

                # UPSERT PESSOA
                p_res = conn.execute(text("""
                    INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                    VALUES (:cpf, :email, :nome, :birth, :tel)
                    ON CONFLICT (cpf) DO UPDATE SET 
                        email = EXCLUDED.email, nome = EXCLUDED.nome, 
                        telefone = EXCLUDED.telefone, data_nascimento = EXCLUDED.data_nascimento
                    RETURNING id
                """), {
                    "cpf": final_cpf, "email": c.get("email"), "nome": c.get("name"),
                    "birth": birth, "tel": str(tel)[:20] if tel else None
                })
                pessoa_id = p_res.fetchone()[0]

                # LP1
                if list_id == "141":
                    conn.execute(text("""
                        INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, como_ficou_sabendo, data_resposta)
                        VALUES (:p_id, '2026', :sab, NOW())
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": pessoa_id, 
                        "sab": f.get("gc_2026_lp1_origem") or f.get("[2025] Como ficou sabendo do Geração Caldeira?")
                    })

                # LP2
                if list_id == "144":
                    conn.execute(text("""
                        INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha)
                        VALUES (:p_id, '2026', :trilha, :esc, :gen, :etn, :trab)
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": pessoa_id, 
                        "trilha": f.get("gc_2026_lp2_trilha_educacional") or f.get("[2025] TRILHAS 2025"),
                        "esc": f.get("gc_2026_lp2_qual_escola") or f.get("nome_da_escola"),
                        "gen": genero_final,
                        "etn": f.get("gc_2026_lp2_etnia") or f.get("[2025] ETNIA"),
                        "trab": f.get("gc_2026_lp2_voce_trabalha") or f.get("[2025] VOCÊ TRABALHA?")
                    })
        print("--- SUCESSO ---")

if __name__ == "__main__":
    process()