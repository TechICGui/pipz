import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")

def fix():
    engine = create_engine(DB_URL)
    df = pd.read_csv('lp1 - codigo alumni.csv')
    
    # Filtra apenas quem tem algum valor preenchido
    df = df[df['contact_custom_gc2026_codigo_alumni'].notna()]
    
    print(f"🚀 Iniciando a correção inteligente de {len(df)} registros...")
    
    with engine.connect() as conn:
        with conn.begin():
            # OPCIONAL: Aumenta o limite da coluna via código para evitar erros
            conn.execute(text("ALTER TABLE form_gc.lp1_respostas ALTER COLUMN codigo_indicacao TYPE VARCHAR(500);"))
            
            for i, row in df.iterrows():
                email = str(row['email']).strip().lower()
                cupom_bruto = str(row['contact_custom_gc2026_codigo_alumni']).strip()
                
                # --- LÓGICA DE LIMPEZA ---
                # 1. Se for uma frase longa (mais de 30 caracteres) ou tiver muitos espaços, 
                #    provavelmente não é um cupom. Vamos ignorar para não sujar o dashboard.
                if len(cupom_bruto) > 30 or " " in cupom_bruto:
                    continue # Pula para o próximo
                
                # 2. Se for um cupom curto, fazemos o update
                conn.execute(text("""
                    UPDATE form_gc.lp1_respostas 
                    SET codigo_indicacao = :cupom 
                    WHERE pessoa_id IN (SELECT id FROM form_gc.pessoas WHERE LOWER(email) = :email)
                    AND (codigo_indicacao IS NULL OR codigo_indicacao = '')
                """), {"cupom": cupom_bruto.upper(), "email": email})
                
                if i % 500 == 0:
                    print(f"Progresso: {i} registros analisados...")
        
    print("✅ Sucesso! O banco foi atualizado e os 'textos longos' foram filtrados.")

if __name__ == "__main__":
    fix()