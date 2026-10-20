import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import os
os.environ["PGCLIENTENCODING"] = "UTF8"
# Connexion à la base
# Remplace le 5432 par 5433
DB_URL = "postgresql+psycopg://admin:admin123@localhost:5433/omnidata"
engine = create_engine(
    DB_URL, 
    connect_args={"client_encoding": "utf8"}
)

GOLD_DIR = Path("data/gold")

# On cherche tous les dossiers se terminant par .parquet
for folder in GOLD_DIR.iterdir():
    if folder.is_dir() and folder.name.endswith(".parquet"):
        # Le nom de la table sera le nom du dossier sans ".parquet"
        table_name = folder.name.replace(".parquet", "")
        
        print(f"Chargement de {table_name} dans PostgreSQL...")
        
        # Lecture du Parquet avec Pandas
        df = pd.read_parquet(folder)
        
        # Envoi vers PostgreSQL
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        
        print(f"✅ Table '{table_name}' chargée ({len(df)} lignes) !")

print("\n Toutes les tables Gold sont dans PostgreSQL !")