# OmniData — Pipeline Data Engineering E-commerce (Olist)

Pipeline de données complet (ELT) automatisé de bout en bout, basé sur le dataset brésilien Olist.

## Architecture
- **Ingestion & Nettoyage** : PySpark (exécuté dans des conteneurs Docker).
- **Stockage (Lakehouse)** : Architecture Médaillon (Raw ➔ Silver ➔ Gold) au format Parquet.
- **Data Warehouse** : PostgreSQL.
- **Orchestration** : Apache Airflow (Docker Compose).
- **Visualisation** : Power BI (Dashboard Ventes, Livraisons, Clients).

## Comment exécuter le projet
1. Cloner le repo et placer les fichiers CSV dans `data/raw/`.
2. Lancer les conteneurs : `docker compose up -d`
3. Accéder à Airflow : `http://localhost:8080` (login: airflow / mdp: airflow)
4. Activer et déclencher le DAG `omnidata_ecommerce_pipeline`.
5. Ouvrir Power BI et actualiser le rapport.

## Résultats clés
- Nettoyage de 99 441 commandes (détection de 23 anomalies chronologiques).
- Automatisation complète du pipeline avec Airflow.