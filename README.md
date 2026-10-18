
# OmniData — E-commerce Data Engineering Pipeline

Plateforme d'analyse e-commerce basée sur le dataset public Olist (Brésil).

##  Architecture
- **Ingestion** : Python (contrôle et validation)
- **Lakehouse** : PySpark (Bronze, Silver, Gold)
- **Entrepôt** : PostgreSQL (Modèle en étoile)
- **Orchestration** : Apache Airflow & Docker
- **Visualisation** : Power BI
- **Machine Learning** : Prédiction des retards de livraison

##  Structure du projet
- `data/` : Données brutes et transformées (ignorées par Git)
- `src/` : Code source (ingestion, transformations, analytics, ml)
- `dags/` : Pipelines Airflow
- `sql/` : Requêtes et création des tables

##  Statut
Phase 1 : Initialisation du projet et exploration des données. (Terminée)