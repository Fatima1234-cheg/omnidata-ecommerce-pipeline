# Rapport de Qualité et d'Investigation des Données

Ce document résume les contrôles effectués sur le dataset Olist et les règles de nettoyage appliquées.

## 1. Contrôles effectués (Phase 2)
- **Doublons** : Aucun doublon détecté sur les clés primaires des 9 tables.
- **Relations (Clés étrangères)** : Aucune anomalie. Tous les articles, paiements et clients sont rattachés à une commande valide.
- **Valeurs manquantes** : Principalement des dates de livraison manquantes (normal pour les commandes annulées ou en cours) et des commentaires d'avis vides.

## 2. Investigation des anomalies
Lors du contrôle de cohérence des dates, 23 commandes présentaient une anomalie majeure : la date de livraison au client était antérieure à la date de remise au transporteur.
- **Décision** : Ces lignes ne sont pas supprimées. Elles sont conservées mais marquées comme invalides pour ne pas fausser les calculs de temps de livraison.
- **Découverte** : 8 commandes marquées comme "livrées" n'ont aucune date de livraison.

## 3. Transformations et Architecture (Bronze / Silver)
Le pipeline utilise le format Parquet pour des raisons de performance et de compression.
- **Couche Bronze** (data/bronze/orders.parquet) : Données brutes converties en Parquet sans modification métier.
- **Couche Silver** (data/silver/orders.parquet) : Données typées (dates converties) et enrichies avec deux nouvelles colonnes :
  - is_delivery_date_valid : Indique si la chronologie de la commande est logique (Vrai/Faux).
  - delivery_duration_days : Calcule le temps de livraison en jours (uniquement pour les dates valides).

## 4. Statistiques clés
- Commandes totales : 99 441
- Chronologies invalides (exclues des calculs) : 23
- Commandes avec une durée de livraison calculable : 96 453
