# TradeCorp International — Data Engineering Pipeline

Projet de data engineering réalisé avec **Apache Spark / PySpark**, **Docker**, **Azure Data Lake Storage Gen2** et **Apache Airflow**.

L’objectif est de mettre en place un pipeline de données modulaire capable de :

- récupérer les données métier depuis Azure Data Lake Storage Gen2 ;
- nettoyer et transformer les données avec PySpark ;
- joindre les différentes sources métier ;
- enrichir les commandes avec la devise du pays du client ;
- appliquer les taux de change du jour ;
- produire un dataset consolidé au format Parquet ;
- déposer le résultat dans la zone `clean` d’ADLS ;
- automatiser l’exécution quotidienne avec Airflow ;
- valider les transformations avec des tests automatisés.

---

# Architecture générale

Le projet suit une architecture de type :

```
Raw → Read → Transform → Enrich → Clean
```

Le flux général est :

```
Exchange Rate API
        ↓
fetch_exchange_rates.py
        ↓
ADLS raw/
        ↓
reader.py
        ↓
transformer.py
        ↓
enrichment.py
        ↓
writer.py
        ↓
ADLS clean/orders_enriched/
```

---

# Stack technique

## Apache Spark / PySpark

Spark constitue le moteur principal de traitement.

Il est utilisé pour :

- lire les fichiers CSV et Parquet ;
- nettoyer les données ;
- effectuer les jointures ;
- calculer les colonnes métier ;
- enrichir les données ;
- produire les fichiers Parquet.

Spark permet également de conserver une architecture adaptée à des volumes de données plus importants.

## Docker / Docker Compose

Docker fournit un environnement reproductible contenant :

- Spark / PySpark ;
- Jupyter ;
- Airflow ;
- PostgreSQL pour les métadonnées Airflow ;
- pgAdmin.

Les différents composants du projet peuvent ainsi fonctionner sans installation locale complexe de Spark, Java ou Airflow.

## Azure Data Lake Storage Gen2

ADLS constitue le stockage principal du projet.

Deux zones sont utilisées :

```
raw/
clean/
```

La zone `raw` contient les données sources et les fichiers de référence.

La zone `clean` contient le dataset transformé final au format Parquet.

Le projet utilise le SDK Python :

```
azure-storage-blob
```

pour communiquer avec Azure.

## Apache Airflow

Airflow orchestre le pipeline et assure :

- l’ordre d’exécution des tâches ;
- la planification quotidienne ;
- les retries ;
- la centralisation des logs ;
- le suivi de l’état du pipeline depuis une interface web.

## Pytest

Pytest permet de tester les transformations principales du pipeline directement sur des DataFrames PySpark.

---

# Structure du projet

```
tradecorp/
│
├── dags/
│   └── tradecorp_pipeline.py
│
├── data/
│   └── tmp/
│       ├── tradecorp_raw/
│       ├── tradecorp_reference/
│       ├── reader_output/
│       ├── transformer_output/
│       └── tradecorp_clean/
│
├── notebooks/
│
├── src/
│   ├── utils.py
│   ├── reader.py
│   ├── transformer.py
│   ├── enrichment.py
│   ├── writer.py
│   ├── pipeline.py
│   └── fetch_exchange_rates.py
│
├── tests/
│   ├── test_transformers.py
│   └── run_tests.py
│
├── Dockerfile
├── Dockerfile.airflow
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

---

# Données utilisées

Le pipeline travaille avec huit fichiers métier :

```
categories.csv
customers.csv
employees.csv
order_details.csv
orders.csv
products.csv
shippers.csv
suppliers.csv
```

Ils sont stockés dans :

```
ADLS raw/
```

Deux fichiers de référence sont également utilisés :

```
raw/reference/country_currency.csv
raw/reference/exchange_rates.json
```

`country_currency.csv` associe un pays à sa devise.

`exchange_rates.json` contient les taux récupérés depuis :

```
https://api.exchangerate-api.com/v4/latest/USD
```

---

# Rôle des principaux modules

## `src/utils.py`

Contient les fonctions communes du projet :

- connexion à Azure Blob Storage ;
- nettoyage des chaînes ;
- conversion des types ;
- nettoyage des clients ;
- nettoyage des commandes ;
- nettoyage des détails de commande ;
- nettoyage des produits ;
- nettoyage des employés ;
- calcul du sous-total.

Le sous-total est calculé avec :

```
sous_total = prix_unitaire × quantite × (1 - discount)
```

---

## `src/reader.py`

Le reader :

- télécharge les fichiers métier depuis ADLS ;
- télécharge les fichiers de référence ;
- lit les CSV avec Spark ;
- écrit les DataFrames métier dans un stockage intermédiaire partagé.

Sortie intermédiaire :

```
/home/jovyan/data/tmp/reader_output/
```

---

## `src/transformer.py`

Le transformer lit les données produites par le reader puis applique :

- les fonctions de nettoyage ;
- les jointures entre les tables ;
- la sélection du schéma final ;
- l’enrichissement avec les devises et les taux de change.

Le résultat est écrit dans :

```
/home/jovyan/data/tmp/transformer_output/orders_enriched/
```

Le dataset contient notamment :

```
order_id
customer_id
employee_id
product_id
order_date
freight
prix_unitaire
quantite
discount
sous_total
customer_name
customer_country
customer_city
product_name
category_name
full_name
shipper_name
currency
exchange_rate
sous_total_local
```

---

## `src/enrichment.py`

Ce module associe le pays du client à sa devise grâce à :

```
country_currency.csv
```

puis récupère le taux correspondant depuis :

```
exchange_rates.json
```

Le montant local est calculé avec :

```
sous_total_local = sous_total × exchange_rate
```

---

## `src/writer.py`

Le writer :

1. lit le Parquet produit par `transformer.py` ;
2. écrit le résultat final localement ;
3. nettoie l’ancien contenu dans ADLS ;
4. upload le nouveau Parquet dans :

```
clean/orders_enriched/
```

Le nettoyage préalable du dossier distant garantit l’idempotence du pipeline.

---

## `src/fetch_exchange_rates.py`

Ce script récupère les taux de change depuis l’API :

```
https://api.exchangerate-api.com/v4/latest/USD
```

puis enregistre la réponse dans :

```
raw/reference/exchange_rates.json
```

Ce script ne nécessite pas Spark.

---

## `src/pipeline.py`

`pipeline.py` permet de lancer manuellement l’ensemble du traitement hors Airflow.

Dans l’orchestration Airflow, les différentes étapes sont désormais exécutées séparément par les tâches du DAG.

---

# Installation

## Prérequis

Il faut disposer de :

- Docker Desktop ;
- Docker Compose ;
- Git ;
- un compte Azure ;
- un Storage Account Azure ;
- les conteneurs ADLS `raw` et `clean`.

---

## 1. Cloner le dépôt

```powershell
git clone https://github.com/SamMebarek/simplon-tradecorp.git
cd simplon-tradecorp
```

---

## 2. Configurer les variables d’environnement

Créer un fichier `.env` à la racine du projet.

Il doit notamment contenir les informations nécessaires à la connexion Azure :

```
AZURE_STORAGE_ACCOUNT_NAME=...
AZURE_STORAGE_ACCOUNT_KEY=...
```

Le fichier `.env` ne doit pas être versionné.

---

## 3. Construire les images

```powershell
docker compose build
```

---

## 4. Démarrer l’environnement

```powershell
docker compose up -d
```

Vérifier les conteneurs :

```powershell
docker ps
```

---

# Services disponibles

## Spark / Jupyter

Jupyter :

```
http://localhost:8888
```

Spark UI :

```
http://localhost:4040
```

## Airflow

Interface :

```
http://localhost:8080
```

Identifiants de développement :

```
Utilisateur : admin
Mot de passe : admin
```

## pgAdmin

Interface :

```
http://localhost:8082
```

La base PostgreSQL utilisée par Airflow contient ses métadonnées : DAG runs, tâches, utilisateurs et états d’exécution.

---

# Tests

Les tests sont définis dans :

```
tests/test_transformers.py
```

Ils vérifient notamment :

- le nettoyage des commandes ;
- le calcul du sous-total ;
- le nettoyage des clients ;
- l’enrichissement avec une devise et un taux de change.

Pour exécuter les tests :

```powershell
docker exec tradecorp_spark spark-submit /home/jovyan/tests/run_tests.py
```

Résultat attendu :

```
4 passed
```

---

# Exécution manuelle du pipeline

Les taux de change peuvent être récupérés manuellement avec :

```powershell
docker exec tradecorp_spark python /home/jovyan/src/fetch_exchange_rates.py
```

Le pipeline complet peut également être lancé hors Airflow avec :

```powershell
docker exec tradecorp_spark spark-submit /home/jovyan/src/pipeline.py
```

---

# Jalon 3 : Orchestration avec Apache Airflow

Le Jalon 3 automatise l’exécution du pipeline TradeCorp.

Airflow ne contient pas de nouvelle logique de transformation : il orchestre les scripts existants dans le bon ordre.

## Architecture du DAG

Le DAG est défini dans :

```
dags/tradecorp_pipeline.py
```

Son identifiant est :

```
tradecorp_etl_pipeline
```

Il contient quatre tâches :

```
fetch_exchange_rates
        ↓
      reader
        ↓
   transformer
        ↓
      writer
```

Chaque tâche utilise un `DockerOperator` basé sur :

```
tradecorp_spark:latest
```

Le `DockerOperator` crée un conteneur temporaire contenant Spark et Java, exécute la commande puis détruit le conteneur.

Airflow accède à Docker grâce au socket :

```
/var/run/docker.sock
```

---

## Partage des données entre les tâches

Chaque tâche s’exécute dans un conteneur différent. Un DataFrame Spark ne peut donc pas être transmis directement d’une tâche à l’autre.

Le volume `data/` sert de stockage partagé :

```
reader.py
   ↓
data/tmp/reader_output/
   ↓
transformer.py
   ↓
data/tmp/transformer_output/orders_enriched/
   ↓
writer.py
   ↓
ADLS clean/orders_enriched/
```

Les conteneurs éphémères disposent notamment des montages :

```
src/  → /home/jovyan/src
data/ → /home/jovyan/data
.env  → /home/jovyan/.env
```

---

## Planification Airflow

Le DAG est configuré avec :

```python
start_date=datetime(2024, 1, 1)
schedule_interval="0 6 * * *"
catchup=False
```

Le pipeline s’exécute  automatiquement tous les jours à **6h**.

Chaque tâche dispose de :

```
1 retry
5 minutes de délai avant retry
```

`catchup=False` empêche Airflow de rejouer toutes les exécutions quotidiennes qui auraient théoriquement dû avoir lieu depuis le 1er janvier 2024.

Lors de l’activation du DAG, Airflow attend simplement la prochaine exécution planifiée.

---

## Logs Airflow

Chaque tâche possède ses propres logs dans l’interface Airflow.

### Récupération des taux de change

Extrait de `fetch_exchange_rates` :

```
2026-09-08 07:14:34,800 | INFO | Taux récupérés : 166 devises
2026-09-08 07:14:34,955 | INFO | exchange_rates.json uploadé avec succès
```

### Upload du résultat final

Extrait de `writer` :

```
2026-09-08 07:15:57,673 | INFO | Nombre de lignes à écrire : 2082
2026-09-08 07:15:59,752 | INFO | Upload de orders_enriched vers ADLS
2026-09-08 07:16:00,018 | INFO | orders_enriched envoyé dans le conteneur clean avec succès
```

Ces logs confirment la récupération de **166 devises** et l’écriture de **2082 lignes** dans le dataset final.

---

## Idempotence

Le DAG peut être déclenché plusieurs fois sans accumuler plusieurs versions du dataset.

Avant chaque nouvel upload, `writer.py` supprime les blobs déjà présents sous :

```
clean/orders_enriched/
```

puis écrit le nouveau résultat.

Ainsi, plusieurs exécutions successives ne mélangent pas plusieurs fichiers Parquet correspondant à des runs différents.

---

# Stockage final

Le résultat local est généré dans :

```
data/tmp/tradecorp_clean/orders_enriched/
```

Le résultat final est ensuite envoyé dans Azure :

```
clean/orders_enriched/
```

Le dataset contient notamment les colonnes :

```
currency
exchange_rate
sous_total_local
```

---

# Sécurité et Git

Le `.gitignore` exclut notamment :

```
.env

__pycache__/
*.py[cod]
.pytest_cache/

.ipynb_checkpoints/

data/tmp/

*.parquet
_SUCCESS
.sparkStaging/

*.log
logs/
```

Les identifiants Azure et autres secrets doivent rester dans `.env`.

---

# Commandes utiles

Démarrer l’environnement :

```powershell
docker compose up -d
```

Arrêter l’environnement :

```powershell
docker compose down
```

Reconstruire les images :

```powershell
docker compose build
```

Afficher les conteneurs :

```powershell
docker ps
```

Exécuter les tests :

```powershell
docker exec tradecorp_spark spark-submit /home/jovyan/tests/run_tests.py
```

---

# Résultat

TradeCorp dispose désormais d’un pipeline de données :

```
ADLS raw
    ↓
PySpark
    ↓
Transformation + enrichissement
    ↓
Parquet
    ↓
ADLS clean
```

orchestré automatiquement par Apache Airflow

Le pipeline est modulaire, testé, planifié quotidiennement, observable depuis l’interface Airflow et idempotent afin d’éviter l’accumulation de résultats issus de plusieurs exécutions.
