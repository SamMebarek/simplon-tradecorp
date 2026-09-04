# TradeCorp International — Data Engineering Pipeline

Projet de data engineering réalisé avec **Apache Spark / PySpark**, **Docker**, **PostgreSQL** et **Azure Data Lake Storage Gen2**.

L'objectif est de mettre en place un pipeline de données modulaire capable de :

- récupérer les données métier depuis Azure Data Lake Storage Gen2 ;
- nettoyer et transformer les données avec PySpark ;
- joindre les différentes sources métier ;
- enrichir les commandes avec la devise du pays du client ;
- appliquer les taux de change du jour ;
- produire un dataset consolidé au format Parquet ;
- déposer le résultat dans la zone `clean` d'ADLS ;
- valider les transformations avec des tests automatisés.

---

## Architecture générale

Le projet suit une architecture de type **Raw → Transform → Enrich → Clean**.

```text
                        ┌─────────────────────┐
                        │   Exchange Rate API │
                        │ exchangerate-api.com│
                        └──────────┬──────────┘
                                   │
                                   ▼
                        fetch_exchange_rates.py
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────┐
│                 Azure Data Lake Storage                  │
│                                                          │
│  raw/                                                    │
│  ├── categories.csv                                      │
│  ├── customers.csv                                       │
│  ├── employees.csv                                       │
│  ├── order_details.csv                                   │
│  ├── orders.csv                                          │
│  ├── products.csv                                        │
│  ├── shippers.csv                                        │
│  ├── suppliers.csv                                       │
│  └── reference/                                          │
│      ├── country_currency.csv                            │
│      └── exchange_rates.json                            │
└───────────────────────┬──────────────────────────────────┘
                        │
                        ▼
                    reader.py
                        │
                        ▼
               DataFrames PySpark
                        │
                        ▼
                  transformer.py
                        │
                        ▼
                 DataFrame enrichi
                        │
                        ▼
                  enrichment.py
                        │
                        ▼
          currency + sous_total_local
                        │
                        ▼
                     writer.py
                        │
                        ▼
┌──────────────────────────────────────────────────────────┐
│                    ADLS - clean                          │
│                                                          │
│  clean/                                                  │
│  └── orders_enriched/                                   │
│      └── *.parquet                                      │
└──────────────────────────────────────────────────────────┘

Le script `pipeline.py` orchestre les différentes étapes du traitement.

---

# Stack technique

## Apache Spark / PySpark

Spark est utilisé comme moteur principal de traitement.

Il permet de :

- charger les fichiers CSV sous forme de DataFrames ;
- nettoyer les données ;
- effectuer les jointures entre les différentes tables ;
- calculer les colonnes métier ;
- produire les données finales au format Parquet.

Le choix de Spark permet également de conserver une architecture adaptée à des volumes de données plus importants que ceux utilisés dans le cadre du projet.

---

## Docker / Docker Compose

Docker permet d'exécuter l'environnement de manière reproductible et isolée.

Docker Compose orchestre :

- Spark / PySpark ;
- Jupyter ;

Cela permet d'éviter une installation locale complexe de Spark et Java.

---

## Azure Data Lake Storage Gen2

ADLS constitue le stockage principal du pipeline.

Deux zones sont utilisées :

```
raw/
```

pour les données sources et les fichiers de référence,

et :

```
clean/
```

pour les données transformées.

Le projet utilise le SDK Python :

```
azure-storage-blob
```

pour télécharger et uploader les fichiers.

---

---

## Pytest

Les transformations critiques sont couvertes par des tests automatisés.

Les tests sont exécutés dans l'environnement Spark afin de tester directement les fonctions manipulant des DataFrames PySpark.

---

## API de taux de change

Les taux sont récupérés depuis l'API gratuite :

```
https://api.exchangerate-api.com/v4/latest/USD
```

Cette API :

- ne nécessite pas de compte ;
- ne nécessite pas de clé API ;
- retourne les taux avec une base USD.

La réponse JSON est conservée dans ADLS.

---

# Structure du projet

```text
tradecorp/
│
├── data/
│   └── tmp/
│       ├── tradecorp_raw/
│       ├── tradecorp_reference/
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
│   ├── fetch_exchange_rates.py
│
├── tests/
│   ├── test_transformers.py
│   └── run_tests.py
│
├── docker-compose.yml
├── .env
├── .gitignore
└── README.md
```

---

# Rôle des modules

## `src/utils.py`

Contient les fonctions communes utilisées par le pipeline.

Il regroupe notamment :

- la connexion à Azure Blob Storage ;
- le nettoyage des chaînes de caractères ;
- les conversions de types ;
- le nettoyage des clients ;
- le nettoyage des commandes ;
- le nettoyage des détails de commandes ;
- le calcul du sous-total ;
- la création du nom complet des employés ;
- la gestion du stock des produits.

Exemples de colonnes calculées :

```
full_name
is_shipped
en_stock
sous_total
```

Le sous-total est calculé selon :

```
sous_total = prix_unitaire × quantite × (1 - discount)
```

---

## `src/reader.py`

Le reader est responsable de la récupération des données.

Il télécharge les 8 fichiers métier depuis :

```
ADLS raw/
```

vers :

```
/home/jovyan/data/tmp/tradecorp_raw/
```

Il télécharge également les fichiers de référence :

```
raw/reference/country_currency.csv
raw/reference/exchange_rates.json
```

vers :

```
/home/jovyan/data/tmp/tradecorp_reference/
```

Les CSV métier sont ensuite chargés sous forme de DataFrames Spark.

---

## `src/transformer.py`

Le transformer applique les fonctions de nettoyage puis construit le DataFrame métier enrichi.

Les principales sources utilisées sont :

```
orders
order_details
customers
products
categories
employees
shippers
```

Les jointures permettent notamment d'obtenir :

```
order_id
customer_id
employee_id
product_id
order_date
required_date
shipped_date
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
is_shipped
en_stock
```

---

## `src/enrichment.py`

Ce module ajoute les informations de devise au DataFrame métier.

Le fichier :

```
country_currency.csv
```

permet d'associer :

```
pays → devise
```

Exemple :

```
FRANCE → EUR
USA → USD
CANADA → CAD
```

Le fichier :

```
exchange_rates.json
```

permet ensuite d'obtenir le taux de change correspondant.

Deux colonnes principales sont ajoutées :

```
currency
sous_total_local
```

Le calcul appliqué est :

```
sous_total_local = sous_total × exchange_rate
```

---

## `src/writer.py`

Le writer est chargé de produire le résultat final.

Le DataFrame est d'abord écrit localement en Parquet dans :

```
/home/jovyan/data/tmp/tradecorp_clean/orders_enriched/
```

Les fichiers sont ensuite uploadés vers :

```
ADLS clean/orders_enriched/
```

---

## `src/fetch_exchange_rates.py`

Script Python indépendant de Spark.

Il appelle :

```
https://api.exchangerate-api.com/v4/latest/USD
```

puis enregistre la réponse JSON brute dans :

```
raw/reference/exchange_rates.json
```

La récupération des taux est volontairement séparée du pipeline Spark.

Cela permet ensuite à un orchestrateur comme Airflow d'exécuter :

```
fetch_exchange_rates
        ↓
pipeline
```

---

## `src/pipeline.py`

Point d'entrée principal du traitement.

Le pipeline orchestre :

```
Lecture
   ↓
Transformation
   ↓
Lecture des références
   ↓
Enrichissement devise
   ↓
Écriture Parquet
   ↓
Upload ADLS
```

Le script utilise le module Python `logging` pour tracer les différentes étapes.

Une structure :

```python
try:
    ...
except Exception:
    ...
finally:
    spark.stop()
```

garantit l'arrêt de la SparkSession même lorsqu'une erreur survient.

---

# Données utilisées

Le pipeline travaille sur les huit fichiers métier suivants :

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

Ils doivent être présents dans le conteneur ADLS :

```
raw/
```

Les fichiers de référence sont stockés dans :

```
raw/reference/
```

avec :

```
country_currency.csv
exchange_rates.json
```

---

# Prérequis

Pour exécuter le projet, il faut disposer de :

- Docker Desktop ;
- Docker Compose ;
- Git ;
- un compte Azure ;
- un Storage Account Azure configuré ;
- les conteneurs ADLS `raw` et `clean`.

Sous Windows, Docker Desktop doit être démarré avant l'exécution des commandes.

---

# Installation

## 1. Cloner le dépôt

```powershell
git clone https://github.com/SamMebarek/simplon-tradecorp.git
```

Puis :

```powershell
cd simplon-tradecorp
```

---

## 2. Configurer les variables d'environnement

Créer un fichier :

```
.env
```

à la racine du projet.

Il doit contenir les informations Azure attendues par :

```python
get_blob_service_client()
```

dans :

```
src/utils.py
```

Le fichier `.env` contient des informations sensibles et ne doit pas être versionné.

Il est donc présent dans `.gitignore`.

---

## 3. Vérifier les volumes Docker

Le service Spark utilise notamment les volumes suivants :

```yaml
volumes:
  - ./data:/home/jovyan/data
  - ./notebooks:/home/jovyan/work
  - ./src:/home/jovyan/src
  - ./tests:/home/jovyan/tests
  - ./.env:/home/jovyan/.env
```

Les fichiers temporaires du pipeline sont ainsi accessibles depuis :

```
data/tmp/
```

sur la machine hôte.

---

## 4. Démarrer les conteneurs

```powershell
docker compose up -d
```

Vérifier leur état :

```powershell
docker ps
```

---

# Services Docker

## Spark / Jupyter

Conteneur :

```
tradecorp_spark
```

Image :

```
quay.io/jupyter/pyspark-notebook:latest
```

Ports :

```
Jupyter   : 8888
Spark UI  : 4040
```

---

# Préparation des références

## Mapping pays / devise

Le fichier :

```
country_currency.csv
```

doit être présent dans :

```
raw/reference/country_currency.csv
```

---

## Récupération des taux de change

Avant le premier lancement du pipeline, exécuter :

```powershell
docker exec tradecorp_spark python /home/jovyan/src/fetch_exchange_rates.py
```

Le script doit créer dans ADLS :

```
raw/reference/exchange_rates.json
```

Le pipeline pourra ensuite télécharger ce fichier.

---

# Lancement du pipeline

Une fois les fichiers de référence disponibles :

```powershell
docker exec tradecorp_spark spark-submit /home/jovyan/src/pipeline.py
```

Le pipeline doit afficher des logs similaires à :

```
Démarrage du pipeline TradeCorp
Début de la lecture des données métier
Lecture des données métier terminée
Début des transformations
Transformations terminées
Début de la lecture des fichiers de référence
Lecture des fichiers de référence terminée
Début de l'enrichissement devise
Enrichissement devise terminé
Début de l'écriture
Écriture terminée
Pipeline TradeCorp terminé avec succès
Arrêt de la SparkSession
```

---

# Tests

Les tests sont définis dans :

```
tests/test_transformers.py
```

Ils couvrent actuellement :

### Nettoyage des commandes

Vérifie que les commandes dont :

```
shipped_date = NULL
```

sont supprimées.

### Calcul du sous-total

Vérifie :

```
10 × 2 × (1 - 0.1) = 18
```

### Nettoyage des clients

Vérifie notamment :

```
"   jean dupont   " → "Jean Dupont"
" france "         → "FRANCE"
```

### Enrichissement devise

Utilise un taux simulé dans le test afin de ne dépendre d'aucun appel réseau.

Exemple :

```python
exchange_rates = {
    "base": "USD",
    "rates": {
        "USD": 1.0,
        "EUR": 0.8
    }
}
```

---

## Lancer les tests

```powershell
docker exec tradecorp_spark spark-submit /home/jovyan/tests/run_tests.py
```

Résultat attendu :

```
collected 4 items

4 passed
```

---

# Vérifier le résultat Parquet

Le résultat local est généré dans :

```
data/tmp/tradecorp_clean/orders_enriched/
```

Le dataset final doit notamment contenir :

```
currency
sous_total_local
```

Le résultat est également envoyé dans :

```
ADLS clean/orders_enriched
```

---

---

# Gestion des fichiers temporaires

Les fichiers récupérés pendant le pipeline sont conservés dans :

```
data/tmp/
```

avec l'organisation suivante :

```
data/tmp/
├── tradecorp_raw/
│   ├── categories.csv
│   ├── customers.csv
│   └── ...
│
├── tradecorp_reference/
│   ├── country_currency.csv
│   └── exchange_rates.json
│
└── tradecorp_clean/
    └── orders_enriched/
```

Ces fichiers ne sont pas destinés à être versionnés.

---

# Sécurité et Git

Les fichiers sensibles ou générés doivent être exclus du dépôt.

Exemple de `.gitignore` :

```
# Variables d'environnement
.env

# Python
__pycache__/
*.py[cod]
.pytest_cache/

# Jupyter
.ipynb_checkpoints/

# Données temporaires
data/tmp/

# Spark / Parquet
*.parquet
_SUCCESS
.sparkStaging/

# Logs
*.log
logs/
```

---

# Commandes utiles

Démarrer l'environnement :

```powershell
docker compose up -d
```

Arrêter l'environnement :

```powershell
docker compose down
```

Afficher les conteneurs :

```powershell
docker ps
```

Récupérer les taux :

```powershell
docker exec tradecorp_spark python /home/jovyan/src/fetch_exchange_rates.py
```

Exécuter les tests :

```powershell
docker exec tradecorp_spark spark-submit /home/jovyan/tests/run_tests.py
```

Lancer le pipeline :

```powershell
docker exec tradecorp_spark spark-submit /home/jovyan/src/pipeline.py
```

---

# Résultat final

Le projet met en place un pipeline de données modulaire avec séparation des responsabilités :

```
reader
    ↓
transformer
    ↓
enrichment
    ↓
writer
```

Les données métier sont récupérées depuis la zone `raw` d'Azure Data Lake Storage, nettoyées et enrichies avec PySpark, puis écrites au format Parquet dans la zone `clean`.

L'enrichissement final permet notamment d'obtenir :

```
customer_country
currency
sous_total
exchange_rate
sous_total_local
```
