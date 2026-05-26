# Atelier-as-a-code — Pipeline données hôpital

Projet de démonstration : ingestion de fichiers CSV patients via **MinIO**, orchestration avec **Apache Airflow 3**, stockage relationnel dans **PostgreSQL**, traitement en **Python**.

## Objectif

- Stocker les CSV bruts dans MinIO (`hopital-data/raw/`)
- Nettoyer et normaliser les données (séparateurs, colonnes, casse, âges, services)
- Charger les patients dans PostgreSQL avec le modèle **1 service → N patients**

## Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (ou Docker Engine + Docker Compose v2)
- Ports libres sur la machine hôte : **5432**, **8080**, **9000**, **9001**

## Structure du dépôt

Les fichiers du projet se trouvent dans ce dossier :

```
Atelier-as-a-code/
├── docker-compose.yaml
├── Dockerfile
├── patients_*.csv          # jeux de données bruts
├── scripts/
│   ├── upload_minio.py     # envoi des CSV vers MinIO
│   └── clean_and_load.py   # nettoyage + chargement PostgreSQL
└── sql/                    # schéma et seeds PostgreSQL
```

> **Important :** lancez toutes les commandes Docker **depuis ce dossier** (là où se trouve `docker-compose.yaml`), pas depuis la racine du dépôt Git parent.

```powershell
cd C:\Users\Admin\Atelier-as-a-code\Atelier-as-a-code
```

## Démarrage rapide

### 1. Lancer l'infrastructure

```powershell
docker compose up -d --build
```

Services démarrés :

| Service | Rôle |
|---------|------|
| `postgres` | Base Airflow + base métier `hopital` |
| `minio` | Stockage objet (CSV bruts) |
| `minio-init` | Upload automatique des `patients_*.csv` vers MinIO |
| `airflow-init` | Initialisation de la base Airflow + utilisateur admin |
| `airflow` | API / interface web Airflow |
| `airflow-scheduler` | Planificateur Airflow |

### 2. Charger les données en PostgreSQL

Le chargement est dans le profil `load` (il ne démarre pas automatiquement) :

```powershell
docker compose --profile load up hopital-load
```

Ce conteneur exécute `clean_and_load.py` : nettoyage des CSV puis insertion dans les tables `service` et `patient`.

### 3. Vérifier les logs

```powershell
# Tous les services
docker compose logs -f

# Un service précis
docker compose logs -f postgres
docker compose logs -f airflow
docker compose logs hopital-load
```

## Accès aux services

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Airflow** | http://localhost:8080 | `airflow` / `airflow` |
| **MinIO Console** | http://localhost:9001 | `minio` / `minio123` |
| **MinIO API** | http://localhost:9000 | — |
| **PostgreSQL (Airflow)** | `localhost:5432` | user `airflow`, mdp `airflow`, base `airflow` |
| **PostgreSQL (métier)** | `localhost:5432` | user `hopital`, mdp `hopital`, base `hopital` |

### Requêtes SQL utiles

```powershell
docker compose exec postgres psql -U hopital -d hopital
```

```sql
SELECT COUNT(*) FROM patient;
SELECT s.nom, COUNT(p.id_patient) AS nb_patients
FROM service s
LEFT JOIN patient p ON p.id_service = s.id_service
GROUP BY s.nom
ORDER BY nb_patients DESC;
```

## Arrêt et nettoyage

```powershell
# Arrêter les conteneurs (conserver les volumes)
docker compose down

# Arrêter et supprimer les volumes (réinitialise Postgres et MinIO)
docker compose down -v
```

## Modèle de données

```
service (1) ──────< patient (N)
```

- **`service`** : `id_service`, `nom` (ex. Cardiologie, Orthopédie…)
- **`patient`** : identité, âge, pathologie, téléphone, lien `id_service`, traçabilité (`fichier_source`, `est_valide`, `motif_correction`)

Les scripts SQL d'initialisation sont dans `sql/` et s'exécutent au premier démarrage de Postgres.

## Fichiers CSV de test

Cinq jeux de données couvrent différents cas de qualité :

| Fichier | Cas testés |
|---------|------------|
| `patients_01_classique_espaces_casse.csv` | Espaces, casse |
| `patients_02_semicolon_doublons_services_abreges.csv` | `;`, doublons, abréviations de services |
| `patients_03_colonnes_anglais_valeurs_invalides.csv` | Colonnes en anglais, valeurs invalides |
| `patients_04_champs_inutiles_accents_commentaires.csv` | Champs parasites, accents, commentaires |
| `patients_05_colonnes_sales_lignes_a_rejeter.csv` | Colonnes bruitées, lignes à rejeter |

## Exécution locale des scripts (optionnel)

Sans Docker, avec Python 3.12+ :

```powershell
pip install -r requirements.txt
python scripts/upload_minio.py
python scripts/clean_and_load.py
```

Variables d'environnement utiles :

| Variable | Défaut |
|----------|--------|
| `MINIO_ENDPOINT` | `http://localhost:9000` |
| `MINIO_ACCESS_KEY` | `minio` |
| `MINIO_SECRET_KEY` | `minio123` |
| `MINIO_BUCKET` | `hopital-data` |
| `HOPITAL_DB_CONN` | `postgresql+psycopg2://hopital:hopital@localhost:5432/hopital` |

## Dépannage

### `no configuration file provided: not found`

Vous n'êtes pas dans le bon répertoire. Placez-vous dans le dossier contenant `docker-compose.yaml` (voir [Structure du dépôt](#structure-du-dépôt)).

### `Bind for 0.0.0.0:9000 failed: port is already allocated`

Le port 9000 est déjà utilisé (autre instance MinIO, autre conteneur, etc.).

```powershell
# Identifier le processus (Windows)
netstat -ano | findstr ":9000"
```

Solutions : arrêter le processus qui occupe le port, ou modifier le mapping dans `docker-compose.yaml` (ex. `"9002:9000"`).

### Postgres ne réinitialise pas le schéma

Les scripts `sql/` ne s'exécutent qu'au **premier** démarrage du volume Postgres. Pour repartir de zéro :

```powershell
docker compose down -v
docker compose up -d --build
```

### `hopital-load` échoue

Vérifiez que Postgres et `minio-init` sont terminés avec succès :

```powershell
docker compose ps -a
docker compose logs minio-init
docker compose logs hopital-load
```

## Stack technique

- **PostgreSQL 15**
- **MinIO** (compatible S3)
- **Apache Airflow 3.0.1** (LocalExecutor)
- **Python 3.12** — pandas, SQLAlchemy, boto3, psycopg2
