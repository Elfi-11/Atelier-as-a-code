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
├── dags/
│   └── dag_minio.py          # DAG unique hopital_csv_to_postgres
├── data/                     # staging Airflow (staging/raw, patients_clean.csv)
├── docker-compose.yaml
├── Dockerfile
├── patients_*.csv            # sources copiées dans l'image (/opt/data)
├── scripts/
│   ├── upload_minio.py       # logique S3 (référence / tests locaux)
│   └── clean_and_load.py     # téléchargement MinIO, nettoyage, insert Postgres
└── sql/                      # schéma et seeds PostgreSQL
```

> **Pipeline :** `minio-init` dépose les CSV bruts dans MinIO au démarrage ; le DAG fait **fetch → clean → load**.

```powershell
cd C:\Users\Admin\Atelier-as-a-code\Atelier-as-a-code
```

Depuis la racine du dépôt Git :

```powershell
cd Atelier-as-a-code
```

## Repartir de zéro (prune + relance)

À lancer **depuis ce dossier** (`Atelier-as-a-code/`, là où se trouve ce `docker-compose.yaml`) :

```powershell
cd C:\Users\Admin\Atelier-as-a-code\Atelier-as-a-code
docker compose down -v
docker system prune -f
docker compose up -d --build
```

`down -v` recrée Postgres et MinIO vides ; les scripts `sql/` recréent la base **`hopital`**.

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
| `minio-init` | Upload des `patients_*.csv` vers `hopital-data/raw/` |
| `airflow-init` | Initialisation de la base Airflow + utilisateur admin |
| `airflow-dag-processor` | Lecture des fichiers `dags/` |
| `airflow` | API / interface web Airflow |
| `airflow-scheduler` | Planificateur Airflow |

### 2. Exécuter le pipeline (DAG)

1. Ouvrir http://localhost:8080 (`airflow` / `airflow`)
2. Activer le DAG **`hopital_csv_to_postgres`**
3. **Trigger DAG** (déclenchement manuel)

Chaîne des tasks :

```
fetch_from_minio → clean_patient_csv → load_to_postgres
```

### 3. Vérifier les logs

```powershell
# Tous les services
docker compose logs -f

# Un service précis
docker compose logs -f postgres
docker compose logs -f airflow
docker compose logs airflow-scheduler
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

### Le DAG échoue sur `fetch_and_clean_from_minio` ou `load_to_postgres`

Vérifiez que MinIO et Postgres sont démarrés, puis consultez les logs de la task dans l'UI Airflow ou :

```powershell
docker compose ps -a
docker compose logs airflow-scheduler
```

## Stack technique

- **PostgreSQL 15**
- **MinIO** (compatible S3)
- **Apache Airflow 3.0.1** (LocalExecutor)
- **Python 3.12** — pandas, SQLAlchemy, boto3, psycopg2
