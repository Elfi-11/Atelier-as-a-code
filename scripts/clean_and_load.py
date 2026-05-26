#!/usr/bin/env python3
"""
Nettoyage des CSV patients (séparateur, colonnes, casse, âge, services)
et chargement dans PostgreSQL (tables service + patient).
"""
from __future__ import annotations

import csv
import glob
import os
import re
import unicodedata
from io import StringIO
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent.parent
DB_URL = os.environ.get(
    "HOPITAL_DB_CONN",
    "postgresql+psycopg2://hopital:hopital@localhost:5432/hopital",
)

# Alias de colonnes -> schéma cible
COLUMN_ALIASES: dict[str, list[str]] = {
    "nom": [
        "nom",
        "nom_patient",
        "last_name",
        "nom_patient",
    ],
    "prenom": [
        "prenom",
        "prenom_patient",
        "first_name",
        "prénom",
    ],
    "age": [
        "age",
        "years_old",
        "age_patient",
    ],
    "pathologie": [
        "pathologie",
        "disease",
        "diagnostic_pathologie",
        "diagnostic",
    ],
    "service": [
        "service",
        "department",
        "service_destination",
        "service_demande",
    ],
    "tel": ["telephone", "tel", "téléphone"],
    "commentaire": ["commentaire", "comment"],
}

# Mapping service brut -> libellé canonique (table service)
SERVICE_MAP: dict[str, str] = {
    "cardio": "Cardiologie",
    "cardiologie": "Cardiologie",
    "chir cardio": "Cardiologie",
    "chir. cardio": "Cardiologie",
    "chirurgie cardiovasculaire": "Cardiologie",
    "ortho": "Orthopédie",
    "orthopedie": "Orthopédie",
    "orthopédie": "Orthopédie",
    "pediatrie": "Pédiatrie",
    "pédiatrie": "Pédiatrie",
    "neuro": "Neurologie",
    "neurologie": "Neurologie",
    "gynecologie": "Gynécologie",
    "gynécologie": "Gynécologie",
    "urgences": "Urgences",
    "endocrinologie": "Endocrinologie",
    "dermatologie": "Dermatologie",
    "test": None,
}


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(c for c in normalized if unicodedata.category(c) != "Mn")


def normalize_header(name: str) -> str:
    clean = strip_accents(str(name).strip().lower())
    clean = re.sub(r"[^a-z0-9]+", "_", clean).strip("_")
    return clean


def detect_separator(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
        return dialect.delimiter
    except csv.Error:
        return ";" if sample.count(";") > sample.count(",") else ","


def read_csv_file(path: Path) -> pd.DataFrame:
    raw = path.read_text(encoding="utf-8-sig")
    sep = detect_separator(raw[:2048])
    df = pd.read_csv(StringIO(raw), sep=sep, dtype=str, keep_default_na=False)
    df.columns = [normalize_header(c) for c in df.columns]
    return df


def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapped: dict[str, pd.Series] = {}
    for target, aliases in COLUMN_ALIASES.items():
        norm_aliases = [normalize_header(a) for a in aliases]
        for col in df.columns:
            if col in norm_aliases:
                mapped[target] = df[col]
                break
        if target not in mapped:
            mapped[target] = ""

    result = pd.DataFrame(mapped)
    return result.fillna("")


def to_title_fr(value: str) -> str:
    value = str(value).strip()
    if not value:
        return ""
    parts = re.split(r"([-\s'])", value.lower())
    return "".join(p.capitalize() if p not in "- '" else p for p in parts)


def parse_age(raw: str) -> tuple[int | None, str | None]:
    text = str(raw).strip().lower()
    if not text:
        return None, "age vide"

    if "mois" in text:
        return None, "age en mois non converti"

    match = re.search(r"\d+", text)
    if not match:
        return None, "age non numerique"

    age = int(match.group())
    if "ans" not in text and not text.isdigit():
        return None, "age non numerique"

    if age < 0:
        return None, "age negatif"
    if age > 110:
        return None, "age aberrant"

    return age, None


def harmonize_service(raw: str) -> tuple[str | None, str | None]:
    text = str(raw).strip()
    if not text:
        return None, "service vide"

    key = strip_accents(text).lower().strip()
    key = re.sub(r"\s+", " ", key)

    if key in SERVICE_MAP:
        canonical = SERVICE_MAP[key]
        if canonical is None:
            return None, "service test exclu"
        return canonical, None

    # Correspondance partielle simple
    for pattern, canonical in SERVICE_MAP.items():
        if pattern and pattern in key and canonical:
            return canonical, None

    canonical = to_title_fr(text)
    return canonical, f"service non reference: {text}"


def normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", str(raw))
    if not digits or digits.upper() == "NA":
        return ""
    if len(digits) == 10:
        return f"{digits[:2]} {digits[2:4]} {digits[4:6]} {digits[6:8]} {digits[8:10]}"
    return str(raw).strip()


def is_test_row(row: pd.Series) -> bool:
    nom = str(row.get("nom", "")).strip().lower()
    prenom = str(row.get("prenom", "")).strip().lower()
    pathologie = str(row.get("pathologie", "")).lower()
    return nom == "test" or "ligne de test" in pathologie


def clean_dataframe(df: pd.DataFrame, source: str) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        if is_test_row(row):
            continue

        nom = to_title_fr(row.get("nom", ""))
        prenom = to_title_fr(row.get("prenom", ""))
        age, age_motif = parse_age(row.get("age", ""))
        service, service_motif = harmonize_service(row.get("service", ""))
        tel = normalize_phone(row.get("tel", ""))
        pathologie = str(row.get("pathologie", "")).strip()
        commentaire = str(row.get("commentaire", "")).strip()

        motifs = [m for m in (age_motif, service_motif) if m]
        if not nom:
            motifs.append("nom manquant")

        rows.append(
            {
                "nom": nom or None,
                "prenom": prenom or None,
                "age": age,
                "tel": tel or None,
                "pathologie": pathologie or None,
                "commentaire": commentaire or None,
                "service_nom": service,
                "fichier_source": source,
                "est_valide": len([m for m in motifs if "exclu" in m]) == 0,
                "motif_correction": "; ".join(motifs) if motifs else None,
            }
        )

    cleaned = pd.DataFrame(rows)
    if cleaned.empty:
        return cleaned

    cleaned = cleaned.drop_duplicates(
        subset=["nom", "prenom", "age", "pathologie", "service_nom"],
        keep="first",
    )
    return cleaned


def find_csv_files() -> list[str]:
    for folder in [ROOT, ROOT / "data", Path("/opt/data")]:
        files = sorted(glob.glob(str(folder / "patients_*.csv")))
        if files:
            return files
    raise FileNotFoundError("Aucun fichier patients_*.csv trouvé")


def load_all_csv() -> pd.DataFrame:
    files = find_csv_files()

    frames = []
    for file_path in files:
        path = Path(file_path)
        raw_df = read_csv_file(path)
        mapped = map_columns(raw_df)
        cleaned = clean_dataframe(mapped, path.name)
        frames.append(cleaned)
        print(f"  {path.name}: {len(cleaned)} lignes nettoyées")

    return pd.concat(frames, ignore_index=True)


def load_to_postgres(df: pd.DataFrame, engine) -> None:
    with engine.begin() as conn:
        services = (
            df["service_nom"].dropna().astype(str).str.strip().unique().tolist()
        )
        for service in sorted(set(services)):
            if service:
                conn.execute(
                    text(
                        "INSERT INTO service (nom) VALUES (:nom) "
                        "ON CONFLICT (nom) DO NOTHING"
                    ),
                    {"nom": service},
                )

        service_ids = {
            row.nom: row.id_service
            for row in conn.execute(text("SELECT id_service, nom FROM service"))
        }

        conn.execute(text("DELETE FROM patient"))

        for _, row in df.iterrows():
            service_nom = row.get("service_nom")
            id_service = service_ids.get(service_nom) if pd.notna(service_nom) else None
            conn.execute(
                text(
                    """
                    INSERT INTO patient (
                        nom, prenom, age, tel, pathologie, commentaire,
                        id_service, fichier_source, est_valide, motif_correction
                    ) VALUES (
                        :nom, :prenom, :age, :tel, :pathologie, :commentaire,
                        :id_service, :fichier_source, :est_valide, :motif_correction
                    )
                    """
                ),
                {
                    "nom": row["nom"],
                    "prenom": row["prenom"],
                    "age": row["age"],
                    "tel": row["tel"],
                    "pathologie": row["pathologie"],
                    "commentaire": row["commentaire"],
                    "id_service": id_service,
                    "fichier_source": row["fichier_source"],
                    "est_valide": bool(row["est_valide"]),
                    "motif_correction": row["motif_correction"],
                },
            )


def main() -> None:
    print("Lecture et nettoyage des CSV...")
    df = load_all_csv()
    print(f"Total: {len(df)} patients")

    out = ROOT / "data" / "patients_clean.csv"
    out.parent.mkdir(exist_ok=True)
    df.to_csv(out, index=False, sep=";")
    print(f"CSV nettoyé exporté: {out}")

    print("Chargement PostgreSQL...")
    engine = create_engine(DB_URL)
    load_to_postgres(df, engine)
    print("Terminé.")


if __name__ == "__main__":
    main()
