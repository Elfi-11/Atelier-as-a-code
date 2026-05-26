\c hopital

CREATE TABLE IF NOT EXISTS service (
    id_service  SERIAL PRIMARY KEY,
    nom         VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS patient (
    id_patient      SERIAL PRIMARY KEY,
    nom             VARCHAR(100),
    prenom          VARCHAR(100),
    age             INTEGER,
    tel             VARCHAR(30),
    pathologie      TEXT,
    commentaire     TEXT,
    id_service      INTEGER REFERENCES service(id_service),
    fichier_source  VARCHAR(150),
    est_valide      BOOLEAN DEFAULT TRUE,
    motif_correction TEXT
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO hopital;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO hopital;
