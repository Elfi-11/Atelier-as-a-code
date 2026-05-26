\c hopital

INSERT INTO service (nom) VALUES
    ('Cardiologie'),
    ('Orthopédie'),
    ('Pédiatrie'),
    ('Gynécologie'),
    ('Neurologie'),
    ('Urgences'),
    ('Endocrinologie'),
    ('Dermatologie')
ON CONFLICT (nom) DO NOTHING;
