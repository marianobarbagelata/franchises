CREATE TYPE work_type AS ENUM ('book', 'movie', 'series', 'game');

CREATE TABLE franchise (
    id          SERIAL PRIMARY KEY,
    slug        TEXT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE work (
    id           SERIAL PRIMARY KEY,
    franchise_id INT NOT NULL REFERENCES franchise(id),
    work_type    work_type NOT NULL,
    title        TEXT NOT NULL,
    release_date DATE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE source (
    id       SERIAL PRIMARY KEY,
    code     TEXT UNIQUE NOT NULL,
    name     TEXT NOT NULL,
    base_url TEXT NOT NULL
);

-- external_id es unico por fuente: es la clave natural que nos deja saber
-- si un item ya lo cargamos antes (idempotencia del task de load).
CREATE TABLE work_source (
    id                 SERIAL PRIMARY KEY,
    work_id            INT NOT NULL REFERENCES work(id) ON DELETE CASCADE,
    source_id          INT NOT NULL REFERENCES source(id),
    external_id        TEXT NOT NULL,
    source_url         TEXT NOT NULL,
    raw_score          NUMERIC,
    raw_scale_max      NUMERIC,
    normalized_score   NUMERIC,
    raw_payload_s3_key TEXT,
    match_method       TEXT NOT NULL DEFAULT 'ai',
    match_confidence   NUMERIC,
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_id, external_id)
);

INSERT INTO source (code, name, base_url) VALUES
    ('tmdb_movie',   'TMDB (peliculas)', 'https://www.themoviedb.org'),
    ('tmdb_series',  'TMDB (series)',    'https://www.themoviedb.org'),
    ('open_library', 'Open Library',     'https://openlibrary.org'),
    ('rawg',         'RAWG',             'https://rawg.io')
ON CONFLICT (code) DO NOTHING;
