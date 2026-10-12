from __future__ import annotations

import datetime
import json
import logging

import anthropic
import requests
import yaml
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task

logger = logging.getLogger(__name__)

SEED_PATH = "/opt/airflow/seeds/franchises.yml"

# Entrenamiento con pocas franquicias antes de correr las 101 -- cada corrida
# completa son ~400 llamadas a APIs externas + ~400 a Claude. Subilo cuando
# confirmes que anda bien.
SEED_LIMIT = 5

MATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "verdicts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "external_id": {"type": "string"},
                    "belongs": {"type": "boolean"},
                    "confidence": {"type": "number"},
                    "reason": {"type": "string"},
                },
                "required": ["external_id", "belongs", "confidence", "reason"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["verdicts"],
    "additionalProperties": False,
}


def _ai_match(source_name: str, franchise: dict, candidates: list[dict]) -> dict:
    """Logica de matching compartida entre las 4 fuentes -- funcion normal,
    no @task, para no repetir el prompt en cada tarea de match. Devuelve el
    payload {franchise, confirmed} para que el siguiente task en la cadena
    (load) tenga todo lo que necesita sin tener que sincronizar dos listas
    expandidas por separado."""
    if not candidates:
        return {"franchise": franchise, "confirmed": []}

    client = anthropic.Anthropic(api_key=Variable.get("ANTHROPIC_API_KEY"))
    candidate_lines = "\n".join(
        f"- id={c['external_id']} | {c['title']} ({c['year']}) - {c['overview'][:200]}"
        for c in candidates
    )

    response = client.messages.create(
        model="claude-opus-5",
        max_tokens=4096,
        system=(
            f"Sos un clasificador estricto. Dada la franquicia '{franchise['name']}' "
            f"({franchise['description']}) y candidatos de la fuente '{source_name}' "
            "obtenidos por busqueda de texto libre, decidis cuales pertenecen "
            "genuinamente a esa franquicia. Documentales, detras de camaras, "
            "conciertos, adaptaciones de fans y contenido tangencial NO pertenecen. "
            "Ante la duda, belongs=false."
        ),
        output_config={"format": {"type": "json_schema", "schema": MATCH_SCHEMA}},
        messages=[{"role": "user", "content": f"Candidatos:\n{candidate_lines}"}],
    )
    text = next(b.text for b in response.content if b.type == "text")
    verdicts = json.loads(text)["verdicts"]
    verdict_by_id = {v["external_id"]: v for v in verdicts}

    confirmed_ids = {v["external_id"] for v in verdicts if v["belongs"] and v["confidence"] >= 0.6}
    logger.info(
        "ai_match[%s][%s]: %d/%d candidatos confirmados",
        franchise["slug"], source_name, len(confirmed_ids), len(candidates),
    )
    for v in verdicts:
        marker = "OK" if v["external_id"] in confirmed_ids else "x "
        logger.info(" %s id=%s confidence=%.2f -- %s", marker, v["external_id"], v["confidence"], v["reason"])

    confirmed = [
        {**c, "match_confidence": verdict_by_id[c["external_id"]]["confidence"]}
        for c in candidates
        if c["external_id"] in confirmed_ids
    ]
    return {"franchise": franchise, "confirmed": confirmed}


def _load_confirmed(source_code: str, work_type: str, payload: dict) -> None:
    """Guarda los items confirmados en Postgres. Idempotente: si un
    (source_id, external_id) ya existe en work_source, actualiza en vez de
    duplicar -- por eso podemos re-correr el DAG sin miedo."""
    franchise = payload["franchise"]
    confirmed = payload["confirmed"]
    if not confirmed:
        logger.info("load[%s][%s]: nada para guardar", franchise["slug"], source_code)
        return

    conn = PostgresHook(postgres_conn_id="franchises_db").get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO franchise (slug, name, description)
        VALUES (%s, %s, %s)
        ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        """,
        (franchise["slug"], franchise["name"], franchise["description"]),
    )
    franchise_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM source WHERE code = %s", (source_code,))
    source_id = cur.fetchone()[0]

    for item in confirmed:
        cur.execute(
            "SELECT work_id FROM work_source WHERE source_id = %s AND external_id = %s",
            (source_id, item["external_id"]),
        )
        row = cur.fetchone()
        if row:
            work_id = row[0]
        else:
            cur.execute(
                "INSERT INTO work (franchise_id, work_type, title) VALUES (%s, %s, %s) RETURNING id",
                (franchise_id, work_type, item["title"]),
            )
            work_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO work_source
                (work_id, source_id, external_id, source_url, raw_score, raw_scale_max, match_method, match_confidence)
            VALUES (%s, %s, %s, %s, %s, %s, 'ai', %s)
            ON CONFLICT (source_id, external_id) DO UPDATE SET
                raw_score = EXCLUDED.raw_score,
                match_confidence = EXCLUDED.match_confidence,
                fetched_at = now()
            """,
            (
                work_id,
                source_id,
                item["external_id"],
                item["source_url"],
                item.get("raw_score"),
                item.get("raw_scale_max"),
                item["match_confidence"],
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logger.info("load[%s][%s]: %d obras guardadas/actualizadas", franchise["slug"], source_code, len(confirmed))


@dag(
    dag_id="franchise_pipeline",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["fase-1"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(seconds=30),
    },
)
def franchise_pipeline():
    @task
    def load_seed() -> list[dict]:
        with open(SEED_PATH, encoding="utf-8") as f:
            franchises = yaml.safe_load(f)
        franchises = franchises[:SEED_LIMIT]
        logger.info("load_seed: %d franquicias (de %d en el archivo)", len(franchises), SEED_LIMIT)
        return franchises

    @task
    def discover_tmdb_movies(franchise: dict) -> dict:
        access_token = Variable.get("TMDB_API_KEY")
        response = requests.get(
            "https://api.themoviedb.org/3/search/movie",
            headers={"Authorization": f"Bearer {access_token}", "accept": "application/json"},
            params={"query": franchise["name"]},
            timeout=10,
        )
        response.raise_for_status()
        results = response.json()["results"]
        candidates = [
            {
                "external_id": str(r["id"]),
                "title": r["title"],
                "year": (r.get("release_date") or "")[:4] or "?",
                "overview": r.get("overview", ""),
                "raw_score": r.get("vote_average"),
                "raw_scale_max": 10,
                "source_url": f"https://www.themoviedb.org/movie/{r['id']}",
            }
            for r in results
        ]
        logger.info("discover_tmdb_movies[%s]: %d candidatos crudos", franchise["slug"], len(candidates))
        return {"franchise": franchise, "candidates": candidates}

    @task
    def discover_tmdb_series(franchise: dict) -> dict:
        access_token = Variable.get("TMDB_API_KEY")
        response = requests.get(
            "https://api.themoviedb.org/3/search/tv",
            headers={"Authorization": f"Bearer {access_token}", "accept": "application/json"},
            params={"query": franchise["name"]},
            timeout=10,
        )
        response.raise_for_status()
        results = response.json()["results"]
        candidates = [
            {
                "external_id": str(r["id"]),
                "title": r["name"],
                "year": (r.get("first_air_date") or "")[:4] or "?",
                "overview": r.get("overview", ""),
                "raw_score": r.get("vote_average"),
                "raw_scale_max": 10,
                "source_url": f"https://www.themoviedb.org/tv/{r['id']}",
            }
            for r in results
        ]
        logger.info("discover_tmdb_series[%s]: %d candidatos crudos", franchise["slug"], len(candidates))
        return {"franchise": franchise, "candidates": candidates}

    @task
    def discover_open_library(franchise: dict) -> dict:
        response = requests.get(
            "https://openlibrary.org/search.json",
            params={"q": franchise["name"], "fields": "key,title,first_publish_year,author_name"},
            headers={"User-Agent": "franchises-project/0.1 (educational project)"},
            timeout=20,
        )
        response.raise_for_status()
        docs = response.json()["docs"]
        candidates = []
        for d in docs[:20]:
            work_id = d["key"].split("/")[-1]  # "OL27448W", sin importar si "key" trae el prefijo /works/
            candidates.append(
                {
                    "external_id": work_id,
                    "title": d.get("title", "?"),
                    "year": str(d.get("first_publish_year", "?")),
                    "overview": ("por " + ", ".join(d.get("author_name", [])[:2])) if d.get("author_name") else "",
                    "raw_score": None,
                    "raw_scale_max": None,
                    "source_url": f"https://openlibrary.org/works/{work_id}",
                }
            )
        logger.info("discover_open_library[%s]: %d candidatos crudos", franchise["slug"], len(candidates))
        return {"franchise": franchise, "candidates": candidates}

    @task
    def discover_rawg(franchise: dict) -> dict:
        api_key = Variable.get("RAWG_API_KEY")
        response = requests.get(
            "https://api.rawg.io/api/games",
            params={"search": franchise["name"], "key": api_key},
            timeout=10,
        )
        response.raise_for_status()
        results = response.json()["results"]
        candidates = [
            {
                "external_id": str(g["id"]),
                "title": g["name"],
                "year": (g.get("released") or "")[:4] or "?",
                "overview": ", ".join(genre["name"] for genre in g.get("genres", [])),
                "raw_score": g.get("rating"),
                "raw_scale_max": 5,
                "source_url": f"https://rawg.io/games/{g.get('slug', g['id'])}",
            }
            for g in results
        ]
        logger.info("discover_rawg[%s]: %d candidatos crudos", franchise["slug"], len(candidates))
        return {"franchise": franchise, "candidates": candidates}

    @task
    def match_tmdb_movies(payload: dict) -> dict:
        return _ai_match("TMDB (peliculas)", payload["franchise"], payload["candidates"])

    @task
    def match_tmdb_series(payload: dict) -> dict:
        return _ai_match("TMDB (series)", payload["franchise"], payload["candidates"])

    @task
    def match_open_library(payload: dict) -> dict:
        return _ai_match("Open Library (libros)", payload["franchise"], payload["candidates"])

    @task
    def match_rawg(payload: dict) -> dict:
        return _ai_match("RAWG (juegos)", payload["franchise"], payload["candidates"])

    @task
    def load_tmdb_movies(payload: dict) -> None:
        _load_confirmed("tmdb_movie", "movie", payload)

    @task
    def load_tmdb_series(payload: dict) -> None:
        _load_confirmed("tmdb_series", "series", payload)

    @task
    def load_open_library(payload: dict) -> None:
        _load_confirmed("open_library", "book", payload)

    @task
    def load_rawg(payload: dict) -> None:
        _load_confirmed("rawg", "game", payload)

    seed = load_seed()

    load_tmdb_movies.expand(payload=match_tmdb_movies.expand(payload=discover_tmdb_movies.expand(franchise=seed)))
    load_tmdb_series.expand(payload=match_tmdb_series.expand(payload=discover_tmdb_series.expand(franchise=seed)))
    load_open_library.expand(payload=match_open_library.expand(payload=discover_open_library.expand(franchise=seed)))
    load_rawg.expand(payload=match_rawg.expand(payload=discover_rawg.expand(franchise=seed)))


franchise_pipeline()
