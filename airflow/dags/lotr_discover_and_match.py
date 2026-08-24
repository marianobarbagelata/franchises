from __future__ import annotations

import datetime
import json
import logging

import anthropic
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task

logger = logging.getLogger(__name__)

FRANCHISE_SLUG = "lord-of-the-rings"
FRANCHISE_NAME = "The Lord of the Rings"
FRANCHISE_DESCRIPTION = (
    "La saga de fantasia de J.R.R. Tolkien, incluyendo su adaptacion "
    "cinematografica dirigida por Peter Jackson."
)

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


def _ai_match(source_name: str, candidates: list[dict]) -> list[dict]:
    """Logica de matching compartida entre las 4 fuentes -- funcion normal,
    no @task, para no repetir el prompt en cada tarea de match."""
    if not candidates:
        return []

    client = anthropic.Anthropic(api_key=Variable.get("ANTHROPIC_API_KEY"))
    candidate_lines = "\n".join(
        f"- id={c['external_id']} | {c['title']} ({c['year']}) - {c['overview'][:200]}"
        for c in candidates
    )

    response = client.messages.create(
        model="claude-opus-5",
        max_tokens=4096,
        system=(
            f"Sos un clasificador estricto. Dada la franquicia '{FRANCHISE_NAME}' "
            f"({FRANCHISE_DESCRIPTION}) y candidatos de la fuente '{source_name}' "
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
    logger.info("ai_match[%s]: %d/%d candidatos confirmados", source_name, len(confirmed_ids), len(candidates))
    for v in verdicts:
        marker = "OK" if v["external_id"] in confirmed_ids else "x "
        logger.info(" %s id=%s confidence=%.2f -- %s", marker, v["external_id"], v["confidence"], v["reason"])

    # Combina cada candidato original (title, source_url, raw_score, ...) con
    # el veredicto de la IA -- el task de load necesita ambas cosas juntas.
    return [
        {**c, "match_confidence": verdict_by_id[c["external_id"]]["confidence"]}
        for c in candidates
        if c["external_id"] in confirmed_ids
    ]


def _load_confirmed(source_code: str, work_type: str, confirmed: list[dict]) -> None:
    """Guarda los items confirmados en Postgres. Idempotente: si un
    (source_id, external_id) ya existe en work_source, actualiza en vez de
    duplicar -- por eso podemos re-correr el DAG sin miedo."""
    if not confirmed:
        logger.info("load[%s]: nada para guardar", source_code)
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
        (FRANCHISE_SLUG, FRANCHISE_NAME, FRANCHISE_DESCRIPTION),
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
    logger.info("load[%s]: %d obras guardadas/actualizadas", source_code, len(confirmed))


@dag(
    dag_id="lotr_discover_and_match",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["fase-1", "dev"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(seconds=30),
    },
)
def lotr_discover_and_match():
    @task
    def discover_tmdb_movies(query: str = FRANCHISE_NAME) -> list[dict]:
        access_token = Variable.get("TMDB_API_KEY")
        response = requests.get(
            "https://api.themoviedb.org/3/search/movie",
            headers={"Authorization": f"Bearer {access_token}", "accept": "application/json"},
            params={"query": query},
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
        logger.info("discover_tmdb_movies: %d candidatos crudos", len(candidates))
        return candidates

    @task
    def discover_tmdb_series(query: str = FRANCHISE_NAME) -> list[dict]:
        access_token = Variable.get("TMDB_API_KEY")
        response = requests.get(
            "https://api.themoviedb.org/3/search/tv",
            headers={"Authorization": f"Bearer {access_token}", "accept": "application/json"},
            params={"query": query},
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
        logger.info("discover_tmdb_series: %d candidatos crudos", len(candidates))
        return candidates

    @task
    def discover_open_library(query: str = FRANCHISE_NAME) -> list[dict]:
        response = requests.get(
            "https://openlibrary.org/search.json",
            params={"q": query, "fields": "key,title,first_publish_year,author_name"},
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
                    # Open Library no expone un puntaje de la comunidad en este endpoint.
                    "raw_score": None,
                    "raw_scale_max": None,
                    "source_url": f"https://openlibrary.org/works/{work_id}",
                }
            )
        logger.info("discover_open_library: %d candidatos crudos", len(candidates))
        return candidates

    @task
    def discover_rawg(query: str = FRANCHISE_NAME) -> list[dict]:
        api_key = Variable.get("RAWG_API_KEY")
        response = requests.get(
            "https://api.rawg.io/api/games",
            params={"search": query, "key": api_key},
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
        logger.info("discover_rawg: %d candidatos crudos", len(candidates))
        return candidates

    @task
    def match_tmdb_movies(candidates: list[dict]) -> list[dict]:
        return _ai_match("TMDB (peliculas)", candidates)

    @task
    def match_tmdb_series(candidates: list[dict]) -> list[dict]:
        return _ai_match("TMDB (series)", candidates)

    @task
    def match_open_library(candidates: list[dict]) -> list[dict]:
        return _ai_match("Open Library (libros)", candidates)

    @task
    def match_rawg(candidates: list[dict]) -> list[dict]:
        return _ai_match("RAWG (juegos)", candidates)

    @task
    def load_tmdb_movies(confirmed: list[dict]) -> None:
        _load_confirmed("tmdb_movie", "movie", confirmed)

    @task
    def load_tmdb_series(confirmed: list[dict]) -> None:
        _load_confirmed("tmdb_series", "series", confirmed)

    @task
    def load_open_library(confirmed: list[dict]) -> None:
        _load_confirmed("open_library", "book", confirmed)

    @task
    def load_rawg(confirmed: list[dict]) -> None:
        _load_confirmed("rawg", "game", confirmed)

    load_tmdb_movies(match_tmdb_movies(discover_tmdb_movies()))
    load_tmdb_series(match_tmdb_series(discover_tmdb_series()))
    load_open_library(match_open_library(discover_open_library()))
    load_rawg(match_rawg(discover_rawg()))


lotr_discover_and_match()
