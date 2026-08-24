from __future__ import annotations

import datetime
import json
import logging

import anthropic
import requests
from airflow.sdk import Variable, dag, task

logger = logging.getLogger(__name__)

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

    confirmed = [v for v in verdicts if v["belongs"] and v["confidence"] >= 0.6]
    logger.info("ai_match[%s]: %d/%d candidatos confirmados", source_name, len(confirmed), len(candidates))
    for v in verdicts:
        marker = "OK" if v in confirmed else "x "
        logger.info(" %s id=%s confidence=%.2f -- %s", marker, v["external_id"], v["confidence"], v["reason"])

    return confirmed


@dag(
    dag_id="lotr_discover_and_match",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["fase-1", "dev"],
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
            timeout=10,
        )
        response.raise_for_status()
        docs = response.json()["docs"]
        candidates = [
            {
                "external_id": d["key"].split("/")[-1],
                "title": d.get("title", "?"),
                "year": str(d.get("first_publish_year", "?")),
                "overview": ("por " + ", ".join(d.get("author_name", [])[:2])) if d.get("author_name") else "",
            }
            for d in docs[:20]
        ]
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

    match_tmdb_movies(discover_tmdb_movies())
    match_tmdb_series(discover_tmdb_series())
    match_open_library(discover_open_library())
    match_rawg(discover_rawg())


lotr_discover_and_match()
