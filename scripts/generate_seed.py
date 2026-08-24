"""Bootstrap del seed de franquicias.

No es parte del pipeline recurrente de Airflow -- es un script que se corre
a mano, una vez (o cada tanto para ampliar el catalogo). Genera una lista
de franquicias candidatas con Claude (usando busqueda web para tener
cobertura y datos frescos) y la guarda en seeds/franchises.yml para que
la revises antes de commitear.

Uso (desde el host, con los contenedores levantados):
    docker compose exec -e ANTHROPIC_API_KEY="tu-key-aca" airflow-apiserver \
        python /opt/airflow/scripts/generate_seed.py
"""
from __future__ import annotations

import json
import os
import sys

import anthropic
import yaml

SEED_SCHEMA = {
    "type": "object",
    "properties": {
        "franchises": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "slug": {"type": "string"},
                    "name": {"type": "string"},
                    "description": {"type": "string"},
                    "present_in": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["book", "movie", "series", "game"]},
                    },
                },
                "required": ["slug", "name", "description", "present_in"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["franchises"],
    "additionalProperties": False,
}


def main() -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Falta ANTHROPIC_API_KEY en el entorno de este comando.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    print("Pidiendole a Claude la lista de franquicias (puede tardar un rato, usa busqueda web)...")
    with client.messages.stream(
        model="claude-opus-5",
        max_tokens=32000,
        tools=[{"type": "web_search_20260209", "name": "web_search", "max_uses": 10}],
        output_config={"format": {"type": "json_schema", "schema": SEED_SCHEMA}},
        messages=[
            {
                "role": "user",
                "content": (
                    "Necesito una lista de ~120 franquicias de entretenimiento muy "
                    "reconocidas que tengan presencia en AL MENOS 2 de estos tipos: "
                    "libro, pelicula, serie, videojuego -- el objetivo es poder comparar "
                    "puntuaciones entre tipos, asi que una franquicia que solo existe "
                    "como pelicula no sirve para esto. Para cada una: slug en kebab-case, "
                    "nombre, descripcion de 1-2 oraciones, y en que tipos tiene presencia "
                    "conocida. Priorizá diversidad, no solo franquicias de Hollywood."
                ),
            }
        ],
    ) as stream:
        response = stream.get_final_message()

    if response.stop_reason == "max_tokens":
        print(
            "ADVERTENCIA: la respuesta se corto por max_tokens de nuevo -- "
            "subi el valor en el script o pedi menos franquicias.",
            file=sys.stderr,
        )
        sys.exit(1)

    text = next(b.text for b in response.content if b.type == "text")
    franchises = json.loads(text)["franchises"]

    output_path = "/opt/airflow/seeds/franchises.yml"
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(franchises, f, allow_unicode=True, sort_keys=False)

    print(f"Generadas {len(franchises)} franquicias -> {output_path}")
    print("Revisala a mano antes de usarla en el pipeline (sacar duplicados, cosas mal categorizadas, etc).")


if __name__ == "__main__":
    main()
