from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import local

import requests
from requests.adapters import HTTPAdapter


BASE_DIR = Path(__file__).resolve().parent
JSON_DIR = BASE_DIR / "jsons"
ENDPOINT = "http://127.0.0.1/sga/Api001/sincronizador"
REQUEST_DELAY_SECONDS = 0
# Numero de envios HTTP em paralelo. Aumente aos poucos se o backend aguentar.
MAX_WORKERS = 4
# Timeout no formato: (segundos para conectar, segundos aguardando a resposta).
REQUEST_TIMEOUT = (5, 30)

JSON_LIST = [
    "usuario.json",
    "usuarioXempresa.json",
    "usuarioXdominio.json",
    "empresas-fnd.json",
    "grupo.json",
    "grupos.json",
    "programas.json",
    "programa-grupo.json",
]
REPORT_FILE = BASE_DIR / "relatorio_processamento.json"
THREAD_STATE = local()


def create_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_thread_session() -> requests.Session:
    session = getattr(THREAD_STATE, "session", None)
    if session is None:
        session = create_session()
        THREAD_STATE.session = session
    return session


def post_json(endpoint: str, payload: dict) -> dict:
    session = get_thread_session()
    try:
        response = session.post(endpoint, json=payload, timeout=REQUEST_TIMEOUT)
        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type.lower():
            try:
                body = response.json()
            except ValueError:
                body = response.text
        else:
            body = response.text
        return {
            "status_code": response.status_code,
            "ok": response.ok,
            "response": body,
        }
    except requests.RequestException as exc:
        return {
            "status_code": None,
            "ok": False,
            "response": str(exc),
        }


def process_payload(payload: dict) -> dict:
    result = post_json(ENDPOINT, payload)
    if REQUEST_DELAY_SECONDS > 0:
        time.sleep(REQUEST_DELAY_SECONDS)
    return result


def process_file(file_path: Path) -> dict:
    if not file_path.is_file():
        return {
            "arquivo": file_path.name,
            "status": "ignorado",
            "motivo": "Arquivo nao encontrado",
        }

    total_linhas = 0
    enviadas = 0
    puladas = 0
    resultados = []
    payloads = []

    with file_path.open("r", encoding="utf-8") as handler:
        for raw_line in handler:
            line = raw_line.strip()
            if not line:
                continue

            total_linhas += 1

            try:
                decoded = json.loads(line)
                payload = decoded["jsonBody"]
                if not isinstance(payload.get("json"), list):
                    raise ValueError("jsonBody.json invalido")
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                puladas += 1
                continue

            payloads.append(payload)
            enviadas += 1

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        resultados = list(executor.map(process_payload, payloads))

    return {
        "arquivo": file_path.name,
        "status": "ok",
        "total_linhas": total_linhas,
        "enviadas": enviadas,
        "puladas": puladas,
        "resultados": resultados,
    }


def build_report() -> dict:
    processados = []
    for json_file in JSON_LIST:
        processados.append(process_file(JSON_DIR / json_file))

    return {
        "mensagem": "Processamento finalizado",
        "arquivos": len(JSON_LIST),
        "processos": processados,
        "gerado_em": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    report = build_report()
    REPORT_FILE.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
