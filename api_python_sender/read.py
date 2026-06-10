from __future__ import annotations

import argparse
import json
from collections import defaultdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


BASE_DIR = Path(__file__).resolve().parent
JSON_DIR = BASE_DIR / "jsons"
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
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_LIMIT = 100


def load_json_records() -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    records_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    summary: dict[str, dict[str, Any]] = {}

    for file_name in JSON_LIST:
        file_path = JSON_DIR / file_name
        if not file_path.is_file():
            continue

        with file_path.open("r", encoding="utf-8") as handler:
            for line_number, raw_line in enumerate(handler, start=1):
                line = raw_line.strip()
                if not line:
                    continue

                try:
                    payload = json.loads(line)
                    body = payload["jsonBody"]
                    tipo = str(body["tipo"])
                    rows = body["json"]
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue

                if not isinstance(rows, list):
                    continue

                bucket = records_by_type[tipo]
                type_summary = summary.setdefault(
                    tipo,
                    {
                        "tipo": tipo,
                        "arquivos": set(),
                        "campos": set(),
                        "registros": 0,
                    },
                )

                for row in rows:
                    if not isinstance(row, dict):
                        continue

                    normalized = dict(row)
                    normalized["_tipo"] = tipo
                    normalized["_arquivo"] = file_name
                    normalized["_linha"] = line_number
                    bucket.append(normalized)
                    type_summary["registros"] += 1
                    type_summary["campos"].update(row.keys())

                type_summary["arquivos"].add(file_name)

    normalized_summary: dict[str, dict[str, Any]] = {}
    for tipo, info in summary.items():
        normalized_summary[tipo] = {
            "tipo": tipo,
            "arquivos": sorted(info["arquivos"]),
            "campos": sorted(info["campos"]),
            "registros": info["registros"],
        }

    return dict(records_by_type), normalized_summary


def match_record(record: dict[str, Any], query: str, field: str) -> bool:
    if not query:
        return True

    query_lower = query.lower()
    if field:
        value = record.get(field, "")
        return query_lower in str(value).lower()

    for value in record.values():
        if query_lower in str(value).lower():
            return True
    return False


def build_frontend_html() -> str:
    return """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Consulta JSON SGA</title>
  <style>
    :root {
      --bg: #f4efe7;
      --panel: rgba(255, 255, 255, 0.92);
      --ink: #1f2933;
      --muted: #52606d;
      --accent: #a63a24;
      --accent-2: #0f766e;
      --line: rgba(31, 41, 51, 0.12);
      --shadow: 0 18px 60px rgba(52, 36, 24, 0.16);
      --radius: 18px;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(166, 58, 36, 0.18), transparent 28%),
        radial-gradient(circle at top right, rgba(15, 118, 110, 0.16), transparent 24%),
        linear-gradient(160deg, #f7f2ea 0%, #ebe2d5 100%);
      min-height: 100vh;
    }

    .wrap {
      width: min(1200px, calc(100% - 32px));
      margin: 32px auto;
      display: grid;
      gap: 20px;
    }

    .hero, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }

    .hero {
      padding: 28px;
      display: grid;
      gap: 10px;
    }

    .hero h1 {
      margin: 0;
      font-size: clamp(28px, 4vw, 44px);
      line-height: 1;
    }

    .hero p {
      margin: 0;
      color: var(--muted);
      max-width: 780px;
    }

    .toolbar {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      padding: 20px;
    }

    label {
      display: grid;
      gap: 6px;
      font-size: 14px;
      color: var(--muted);
    }

    input, select, button {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px 14px;
      font: inherit;
      color: var(--ink);
      background: white;
    }

    button {
      align-self: end;
      background: linear-gradient(135deg, var(--accent), #d97706);
      border: none;
      color: white;
      cursor: pointer;
      font-weight: 600;
    }

    button.secondary {
      background: linear-gradient(135deg, var(--accent-2), #0ea5a3);
    }

    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      padding: 0 20px 20px;
      color: var(--muted);
      font-size: 14px;
    }

    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      padding: 20px;
    }

    .card {
      padding: 16px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(244,239,231,0.82));
    }

    .card h2 {
      margin: 0 0 8px;
      font-size: 18px;
    }

    .card strong {
      display: block;
      margin-bottom: 8px;
      font-size: 26px;
    }

    .results {
      padding: 0 20px 20px;
      overflow: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 820px;
      background: white;
      border-radius: 14px;
      overflow: hidden;
    }

    th, td {
      text-align: left;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      font-size: 14px;
    }

    th {
      position: sticky;
      top: 0;
      background: #f9f5ef;
      z-index: 1;
    }

    .empty {
      padding: 20px;
      color: var(--muted);
    }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <h1>Consulta de JSON por tipo</h1>
      <p>Leitura local dos arquivos em <code>jsons/</code> com filtro por tipo, campo e texto. A pagina consome a API do proprio <code>read.py</code>.</p>
    </section>

    <section class="panel">
      <div class="cards" id="cards"></div>
      <div class="toolbar">
        <label>Tipo
          <select id="tipo"></select>
        </label>
        <label>Campo
          <select id="campo"></select>
        </label>
        <label>Buscar
          <input id="busca" type="text" placeholder="nome, codigo, email...">
        </label>
        <label>Limite
          <input id="limite" type="number" min="1" max="1000" value="100">
        </label>
        <button id="aplicar">Consultar</button>
        <button id="limpar" class="secondary" type="button">Limpar</button>
      </div>
      <div class="meta" id="meta"></div>
      <div class="results">
        <table id="tabela" hidden>
          <thead id="thead"></thead>
          <tbody id="tbody"></tbody>
        </table>
        <div class="empty" id="empty">Carregando...</div>
      </div>
    </section>
  </main>

  <script>
    const state = {
      summary: {},
      currentType: "",
    };

    async function fetchJson(url) {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error("Falha ao consultar " + url);
      }
      return response.json();
    }

    function fillTypeOptions(summary) {
      const select = document.getElementById("tipo");
      select.innerHTML = "";
      for (const tipo of Object.keys(summary).sort()) {
        const option = document.createElement("option");
        option.value = tipo;
        option.textContent = tipo + " (" + summary[tipo].registros + ")";
        select.appendChild(option);
      }
      state.currentType = select.value || "";
      fillFieldOptions();
    }

    function fillFieldOptions() {
      const fieldSelect = document.getElementById("campo");
      const typeInfo = state.summary[state.currentType];
      fieldSelect.innerHTML = "";

      const allOption = document.createElement("option");
      allOption.value = "";
      allOption.textContent = "Todos os campos";
      fieldSelect.appendChild(allOption);

      if (!typeInfo) {
        return;
      }

      for (const field of typeInfo.campos) {
        const option = document.createElement("option");
        option.value = field;
        option.textContent = field;
        fieldSelect.appendChild(option);
      }
    }

    function renderCards(summary) {
      const cards = document.getElementById("cards");
      cards.innerHTML = "";
      for (const tipo of Object.keys(summary).sort()) {
        const info = summary[tipo];
        const card = document.createElement("article");
        card.className = "card";
        card.innerHTML = `
          <h2>${tipo}</h2>
          <strong>${info.registros}</strong>
          <div>${info.campos.length} campos</div>
          <div>${info.arquivos.join(", ")}</div>
        `;
        card.addEventListener("click", () => {
          document.getElementById("tipo").value = tipo;
          state.currentType = tipo;
          fillFieldOptions();
          loadRecords();
        });
        cards.appendChild(card);
      }
    }

    function getVisibleHeaders(payload) {
      const headers = Object.keys(payload.registros[0] || {});
      const preferredHeadersByType = {
        usuario: [
          "cod_usuario",
          "nome_usuario",
          "email",
          "ativo",
          "Tipo_acesso",
          "cod_gestor",
          "funcao",
          "inicio_validade",
          "final_validade",
          "dt_ultimo_acesso",
        ],
        programas: [
          "cod_programa",
          "descricao_programa",
          "codigo_rotina",
          "descricao_rotina",
          "descricao_modulo",
          "cod_modulo",
          "idEmpresa",
          "visualiza_menu",
          "registro_padrao",
          "upc",
        ],
      };

      const preferredHeaders = preferredHeadersByType[payload.tipo];
      if (!preferredHeaders) {
        return headers;
      }

      return preferredHeaders.filter((header) => headers.includes(header));
    }

    function renderTable(payload) {
      const table = document.getElementById("tabela");
      const thead = document.getElementById("thead");
      const tbody = document.getElementById("tbody");
      const empty = document.getElementById("empty");
      const meta = document.getElementById("meta");

      if (!payload.registros.length) {
        table.hidden = true;
        empty.hidden = false;
        empty.textContent = "Nenhum registro encontrado.";
        return;
      }

      const headers = getVisibleHeaders(payload);
      const hiddenColumns = headers.length !== Object.keys(payload.registros[0]).length
        ? ` | Colunas exibidas: ${headers.length} de ${Object.keys(payload.registros[0]).length}`
        : "";

      meta.textContent = `Tipo: ${payload.tipo} | Total filtrado: ${payload.total_filtrado} | Exibindo: ${payload.registros.length}${hiddenColumns}`;

      thead.innerHTML = "<tr>" + headers.map((header) => `<th>${header}</th>`).join("") + "</tr>";
      tbody.innerHTML = payload.registros.map((registro) => {
        return "<tr>" + headers.map((header) => `<td>${String(registro[header] ?? "")}</td>`).join("") + "</tr>";
      }).join("");

      empty.hidden = true;
      table.hidden = false;
    }

    async function loadSummary() {
      const summary = await fetchJson("/api/summary");
      state.summary = summary.tipos;
      renderCards(summary.tipos);
      fillTypeOptions(summary.tipos);
    }

    async function loadRecords() {
      state.currentType = document.getElementById("tipo").value;
      const field = document.getElementById("campo").value;
      const query = encodeURIComponent(document.getElementById("busca").value.trim());
      const limit = encodeURIComponent(document.getElementById("limite").value || "100");
      const fieldParam = encodeURIComponent(field);
      const tipoParam = encodeURIComponent(state.currentType);
      const payload = await fetchJson(`/api/records?tipo=${tipoParam}&field=${fieldParam}&q=${query}&limit=${limit}`);
      renderTable(payload);
    }

    async function start() {
      try {
        await loadSummary();
        await loadRecords();
      } catch (error) {
        const empty = document.getElementById("empty");
        empty.hidden = false;
        empty.textContent = error.message;
      }
    }

    document.getElementById("tipo").addEventListener("change", () => {
      state.currentType = document.getElementById("tipo").value;
      fillFieldOptions();
      loadRecords();
    });
    document.getElementById("aplicar").addEventListener("click", loadRecords);
    document.getElementById("limpar").addEventListener("click", () => {
      document.getElementById("busca").value = "";
      document.getElementById("campo").value = "";
      document.getElementById("limite").value = "100";
      loadRecords();
    });

    start();
  </script>
</body>
</html>
"""


class QueryServiceHandler(BaseHTTPRequestHandler):
    server_version = "JsonQueryService/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.respond_html(build_frontend_html())
            return

        if parsed.path == "/api/summary":
            self.respond_json(
                {
                    "tipos": self.server.summary,  # type: ignore[attr-defined]
                    "total_tipos": len(self.server.summary),  # type: ignore[attr-defined]
                }
            )
            return

        if parsed.path == "/api/records":
            self.handle_records_request(parsed.query)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Rota nao encontrada")

    def handle_records_request(self, raw_query: str) -> None:
        params = parse_qs(raw_query)
        tipo = params.get("tipo", [""])[0]
        query = params.get("q", [""])[0].strip()
        field = params.get("field", [""])[0].strip()
        limit_text = params.get("limit", [str(DEFAULT_LIMIT)])[0]

        try:
            limit = max(1, min(int(limit_text), 1000))
        except ValueError:
            limit = DEFAULT_LIMIT

        data = self.server.records_by_type  # type: ignore[attr-defined]
        summary = self.server.summary  # type: ignore[attr-defined]

        if not tipo:
            tipo = next(iter(data), "")

        if tipo not in data:
            self.respond_json(
                {
                    "tipo": tipo,
                    "total_filtrado": 0,
                    "registros": [],
                    "campos_disponiveis": [],
                }
            )
            return

        filtered = [
            record
            for record in data[tipo]
            if match_record(record, query=query, field=field)
        ]

        self.respond_json(
            {
                "tipo": tipo,
                "total_filtrado": len(filtered),
                "registros": filtered[:limit],
                "campos_disponiveis": summary[tipo]["campos"],
            }
        )

    def log_message(self, format: str, *args: Any) -> None:
        return

    def respond_html(self, body: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def respond_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def create_server(host: str, port: int) -> ThreadingHTTPServer:
    records_by_type, summary = load_json_records()
    server = ThreadingHTTPServer((host, port), QueryServiceHandler)
    server.records_by_type = records_by_type  # type: ignore[attr-defined]
    server.summary = summary  # type: ignore[attr-defined]
    return server


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Servico local para consultar os JSONs por tipo."
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host para bind do servico.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Porta HTTP.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Apenas valida a leitura dos arquivos e imprime um resumo.",
    )
    return parser


def run_check() -> int:
    _, summary = load_json_records()
    print(json.dumps({"tipos": summary, "total_tipos": len(summary)}, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    args = build_cli().parse_args()

    if args.check:
        return run_check()

    server = create_server(args.host, args.port)
    print(f"Servico iniciado em http://{args.host}:{args.port}")
    print("Pressione Ctrl+C para encerrar.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
