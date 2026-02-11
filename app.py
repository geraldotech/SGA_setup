from flask import Flask, render_template, jsonify, request
from pathlib import Path
from datetime import datetime
import json
import secrets

app = Flask(__name__)

ACTION_TOKEN = "troque-este-token"

ROOT_DIR = Path(__file__).resolve().parent


def load_status(status_file: Path):
    if not status_file.exists():
        return {"ok": False, "last_run": None, "file": None, "error": None}
    return json.loads(status_file.read_text(encoding="utf-8"))


def save_status(status_file: Path, data: dict):
    status_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def require_token(payload: dict):
    return payload.get("token") == ACTION_TOKEN


def run_create_txt(action_name: str, status_file: Path):
    status = load_status(status_file)

    if status.get("ok") is True:
        return jsonify({"ok": True, "message": f"{action_name}: já executada", "status": status})

    try:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        rid = secrets.token_hex(3)
        filename = f"{action_name}_{ts}_{rid}.txt"
        file_path = ROOT_DIR / filename

        file_path.write_text(f"{action_name} em {datetime.now().isoformat(timespec='seconds')}\n", encoding="utf-8")

        new_status = {
            "ok": True,
            "last_run": datetime.now().isoformat(timespec="seconds"),
            "file": str(file_path),
            "error": None
        }
        save_status(status_file, new_status)

        return jsonify({"ok": True, "message": f"{action_name}: sucesso", "file": filename})

    except Exception as e:
        new_status = {
            "ok": False,
            "last_run": datetime.now().isoformat(timespec="seconds"),
            "file": None,
            "error": str(e)
        }
        save_status(status_file, new_status)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/status/<acao>")
def status(acao):
    if acao == "1":
        return jsonify(load_status(ROOT_DIR / "status_acao1.json"))
    if acao == "2":
        return jsonify(load_status(ROOT_DIR / "status_acao2.json"))
    return jsonify({"ok": False, "error": "Ação inválida"}), 404


@app.post("/criar-txt")
def acao1():
    payload = request.get_json(silent=True) or {}
    if not require_token(payload):
        return jsonify({"ok": False, "error": "Token inválido"}), 403

    return run_create_txt("acao1", ROOT_DIR / "status_acao1.json")


@app.post("/criar-txt-2")
def acao2():
    payload = request.get_json(silent=True) or {}
    if not require_token(payload):
        return jsonify({"ok": False, "error": "Token inválido"}), 403

    return run_create_txt("acao2", ROOT_DIR / "status_acao2.json")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5052, debug=True)
