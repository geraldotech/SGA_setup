import os
import time
import smtplib
import ssl
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from threading import Thread, Event
from email.message import EmailMessage
from dotenv import load_dotenv
load_dotenv()

import pymysql
from flask import Flask, jsonify, request

# =====================================================
# CONFIGURAÇÕES
# =====================================================
APP_NAME = "MailWorker"
APP_PORT = 5001

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "sua_base")

OUTBOX_TABLE = os.getenv("OUTBOX_TABLE", "email_outbox")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))  # a cada quantos segundos checa de novo
AUTO_START = os.getenv("AUTO_START", "1") == "1"



# =====================================================
# LOGGER
# =====================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "info.log")

logger = logging.getLogger(APP_NAME)
logger.setLevel(logging.INFO)
logger.propagate = False

# =====================================================
# LOGGER DE SUCESSO (emails enviados)
# =====================================================
SUCCESS_LOG_FILE = os.path.join(LOG_DIR, "success.log")

success_logger = logging.getLogger("MailWorkerSuccess")
success_logger.setLevel(logging.INFO)
success_logger.propagate = False

if not success_logger.handlers:
    success_handler = RotatingFileHandler(
        SUCCESS_LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    success_formatter = logging.Formatter(
        "[%(asctime)s] %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )
    success_handler.setFormatter(success_formatter)
    success_logger.addHandler(success_handler)

if not logger.handlers:
    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger.info("======================================")
logger.info("Inicializando %s", APP_NAME)
logger.info("Log: %s", LOG_FILE)
logger.info("DB: %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
logger.info("OUTBOX_TABLE: %s", OUTBOX_TABLE)
logger.info("BATCH_SIZE: %s | POLL_SECONDS: %s | AUTO_START: %s", BATCH_SIZE, POLL_SECONDS, AUTO_START)
logger.info("======================================")

# =====================================================
# FLASK
# =====================================================
app = Flask(__name__)

# =====================================================
# CONTROLE DO WORKER
# =====================================================
stop_event = Event()
kick_event = Event()  # para "acordar" o loop quando chamarem /start

# =====================================================
# DB HELPERS
# =====================================================
def db_conn():
    logger.info("Conectando ao banco %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )
    logger.info("Conectado ao banco com sucesso")
    return conn

def ensure_table_exists(conn):
    # Checa se a tabela existe (log amigável)
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES LIKE %s", (OUTBOX_TABLE,))
        ok = cur.fetchone() is not None
    if ok:
        logger.info("Tabela '%s' encontrada", OUTBOX_TABLE)
    else:
        logger.error("Tabela '%s' NÃO encontrada", OUTBOX_TABLE)
    return ok

def get_email_config(conn):
    logger.info("Carregando config SMTP da prm_email")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT email, senha, remetente, smtp, portaSmtp, autenticacao
            FROM prm_email
            LIMIT 1
        """)
        row = cur.fetchone()

    if not row:
        raise Exception("Config SMTP não encontrada (prm_email vazia?)")

    row["portaSmtp"] = int(row.get("portaSmtp") or 0)
    row["autenticacao"] = int(row.get("autenticacao") or 0)

    logger.info(
        "SMTP OK | servidor=%s porta=%s auth=%s remetente=%s",
        row["smtp"], row["portaSmtp"], row["autenticacao"], row["remetente"] or row["email"]
    )
    return row

def claim_emails(conn, limit):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, to_email, subject, body_html, body_text, attempts
            FROM {OUTBOX_TABLE}
            WHERE status = 0
            ORDER BY id
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

    if rows:
        logger.info("Encontrados %s emails pendentes (status=0)", len(rows))
    else:
        logger.info("Nenhum email pendente (status=0)")

    return rows


def mark_sent(conn, email_id):
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {OUTBOX_TABLE} SET status=1, sent_at=NOW(), last_error=NULL WHERE id=%s",
            (email_id,)
        )

def mark_failed(conn, email_id, err):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {OUTBOX_TABLE}
            SET status = -1,
                attempts = attempts + 1,
                last_error = %s
            WHERE id = %s
            """,
            (str(err)[:8000], email_id)
        )


# =====================================================
# SMTP SEND
# =====================================================
def send_email(cfg, to_email, subject, html, text):
    msg = EmailMessage()
    msg["From"] = cfg.get("remetente") or cfg["email"]
    msg["To"] = to_email
    msg["Subject"] = subject

    msg.set_content(text or "")
    if html:
        msg.add_alternative(html, subtype="html")

    host = cfg["smtp"]
    port = int(cfg["portaSmtp"])
    auth = int(cfg["autenticacao"]) == 1

    # 465 = SSL direto, outros tenta STARTTLS
    if port == 465:
        with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(), timeout=30) as server:
            if auth:
                server.login(cfg["email"], cfg["senha"])
            server.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=30) as server:
            server.ehlo()
            try:
                server.starttls(context=ssl.create_default_context())
                server.ehlo()
            except Exception:
                pass
            if auth:
                server.login(cfg["email"], cfg["senha"])
            server.send_message(msg)

# =====================================================
# PROCESSADOR (1 ciclo)
# =====================================================
def process_once(limit=BATCH_SIZE):
    conn = None
    sent = 0
    failed = 0

    try:
        conn = db_conn()

        # checar tabela outbox
        if not ensure_table_exists(conn):
            return {"ok": False, "sent": 0, "failed": 0, "error": "outbox_table_missing"}

        # carregar config SMTP
        cfg = get_email_config(conn)

        # reservar emails pendentes
        emails = claim_emails(conn, limit)
        conn.commit()

        if not emails:
            logger.info("Sem emails pendentes no momento")
            return {"ok": True, "sent": 0, "failed": 0}

        logger.info("Processando %s emails", len(emails))

        for e in emails:
            try:
                send_email(cfg, e["to_email"], e["subject"], e.get("body_html"), e.get("body_text"))
                mark_sent(conn, e["id"])
                conn.commit()
                sent += 1
                success_logger.info(
                    "ENVIADO | id=%s | to=%s | subject='%s'",
                    e["id"], e["to_email"], e["subject"]
                )

            except Exception as ex:
                mark_failed(conn, e["id"], ex)
                conn.commit()
                failed += 1
                logger.error("Falha id=%s | %s", e["id"], ex)

        logger.info("Ciclo finalizado | enviados=%s falhas=%s", sent, failed)
        return {"ok": True, "sent": sent, "failed": failed}

    except Exception as e:
        logger.exception("Erro geral no process_once: %s", e)
        if conn:
            conn.rollback()
        return {"ok": False, "sent": 0, "failed": 0, "error": str(e)}

    finally:
        if conn:
            conn.close()

# =====================================================
# LOOP EM BACKGROUND (auto-start)
# =====================================================
def worker_loop():
    logger.info("Worker loop iniciado (auto)")

    # Dispara um ciclo logo ao subir
    process_once(BATCH_SIZE)

    while not stop_event.is_set():
        # Espera pelo kick_event OU timeout (poll)
        kicked = kick_event.wait(timeout=POLL_SECONDS)
        kick_event.clear()

        if stop_event.is_set():
            break

        if kicked:
            logger.info("Kick recebido (rota /start). Executando ciclo imediatamente.")
        else:
            logger.info("Tick (poll). Executando ciclo periódico.")

        process_once(BATCH_SIZE)

    logger.info("Worker loop finalizado")

# =====================================================
# ROTAS
# =====================================================
@app.get("/health")
def health():
    return jsonify({"ok": True, "service": APP_NAME})

@app.post("/start")
def start():
    logger.info("Rota /start acionada | IP=%s", request.remote_addr)

    def _bg():
        process_once(BATCH_SIZE)

    Thread(target=_bg, daemon=True).start()
    return jsonify({"ok": True, "message": "Processamento iniciado em background"}), 200




@app.post("/run-once")
def run_once():
    """
    Se você quiser processar apenas 1 ciclo e receber retorno (bom para debug)
    """
    limit = request.json.get("limit") if request.is_json else None
    limit = int(limit) if limit else BATCH_SIZE
    limit = max(1, min(limit, 200))
    res = process_once(limit)
    return jsonify(res), (200 if res.get("ok") else 500)

@app.post("/stop")
def stop():
    """
    Opcional: parar o loop (se você estiver rodando em modo simples)
    """
    logger.info("Rota /stop acionada | IP=%s", request.remote_addr)
    stop_event.set()
    kick_event.set()
    return jsonify({"ok": True, "message": "Stop solicitado"}), 200

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    if AUTO_START:
        Thread(target=worker_loop, daemon=True).start()

    logger.info("Flask rodando na porta %s", APP_PORT)
    app.run(host="0.0.0.0", port=APP_PORT, debug=False, use_reloader=False)

