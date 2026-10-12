"""Сборка списка файлов безлимитного фото-хранилища (photoslice) через web API.

Использует cookies.json (экспортированные из браузера расширением Cookie-Editor).
Запоминает все файлы в unlim_state.db со статусом 'pending'. Само скачивание —
в unlim_worker.py (можно запускать несколько параллельно).

CLI:
  python unlim_download.py --collect    # собрать список из photoslice
  python unlim_download.py --status     # показать статус БД
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import threading
import time

import requests
from dotenv import load_dotenv
from tqdm import tqdm

from unlim_http import (PHOTO_EXTS, ApiError, AuthError, call_model, describe_cookies,
                        get_sk_uid, load_cookies, make_session)

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT, ".env"))

DB_FILE = os.path.join(PROJECT, "unlim_state.db")
LOG_FILE = os.path.join(PROJECT, "logs", "unlim.log")

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, encoding="utf-8",
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("unlim")


# ── DB ──────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS unlim_files (
    file_id TEXT PRIMARY KEY,
    cluster_id TEXT,
    name TEXT,
    size INTEGER,
    md5 TEXT,
    mime TEXT,
    status TEXT DEFAULT 'pending',
    local_path TEXT,
    error TEXT,
    worker_id INTEGER,
    assigned_at TIMESTAMP,
    downloaded_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_unlim_status ON unlim_files(status);
CREATE TABLE IF NOT EXISTS unlim_clusters (
    cluster_id TEXT PRIMARY KEY,
    size INTEGER,
    fetched INTEGER DEFAULT 0
);
"""

_tls = threading.local()


def db() -> sqlite3.Connection:
    c = getattr(_tls, "c", None)
    if c is None:
        c = sqlite3.connect(DB_FILE, timeout=30, isolation_level=None,
                            check_same_thread=False)
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
        c.row_factory = sqlite3.Row
        _tls.c = c
    return c


def db_init():
    db().executescript(SCHEMA)


# ── HTTP ────────────────────────────────────────────────────────────────────


def api_call(s: requests.Session, sk: str, uid: str, method: str,
             params=None, retries: int = 3):
    last_err = None
    for att in range(retries):
        try:
            return call_model(s, sk, uid, method, params)
        except AuthError:
            raise
        except ApiError as e:
            last_err = str(e)
            if " HTTP 429" in last_err:
                wait = 30 + att * 30
                log.warning(f"{method} HTTP 429 → sleep {wait}")
                time.sleep(wait)
                continue
            log.warning(last_err)
        except Exception as e:
            last_err = f"{method}: {e}"
            log.warning(last_err)
        time.sleep(2 ** att)
    raise RuntimeError(last_err)


# ── Логика сбора photoslice ─────────────────────────────────────────────────


def init_snapshot(s, sk, uid) -> str:
    d = api_call(s, sk, uid, "intapi/photo-init-snapshot")
    psid = d.get("photoslice_id")
    if not psid:
        raise RuntimeError(f"no photoslice_id in {d}")
    return psid


def get_all_clusters(s, sk, uid, psid) -> list[dict]:
    """Получаем плоский список всех кластеров (дни)."""
    snap = api_call(s, sk, uid, "intapi/photo-get-snapshot",
                    {"id": psid, "locale": "ru",
                     "amount": 100000, "offset": 0})
    return snap.get("items", [])


def save_clusters(items: list[dict]):
    c = db()
    c.execute("BEGIN IMMEDIATE")
    try:
        for it in items:
            c.execute(
                "INSERT OR IGNORE INTO unlim_clusters (cluster_id, size) VALUES (?, ?)",
                (it.get("id"), int(it.get("size", 0) or 0)),
            )
        c.execute("COMMIT")
    except Exception:
        try: c.execute("ROLLBACK")
        except sqlite3.Error: pass
        raise


def fetch_cluster_resources(s, sk, uid, psid, cluster_rows: list[dict]) -> int:
    """Запрашивает файлы для пачки кластеров."""
    clusters_map = {}
    for row in cluster_rows:
        size = max(int(row["size"] or 0), 1)
        clusters_map[row["cluster_id"]] = {"range": [0, max(size - 1, 0)]}
    res = api_call(s, sk, uid, "intapi/photo-get-clusters-with-resources", {
        "photosliceId": psid,
        "clusters": clusters_map,
        "hideScreenshots": False,
    })
    fetched = (res.get("clusters") or {}).get("fetched") or []
    if not getattr(fetch_cluster_resources, "_sampled", False):
        sample = next((f for cl in fetched for f in cl.get("items") or []), None)
        if sample is not None:
            # Один пример элемента — чтобы видеть, в каком виде приходит id
            log.info(f"sample item: {json.dumps(sample, ensure_ascii=False)[:1000]}")
            tqdm.write(f"  пример id файла: {sample.get('id') or sample.get('resource_id')!r}")
            fetch_cluster_resources._sampled = True
    added = 0
    c = db()
    c.execute("BEGIN IMMEDIATE")
    try:
        for cl in fetched:
            cid = cl.get("id")
            for f in cl.get("items", []) or []:
                fid = f.get("id") or f.get("resource_id")
                if not fid:
                    continue
                name = f.get("name") or fid.rsplit("/", 1)[-1]
                size = int(f.get("size", 0) or 0)
                md5 = f.get("md5") or f.get("etag")
                mime = f.get("mimetype") or f.get("type")
                c.execute(
                    """INSERT INTO unlim_files
                       (file_id, cluster_id, name, size, md5, mime, status)
                       VALUES (?, ?, ?, ?, ?, ?, 'pending')
                       ON CONFLICT(file_id) DO NOTHING""",
                    (fid, cid, name, size, md5, mime),
                )
                added += 1
            c.execute("UPDATE unlim_clusters SET fetched=1 WHERE cluster_id=?",
                      (cid,))
        c.execute("COMMIT")
    except Exception:
        try: c.execute("ROLLBACK")
        except sqlite3.Error: pass
        raise
    return added


# ── CLI ─────────────────────────────────────────────────────────────────────


def cmd_collect():
    db_init()
    cookies = load_cookies()
    print(f"  cookies.json: {describe_cookies(cookies)}")
    log.info(f"cookies: {describe_cookies(cookies)}")
    s = make_session(cookies)
    try:
        sk, uid = get_sk_uid(s, log)
        print(f"  ✓ uid={uid}, sk OK")

        psid = init_snapshot(s, sk, uid)
        print(f"  ✓ photoslice_id получен")

        clusters = get_all_clusters(s, sk, uid, psid)
        print(f"  ✓ кластеров (дней с фото): {len(clusters)}")
        save_clusters(clusters)

        todo = [dict(r) for r in db().execute(
            "SELECT cluster_id, size FROM unlim_clusters WHERE fetched=0"
        )]
        print(f"\nЗапрашиваю файлы для {len(todo)} кластеров...")
        BATCH = 15
        bar = tqdm(total=len(todo), unit="cluster")
        added_total = 0
        errors = 0
        try:
            for i in range(0, len(todo), BATCH):
                batch = todo[i:i + BATCH]
                try:
                    n = fetch_cluster_resources(s, sk, uid, psid, batch)
                    added_total += n
                except AuthError:
                    raise
                except Exception as e:
                    errors += 1
                    log.error(f"batch {i}: {e}")
                    if errors <= 3:
                        tqdm.write(f"  ✗ пачка {i}: {e}")
                    time.sleep(2)
                bar.update(len(batch))
                bar.set_postfix(files=added_total)
        finally:
            bar.close()

        stats = {r["status"]: r["n"] for r in db().execute(
            "SELECT status, COUNT(*) AS n FROM unlim_files GROUP BY status"
        )}
        print(f"\n  ✓ Файлов в БД: {sum(stats.values())}")
        for k, v in stats.items():
            print(f"     {k}: {v}")
        if errors:
            print(f"  ⚠ пачек с ошибкой: {errors} (подробности в {LOG_FILE})")
            if not added_total:
                sys.exit("Ни одного файла не собрано — см. ошибки выше")
    finally:
        try: s.close()
        except Exception: pass


def cmd_status():
    if not os.path.exists(DB_FILE):
        print("БД пуста — запусти --collect")
        return
    db_init()
    stats = {r["status"]: r["n"] for r in db().execute(
        "SELECT status, COUNT(*) AS n FROM unlim_files GROUP BY status"
    )}
    total = sum(stats.values())
    print(f"Всего файлов в безлимите: {total}")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    recent = db().execute(
        "SELECT COUNT(*) FROM unlim_files WHERE status='downloaded' "
        "AND downloaded_at > datetime('now', '-5 minutes')").fetchone()[0]
    print(f"Скачано за последние 5 минут: {recent}")
    try:
        workers = db().execute(
            "SELECT * FROM unlim_workers ORDER BY worker_id").fetchall()
    except sqlite3.OperationalError:
        workers = []  # таблицу создаёт unlim_worker.py при первом запуске
    if workers:
        now = time.time()
        print("\nВоркеры:")
        for w in workers:
            line = f"  w{w['worker_id']:<3} {w['state'] or '—':<12} скачал {w['files_done'] or 0:>5}"
            if w["state"] == "running" and w["file_id"]:
                secs = int(now - (w["file_started"] or now))
                mark = "  ← залип?" if secs > 60 else ""
                line += f"  сейчас: {w['file_id']} ({secs}с){mark}"
            elif w["last_error"]:
                line += f"  последняя ошибка: {w['last_error'][:100]}"
            print(line)


# Та же метка, что ставит WebUI при выборе категорий — его «Скачать выбранное»
# умеет её снимать, и наоборот
SKIP_MARKER = "not in selected categories"


def is_photo(file_id: str, name: str | None, mime: str | None) -> bool:
    """Сначала по MIME (image/... или голый type от photoslice), потом по расширению."""
    kind = (mime or "").lower().split("/", 1)[0]
    if kind == "image":
        return True
    if kind in ("video", "audio", "application", "text"):
        return False
    for s in (name, file_id):
        if s and os.path.splitext(s)[1].lower() in PHOTO_EXTS:
            return True
    return False


def cmd_photos_only():
    """Помечает pending не-фото как skipped — воркеры их не берут."""
    if not os.path.exists(DB_FILE):
        sys.exit("БД пуста — запусти --collect")
    db_init()
    c = db()
    rows = c.execute(
        "SELECT file_id, name, mime FROM unlim_files WHERE status='pending'"
    ).fetchall()
    skip = [r for r in rows if not is_photo(r["file_id"], r["name"], r["mime"])]
    c.execute("BEGIN IMMEDIATE")
    try:
        c.executemany(
            "UPDATE unlim_files SET status='skipped', error=? WHERE file_id=?",
            [(SKIP_MARKER, r["file_id"]) for r in skip],
        )
        c.execute("COMMIT")
    except Exception:
        try: c.execute("ROLLBACK")
        except sqlite3.Error: pass
        raise
    print(f"Только фото: оставлено {len(rows) - len(skip)}, пропущено {len(skip)}")
    kinds: dict[str, int] = {}
    for r in skip:
        k = (r["mime"] or os.path.splitext(r["name"] or r["file_id"])[1] or "?").lower()
        kinds[k] = kinds.get(k, 0) + 1
    for k, n in sorted(kinds.items(), key=lambda kv: -kv[1])[:10]:
        print(f"  пропущено {k}: {n}")


def cmd_all_types():
    """Откатывает --photos-only: возвращает пропущенные по типу в pending."""
    if not os.path.exists(DB_FILE):
        sys.exit("БД пуста — запусти --collect")
    db_init()
    n = db().execute(
        "UPDATE unlim_files SET status='pending', error=NULL "
        "WHERE status='skipped' AND error=?", (SKIP_MARKER,),
    ).rowcount
    print(f"Возвращено в очередь: {n}")


def cmd_retry_failed():
    """Возвращает failed в очередь (например, после сбоя хранилища Яндекса)."""
    if not os.path.exists(DB_FILE):
        sys.exit("БД пуста — запусти --collect")
    db_init()
    rows = db().execute(
        "SELECT error, COUNT(*) AS n FROM unlim_files WHERE status='failed' "
        "GROUP BY error ORDER BY n DESC LIMIT 5").fetchall()
    for r in rows:
        print(f"  {r['n']}× {r['error']}")
    try:
        n = db().execute(
            "UPDATE unlim_files SET status='pending', error=NULL, retries=0, "
            "next_try_at=NULL WHERE status='failed'").rowcount
    except sqlite3.OperationalError:
        # Колонок retries/next_try_at ещё нет — их добавляет unlim_worker.py
        n = db().execute(
            "UPDATE unlim_files SET status='pending', error=NULL WHERE status='failed'"
        ).rowcount
    print(f"Возвращено в очередь: {n}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--collect", action="store_true",
                    help="Собрать список файлов из photoslice")
    ap.add_argument("--photos-only", action="store_true",
                    help="Качать только фото: остальное пометить skipped "
                         "(можно вместе с --collect)")
    ap.add_argument("--all-types", action="store_true",
                    help="Отменить --photos-only")
    ap.add_argument("--retry-failed", action="store_true",
                    help="Вернуть failed в очередь")
    ap.add_argument("--status", action="store_true",
                    help="Показать статус БД")
    args = ap.parse_args()
    if not (args.collect or args.photos_only or args.all_types
            or args.retry_failed or args.status):
        ap.print_help()
        return
    if args.collect:
        cmd_collect()
    if args.all_types:
        cmd_all_types()
    if args.photos_only:
        cmd_photos_only()
    if args.retry_failed:
        cmd_retry_failed()
    if args.status:
        cmd_status()


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except RuntimeError as e:  # AuthError/ApiError и исчерпанные ретраи
        log.error(str(e))
        sys.exit(f"✗ {e}")
    except KeyboardInterrupt:
        sys.exit(130)
