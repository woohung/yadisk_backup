"""Параллельный воркер скачивания безлимита (photoslice).

Берёт pending файлы пачками, для каждой просит Яндекс собрать ZIP,
скачивает, распаковывает в DOWNLOAD_DIR/_unlim/, удаляет ZIP.

Можно запускать несколько процессов параллельно — каждый атомарно
забирает свою пачку из БД и не мешает другим.

Режим --single качает по одному файлу через mpfs/url (как кнопка «Скачать»
в вебе) — работает, даже когда Яндекс отключил архивы (code 394).

Запуск:
  python unlim_worker.py --id 1
  python unlim_worker.py --id 2
  python unlim_worker.py --id 1 --single --limit 20   # проба на 20 файлах
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
import time
import zipfile
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

from unlim_http import (PHOTO_EXTS, AuthError, call_model, describe_cookies,
                        get_sk_uid, load_cookies, make_session)

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT, ".env"))

DB_FILE = os.path.join(PROJECT, "unlim_state.db")
LOG_FILE = os.path.join(PROJECT, "logs", "unlim_worker.log")

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", os.path.join(PROJECT, "downloads")).rstrip("/\\")
EXTRACT_DIR = os.path.join(DOWNLOAD_DIR, "_unlim")
ZIP_TMP_DIR = os.path.join(DOWNLOAD_DIR, "_unlim_tmp_zips")
# Превью (до 1280 px) для фото, чей оригинал Яндекс не отдаёт. Отдельно от
# оригиналов; когда оригинал всё же скачается — превью удаляется
PREVIEW_DIR = os.path.join(DOWNLOAD_DIR, "_unlim_previews")
PREVIEW_SIZE = "XXXL"  # самое крупное: больше 1280 px Яндекс не отдаёт
BATCH_SIZE = 250
MAX_PREPARE_FAILS = 5  # подряд — дальше крутиться бессмысленно
MAX_SINGLE_FAILS = 10  # подряд в режиме --single

# Залипания в --single: ловим быстро и не долбим тот же файл
READ_TIMEOUT = 20          # сек тишины от storage — обрываем
SLOW_CHECK_AFTER = 30      # через столько сек проверяем скорость…
SLOW_MIN_BPS = 64 * 1024   # …и обрываем, если меньше 64 КБ/с
DEFER_STEPS = (300, 900)   # отложить на 5, потом на 15 мин; третья неудача — failed
PAUSE_AFTER_TRANSIENT = 5  # столько подряд таймаутов — хранилище «легло»…
PAUSE_STEPS = tuple(range(5, 121, 5))  # …передышка 5, 10, 15 … 120 с (растёт, пока не будет успеха)
HEARTBEAT_SEC = 15         # как часто воркер отмечается в БД во время закачки
STAGGER_SEC = 0.3          # задержка старта на каждый следующий id


class TransientError(RuntimeError):
    """Временная проблема с конкретной закачкой (медленно, недокачано)."""


class FileLostError(RuntimeError):
    """Хранилище Яндекса не может прочитать файл ("file IO failure: no more
    reliable hosts" и т.п.). Проблема файла, а не воркера; битые файлы идут
    кучками по датам — в «ошибки подряд» не считаем."""


# Файл, который качается прямо сейчас — чтобы при остановке отложить именно его
_current_file: dict[str, str | None] = {"fid": None}
# Сколько пауз подряд без единого успеха — между пачками тоже помним
_pauses_in_row = {"n": 0}

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, encoding="utf-8",
                    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")


# ── Состояние воркеров (для WebUI и --status) ───────────────────────────────

WORKERS_SCHEMA = """
CREATE TABLE IF NOT EXISTS unlim_workers (
    worker_id INTEGER PRIMARY KEY,
    pid INTEGER,
    mode TEXT,
    state TEXT,          -- running / waiting / finished / stopped / failed
    file_id TEXT,        -- что качается прямо сейчас
    file_started REAL,   -- unix time начала текущего файла
    attempt INTEGER,
    files_done INTEGER DEFAULT 0,
    bytes_done INTEGER DEFAULT 0,
    last_ok REAL,
    last_error TEXT,
    last_error_at REAL,
    started REAL,
    updated REAL
);
"""


def ws_set(worker_id: int, **fields):
    """Обновляет строку воркера. Значение-строка вида '+=N' — инкремент.
    Ошибки записи статуса не должны ронять скачивание."""
    fields["updated"] = time.time()
    cols, vals = [], []
    for k, v in fields.items():
        if isinstance(v, str) and v.startswith("+="):
            cols.append(f"{k} = COALESCE({k}, 0) + ?")
            vals.append(int(v[2:]))
        else:
            cols.append(f"{k} = ?")
            vals.append(v)
    try:
        c = sqlite3.connect(DB_FILE, timeout=10)
        try:
            c.execute(WORKERS_SCHEMA)
            c.execute("INSERT OR IGNORE INTO unlim_workers (worker_id) VALUES (?)",
                      (worker_id,))
            c.execute(f"UPDATE unlim_workers SET {', '.join(cols)} WHERE worker_id = ?",
                      vals + [worker_id])
            c.commit()
        finally:
            c.close()
    except sqlite3.Error as e:
        logging.warning(f"w{worker_id} status: {e}")


def ensure_retry_columns():
    """retries/next_try_at — для откладывания залипших файлов (старые БД без них)."""
    c = sqlite3.connect(DB_FILE, timeout=60)
    try:
        cols = {r[1] for r in c.execute("PRAGMA table_info(unlim_files)")}
        if "retries" not in cols:
            c.execute("ALTER TABLE unlim_files ADD COLUMN retries INTEGER DEFAULT 0")
        if "next_try_at" not in cols:
            c.execute("ALTER TABLE unlim_files ADD COLUMN next_try_at REAL")
        if "preview_path" not in cols:
            c.execute("ALTER TABLE unlim_files ADD COLUMN preview_path TEXT")
        if "fails_total" not in cols:
            # Сколько раз файл падал за всё время. «Вернуть ошибки в очередь»
            # его не сбрасывает — по нему take_batch ставит такие файлы в конец
            c.execute("ALTER TABLE unlim_files ADD COLUMN fails_total INTEGER DEFAULT 0")
        c.commit()
    finally:
        c.close()


def take_batch(worker_id: int, size: int) -> list[str]:
    """Атомарно забирает пачку pending под себя (status='in_progress').
    Отложенные файлы (next_try_at в будущем) пропускает."""
    c = sqlite3.connect(DB_FILE, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    try:
        c.execute("BEGIN IMMEDIATE")
        rows = list(c.execute(
            "SELECT file_id FROM unlim_files WHERE status='pending' "
            "AND (next_try_at IS NULL OR next_try_at <= ?) "
            # Сначала ни разу не падавшие: иначе после «вернуть ошибки» вся
            # голова очереди — заведомо проблемные файлы, и воркеры стоят
            "ORDER BY COALESCE(fails_total, 0), rowid LIMIT ?",
            (time.time(), size),
        ))
        if not rows:
            c.execute("COMMIT")
            return []
        ids = [r["file_id"] for r in rows]
        c.executemany(
            "UPDATE unlim_files SET status='in_progress', worker_id=?, "
            "assigned_at=CURRENT_TIMESTAMP WHERE file_id=?",
            [(worker_id, i) for i in ids],
        )
        c.execute("COMMIT")
        return ids
    except Exception:
        try: c.execute("ROLLBACK")
        except sqlite3.Error: pass
        raise
    finally:
        c.close()


def mark_done(file_ids: list[str], size: int | None = None):
    """size — для одного файла в --single: photoslice размеров не отдаёт,
    а без них в превью не посчитать, сколько уже скачано."""
    c = sqlite3.connect(DB_FILE, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    try:
        c.execute("BEGIN IMMEDIATE")
        c.executemany(
            "UPDATE unlim_files SET status='downloaded', downloaded_at=CURRENT_TIMESTAMP, "
            "worker_id=NULL, error=NULL WHERE file_id=?",
            [(i,) for i in file_ids],
        )
        if size is not None and len(file_ids) == 1:
            c.execute("UPDATE unlim_files SET size=? WHERE file_id=?", (size, file_ids[0]))
        # Был скачан только превью — оригинал его заменяет
        marks = ",".join("?" * len(file_ids))
        try:
            olds = [r[0] for r in c.execute(
                f"SELECT preview_path FROM unlim_files WHERE preview_path IS NOT NULL "
                f"AND file_id IN ({marks})", file_ids)]
            c.execute(f"UPDATE unlim_files SET preview_path=NULL WHERE file_id IN ({marks})",
                      file_ids)
        except sqlite3.OperationalError:
            olds = []  # колонки ещё нет — старая БД
        for old in olds:
            try:
                os.remove(old)
            except OSError:
                pass
        c.execute("COMMIT")
    finally:
        c.close()


def mark_pending(file_ids: list[str], error: str):
    c = sqlite3.connect(DB_FILE, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    try:
        c.execute("BEGIN IMMEDIATE")
        c.executemany(
            "UPDATE unlim_files SET status='pending', worker_id=NULL, error=? WHERE file_id=?",
            [(error[:500], i) for i in file_ids],
        )
        c.execute("COMMIT")
    finally:
        c.close()


def mark_failed(file_ids: list[str], error: str):
    """Ошибка конкретного файла (например, Яндекс не может его прочитать) —
    не возвращаем в pending, иначе воркер будет брать его по кругу.
    Повторить: unlim_download.py --retry-failed"""
    c = sqlite3.connect(DB_FILE, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    try:
        c.execute("BEGIN IMMEDIATE")
        c.executemany(
            "UPDATE unlim_files SET status=" + FAILED_OR_PREVIEW + ", worker_id=NULL, "
            "error=?, fails_total=COALESCE(fails_total, 0) + 1 WHERE file_id=?",
            [(error[:500], i) for i in file_ids],
        )
        c.execute("COMMIT")
    finally:
        c.close()


def defer_or_fail(file_id: str, error: str) -> str:
    """Временная ошибка: откладываем файл в конец очереди, после
    len(DEFER_STEPS) откладываний — failed. Возвращает, что сделали."""
    c = sqlite3.connect(DB_FILE, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    try:
        row = c.execute("SELECT retries FROM unlim_files WHERE file_id=?",
                        (file_id,)).fetchone()
        retries = (row[0] or 0) + 1 if row else 1
        if retries > len(DEFER_STEPS):
            c.execute("UPDATE unlim_files SET status=" + FAILED_OR_PREVIEW + ", worker_id=NULL, "
                      "fails_total=COALESCE(fails_total, 0) + 1, "
                      "retries=?, next_try_at=NULL, error=? WHERE file_id=?",
                      (retries, error[:500], file_id))
            what = f"не скачался за {retries} попытки — в ошибки (или остаётся превью)"
        else:
            wait = DEFER_STEPS[retries - 1]
            c.execute("UPDATE unlim_files SET status='pending', worker_id=NULL, "
                      "fails_total=COALESCE(fails_total, 0) + 1, "
                      "retries=?, next_try_at=?, error=? WHERE file_id=?",
                      (retries, time.time() + wait, error[:500], file_id))
            what = f"отложен на {wait // 60} мин"
        c.commit()
        return what
    finally:
        c.close()


def next_deferred() -> float | None:
    """Когда созреет ближайший отложенный файл (None — отложенных нет)."""
    c = sqlite3.connect(DB_FILE, timeout=60)
    try:
        return c.execute("SELECT MIN(next_try_at) FROM unlim_files "
                         "WHERE status='pending' AND next_try_at > ?",
                         (time.time(),)).fetchone()[0]
    finally:
        c.close()


# status для окончательной неудачи с оригиналом: превью уже есть — остаёмся в preview
FAILED_OR_PREVIEW = "CASE WHEN preview_path IS NOT NULL THEN 'preview' ELSE 'failed' END"


def take_preview_batch(worker_id: int, size: int) -> list[str]:
    """Фото со статусом failed (оригинал не дался) — берём за превью."""
    photo_cond = " OR ".join(f"lower(file_id) LIKE '%{e}'" for e in sorted(PHOTO_EXTS))
    c = sqlite3.connect(DB_FILE, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    try:
        c.execute("BEGIN IMMEDIATE")
        ids = [r[0] for r in c.execute(
            f"SELECT file_id FROM unlim_files WHERE status='failed' "
            f"AND preview_path IS NULL AND ({photo_cond}) LIMIT ?", (size,))]
        c.executemany(
            "UPDATE unlim_files SET status='preview_in_progress', worker_id=?, "
            "assigned_at=CURRENT_TIMESTAMP WHERE file_id=?",
            [(worker_id, i) for i in ids])
        c.execute("COMMIT")
        return ids
    except Exception:
        try: c.execute("ROLLBACK")
        except sqlite3.Error: pass
        raise
    finally:
        c.close()


def download_preview(s, sk, uid, file_id: str) -> tuple[str, int]:
    """Качает самое крупное превью (JPEG) в PREVIEW_DIR, дату файла ставит
    по дате снимка. Возвращает (путь, размер)."""
    data = call_model(s, sk, uid, "mpfs/info", {"path": file_id}, timeout=30)
    meta = (data or {}).get("meta") or {}
    url = next((x["url"] for x in meta.get("sizes") or [] if x.get("name") == PREVIEW_SIZE), None)
    if not url:
        raise RuntimeError(f"нет превью {PREVIEW_SIZE} в ответе")
    if url.startswith("//"):
        url = "https:" + url
    name = file_id.replace("\\", "/").rsplit("/", 1)[-1]
    if name in ("", ".", ".."):
        raise RuntimeError(f"странное имя файла: {file_id!r}")
    os.makedirs(PREVIEW_DIR, exist_ok=True)
    dst = os.path.join(PREVIEW_DIR, name + ".preview.jpg")
    with s.get(url, timeout=(10, READ_TIMEOUT)) as r:
        if r.status_code != 200 or not r.content:
            raise RuntimeError(f"превью HTTP {r.status_code}: {r.text[:150].strip()}")
        tmp = dst + ".part"
        with open(tmp, "wb") as f:
            f.write(r.content)
        os.replace(tmp, dst)
    taken = meta.get("photoslice_time") or meta.get("etime") or data.get("etime")
    if taken:
        try:
            os.utime(dst, (taken, taken))
        except (OSError, OverflowError, ValueError):
            pass
    return dst, len(r.content)


def mark_preview(file_id: str, path: str):
    c = sqlite3.connect(DB_FILE, timeout=60)
    try:
        c.execute("UPDATE unlim_files SET status='preview', worker_id=NULL, preview_path=? "
                  "WHERE file_id=?", (path, file_id))
        c.commit()
    finally:
        c.close()


def grab_preview_early(s, sk, uid, worker_id: int, fid: str) -> None:
    """Оригинал фото не дался с первой попытки — сразу забираем превью, не
    дожидаясь, пока файл пройдёт все откладывания и попадёт в failed.
    Статус не трогаем: оригинал дальше пробуем как обычно, а при окончательной
    неудаче FAILED_OR_PREVIEW оставит файл в preview."""
    if os.path.splitext(fid)[1].lower() not in PHOTO_EXTS:
        return
    c = sqlite3.connect(DB_FILE, timeout=60)
    try:
        row = c.execute("SELECT preview_path FROM unlim_files WHERE file_id=?",
                        (fid,)).fetchone()
    finally:
        c.close()
    if not row or row[0]:
        return
    ws_set(worker_id, state="running", file_id=f"превью: {fid}",
           file_started=time.time(), attempt=1)
    try:
        path, n = download_preview(s, sk, uid, fid)
    except Exception as e:  # и AuthError — его поймает следующий оригинал
        logging.warning(f"w{worker_id} early preview {fid}: {e}")
        return
    c = sqlite3.connect(DB_FILE, timeout=60)
    try:
        c.execute("UPDATE unlim_files SET preview_path=? WHERE file_id=?", (path, fid))
        c.commit()
    finally:
        c.close()
    print(f"[w{worker_id}] 🖼 {fid}: взял превью ({n // 1024} КБ)")


def run_preview_batch(s, sk, uid, worker_id: int, paths: list[str]) -> tuple[int, int]:
    files = bytes_ = 0
    for i, fid in enumerate(paths):
        _current_file["fid"] = None  # превью не откладываем при остановке
        ws_set(worker_id, state="running", file_id=f"превью: {fid}",
               file_started=time.time(), attempt=1)
        try:
            path, n = download_preview(s, sk, uid, fid)
        except AuthError as e:
            print(f"[w{worker_id}] ✗ {e}")
            mark_failed(paths[i:], f"auth: {e}")
            return files, -1
        except Exception as e:
            logging.warning(f"w{worker_id} preview {fid}: {e}")
            ws_set(worker_id, last_error=f"превью {fid}: {str(e)[:300]}",
                   last_error_at=time.time())
            # Остаётся failed — превью попробуем в следующий раз
            c = sqlite3.connect(DB_FILE, timeout=60)
            try:
                c.execute("UPDATE unlim_files SET status='failed', worker_id=NULL "
                          "WHERE file_id=?", (fid,))
                c.commit()
            finally:
                c.close()
            continue
        mark_preview(fid, path)
        ws_set(worker_id, files_done="+=1", bytes_done=f"+={n}", last_ok=time.time())
        files += 1
        bytes_ += n
    return files, bytes_


def release_mine(worker_id: int) -> int:
    """При остановке воркера возвращает его in_progress в очередь
    (а взятые за превью — обратно в failed)."""
    c = sqlite3.connect(DB_FILE, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    try:
        c.execute("UPDATE unlim_files SET status='failed', worker_id=NULL "
                  "WHERE status='preview_in_progress' AND worker_id=?", (worker_id,))
        return c.execute(
            "UPDATE unlim_files SET status='pending', worker_id=NULL "
            "WHERE status='in_progress' AND worker_id=?", (worker_id,),
        ).rowcount
    finally:
        c.commit()
        c.close()


def recover_stuck(timeout_sec: int = 1800) -> int:
    """Возвращает в pending файлы которые в in_progress дольше timeout."""
    c = sqlite3.connect(DB_FILE, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    try:
        c.execute("BEGIN IMMEDIATE")
        n = c.execute(
            "UPDATE unlim_files SET status='pending', worker_id=NULL "
            "WHERE status='in_progress' AND "
            "(julianday(CURRENT_TIMESTAMP) - julianday(assigned_at)) * 86400 > ?",
            (timeout_sec,),
        ).rowcount
        c.execute(
            "UPDATE unlim_files SET status='failed', worker_id=NULL "
            "WHERE status='preview_in_progress' AND "
            "(julianday(CURRENT_TIMESTAMP) - julianday(assigned_at)) * 86400 > ?",
            (timeout_sec,))
        c.execute("COMMIT")
        return n
    finally:
        c.close()


# ── HTTP ────────────────────────────────────────────────────────────────────


def prepare_zip(s, sk, uid, paths: list[str]) -> str:
    """Просит Яндекс собрать ZIP. Бросает исключение с телом ответа при ошибке."""
    data = call_model(s, sk, uid, "mpfs/bulk-download-prepare",
                      {"items": paths}, timeout=60)
    url = data.get("download_url") if isinstance(data, dict) else None
    if not url:
        raise RuntimeError(f"нет download_url в ответе: "
                           f"{json.dumps(data, ensure_ascii=False)[:500]}")
    if url.startswith("//"):
        url = "https:" + url
    return url


def download_and_extract(s, url: str, worker_id: int, batch_idx: int) -> tuple[int, int]:
    os.makedirs(ZIP_TMP_DIR, exist_ok=True)
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    zip_path = os.path.join(ZIP_TMP_DIR, f"w{worker_id}_b{batch_idx}.zip")
    try:
        with s.get(url, stream=True, timeout=900) as r:
            r.raise_for_status()
            tmp = zip_path + ".part"
            try:
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(2 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)
                os.replace(tmp, zip_path)
            except OSError as e:
                # disk full / permission denied
                logging.error(f"w{worker_id} b{batch_idx} write: {e}")
                print(f"[w{worker_id}] batch {batch_idx}: запись на диск: {e}")
                try: os.remove(tmp)
                except OSError: pass
                return 0, 0
    except Exception as e:
        logging.error(f"w{worker_id} b{batch_idx} download: {e}")
        print(f"[w{worker_id}] batch {batch_idx}: скачивание ZIP: {e}")
        return 0, 0

    extracted = 0
    total_bytes = 0
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for m in zf.infolist():
                if m.is_dir():
                    continue
                safe = m.filename.replace("\\", "/").lstrip("/")
                if ".." in safe.split("/"):
                    continue
                dst = os.path.join(EXTRACT_DIR, safe)
                if os.path.exists(dst) and os.path.getsize(dst) == m.file_size:
                    extracted += 1
                    total_bytes += m.file_size
                    continue
                os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
                try:
                    with zf.open(m) as src, open(dst, "wb") as f:
                        while True:
                            chunk = src.read(1024 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                    extracted += 1
                    total_bytes += m.file_size
                except Exception as e:
                    logging.warning(f"w{worker_id} extract {safe}: {e}")
    except zipfile.BadZipFile as e:
        logging.error(f"w{worker_id} b{batch_idx} bad zip: {e}")
        print(f"[w{worker_id}] batch {batch_idx}: битый ZIP: {e}")
        return 0, 0
    finally:
        try:
            os.remove(zip_path)
        except OSError:
            pass

    return extracted, total_bytes


def download_single(s, sk, uid, file_id: str, heartbeat=None) -> int:
    """Качает один файл через mpfs/url в EXTRACT_DIR. Возвращает размер."""
    data = call_model(s, sk, uid, "mpfs/url", {"path": file_id}, timeout=60)
    url = data.get("file") if isinstance(data, dict) else None
    if not url:
        raise RuntimeError(f"нет ссылки в ответе: "
                           f"{json.dumps(data, ensure_ascii=False)[:300]}")
    if url.startswith("//"):
        url = "https:" + url
    fsize = int((parse_qs(urlparse(url).query).get("fsize") or ["0"])[0] or 0)

    name = file_id.replace("\\", "/").rsplit("/", 1)[-1]
    if name in ("", ".", ".."):
        raise RuntimeError(f"странное имя файла: {file_id!r}")
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    dst = os.path.join(EXTRACT_DIR, name)
    if fsize and os.path.exists(dst) and os.path.getsize(dst) == fsize:
        return fsize

    tmp = dst + ".part"
    try:
        with s.get(url, stream=True, timeout=(10, READ_TIMEOUT)) as r:
            if r.status_code != 200:
                # Без URL: в нём подписанная ссылка на файл
                msg = f"downloader HTTP {r.status_code}: {r.text[:200].strip()}"
                if "file IO failure" in msg:
                    raise FileLostError(msg)
                raise RuntimeError(msg)
            started, got = time.monotonic(), 0
            beat = started
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(256 * 1024):
                    if chunk:
                        f.write(chunk)
                        got += len(chunk)
                    # Таймаут ловит только полную тишину, а не «капает по байту»
                    if heartbeat and time.monotonic() - beat > HEARTBEAT_SEC:
                        heartbeat()  # большой файл качается долго — «я жив»
                        beat = time.monotonic()
                    el = time.monotonic() - started
                    if el > SLOW_CHECK_AFTER and got / el < SLOW_MIN_BPS:
                        raise TransientError(
                            f"слишком медленно: {got // 1024} КБ за {int(el)}с")
        got = os.path.getsize(tmp)
        if fsize and got != fsize:
            raise TransientError(f"недокачан: {got} из {fsize} байт")
        os.replace(tmp, dst)
        return got
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def run_single_batch(s, sk, uid, worker_id: int, paths: list[str],
                     delay: float) -> tuple[int, int, int]:
    """Качает пачку по одному файлу. Возвращает (files, bytes, rc);
    rc != 0 — воркеру надо выйти, недокачанное уже возвращено в pending."""
    files = bytes_ = fails = transient = 0
    for i, fid in enumerate(paths):
        if transient >= PAUSE_AFTER_TRANSIENT:
            # Подряд одни таймауты — хранилище не отдаёт. Не жжём очередь, ждём
            pause = PAUSE_STEPS[min(_pauses_in_row["n"], len(PAUSE_STEPS) - 1)]
            _pauses_in_row["n"] += 1
            print(f"[w{worker_id}] {transient} таймаутов подряд — пауза {pause}s")
            ws_set(worker_id, state=f"waiting {pause}s (storage)",
                   file_id=None, file_started=None)
            time.sleep(pause)
            transient = 0
        _current_file["fid"] = fid
        for attempt in range(3):
            ws_set(worker_id, state="running", file_id=fid,
                   file_started=time.time(), attempt=attempt + 1)
            try:
                n = download_single(s, sk, uid, fid,
                                    heartbeat=lambda: ws_set(worker_id))
            except AuthError as e:
                print(f"[w{worker_id}] ✗ {e}")
                mark_pending(paths[i:], f"auth: {e}")
                return files, bytes_, 2
            except Exception as e:
                ws_set(worker_id, last_error=f"{fid}: {str(e)[:300]}",
                       last_error_at=time.time())
                if " HTTP 429" in str(e) and attempt < 2:
                    wait = 60 * (attempt + 1)
                    ws_set(worker_id, state=f"waiting {wait}s (429)")
                    print(f"[w{worker_id}] 429 на {fid} — жду {wait}s")
                    time.sleep(wait)
                    continue
                if isinstance(e, (requests.RequestException, TransientError)):
                    # Таймаут/обрыв/медленно на узле storage — не долбим тот же
                    # файл, а откладываем и сразу берём следующий
                    transient += 1
                    grab_preview_early(s, sk, uid, worker_id, fid)
                    what = defer_or_fail(fid, f"single: {e}")
                    logging.info(f"w{worker_id} {fid}: {e} → {what}")
                    print(f"[w{worker_id}] ⏭ {fid}: {str(e)[:150]} → {what}")
                    break
                if not isinstance(e, FileLostError):
                    fails += 1
                logging.warning(f"w{worker_id} single {fid}: {e}")
                print(f"[w{worker_id}] ✗ {fid}: {str(e)[:300]}")
                grab_preview_early(s, sk, uid, worker_id, fid)
                mark_failed([fid], f"single: {e}")
                if fails >= MAX_SINGLE_FAILS:
                    print(f"[w{worker_id}] {fails} ошибок подряд — выхожу")
                    mark_pending(paths[i + 1:], "worker stopped")
                    return files, bytes_, 3
                break
            else:
                fails = transient = 0
                _pauses_in_row["n"] = 0
                mark_done([fid], size=n)
                ws_set(worker_id, files_done="+=1", bytes_done=f"+={n}",
                       last_ok=time.time())
                files += 1
                bytes_ += n
                break
        _current_file["fid"] = None
        if delay:
            time.sleep(delay)
    return files, bytes_, 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int, required=True, help="ID воркера (1, 2, ...)")
    ap.add_argument("--batch", type=int, default=BATCH_SIZE)
    ap.add_argument("--single", action="store_true",
                    help="Качать по одному файлу (mpfs/url), без ZIP-архивов")
    ap.add_argument("--delay", type=float, default=0.3,
                    help="Пауза между файлами в --single, сек")
    ap.add_argument("--limit", type=int, default=0,
                    help="Остановиться после N файлов (для пробы)")
    ap.add_argument("--previews", action="store_true",
                    help="Качать превью (до 1280 px) для фото со статусом failed")
    args = ap.parse_args()
    if args.single and args.batch == BATCH_SIZE:
        args.batch = 50  # мельче пачка — меньше файлов «зависает» при обрыве
    if args.limit:
        args.batch = min(args.batch, args.limit)

    worker_id = args.id
    # WebUI останавливает воркеров через terminate() — это SIGTERM
    signal.signal(signal.SIGTERM, _interrupt)
    try:
        return _run(args, worker_id)
    except KeyboardInterrupt:
        ws_set(worker_id, state="stopped", file_id=None, file_started=None)
        fid = _current_file["fid"]
        if fid:
            # Скорее всего на нём и залип — пусть следующий воркер его не хватает
            print(f"[w{worker_id}] {fid} → {defer_or_fail(fid, 'прерван на этом файле')}")
        n = release_mine(worker_id)
        print(f"[w{worker_id}] остановлен, вернул в очередь {n} файлов")
        return 130


def _interrupt(*_):
    raise KeyboardInterrupt


def _run(args, worker_id: int) -> int:
    print(f"[w{worker_id}] старт. batch={args.batch}"
          f"{', режим: по одному файлу' if args.single else ''}")

    if not os.path.exists(DB_FILE):
        sys.exit("Нет unlim_state.db — сначала запусти unlim_download.py --collect")

    recover_stuck()
    ensure_retry_columns()

    # Разносим старт: десятки воркеров разом дёргают /client/disk за sk —
    # такой всплеск может получить капчу и уронить всех с ошибкой авторизации
    if worker_id > 1:
        time.sleep(min(30.0, ((worker_id - 1) % 100) * STAGGER_SEC))
    try:
        cookies = load_cookies()
        logging.info(f"w{worker_id} cookies: {describe_cookies(cookies)}")
        s = make_session(cookies)
        sk, uid = get_sk_uid(s, logging)
    except AuthError as e:
        logging.error(f"w{worker_id} auth: {e}")
        ws_set(worker_id, pid=os.getpid(), state="failed (auth)",
               last_error=str(e)[:300], last_error_at=time.time())
        sys.exit(f"[w{worker_id}] ✗ {e}")
    print(f"[w{worker_id}] авторизован uid={uid}")
    ws_set(worker_id, pid=os.getpid(),
           mode="preview" if args.previews else "single" if args.single else "zip",
           state="running", started=time.time(), files_done=0, bytes_done=0,
           file_id=None, file_started=None, last_ok=None, last_error=None,
           last_error_at=None)

    batch_idx = 0
    total_files = 0
    total_bytes = 0
    empty_streak = 0
    prepare_fails = 0
    rc = 0

    while True:
        size = args.batch
        if args.limit:
            size = min(size, args.limit - total_files)
        if args.previews:
            paths = take_preview_batch(worker_id, size) if size > 0 else []
            if not paths:
                print(f"[w{worker_id}] превью: больше нечего, выход")
                break
            batch_idx += 1
            files, bytes_ = run_preview_batch(s, sk, uid, worker_id, paths)
            if bytes_ < 0:
                rc = 2
                break
            total_files += files
            total_bytes += bytes_
            print(f"[w{worker_id}] превью, пачка {batch_idx}: ✓ {files}/{len(paths)}, "
                  f"всего {total_files}")
            if args.limit and total_files >= args.limit:
                break
            continue

        paths = take_batch(worker_id, size) if size > 0 else []
        if not paths:
            due = next_deferred()
            if due and not args.limit:
                wait = max(5, min(60, int(due - time.time()) + 1))
                ws_set(worker_id, state=f"waiting {wait}s (отложенные)",
                       file_id=None, file_started=None)
                time.sleep(wait)
                continue
            empty_streak += 1
            if empty_streak >= 3:
                print(f"[w{worker_id}] нет работы, выход")
                break
            time.sleep(5)
            continue
        empty_streak = 0
        batch_idx += 1

        if args.single:
            start = time.monotonic()
            files, bytes_, rc = run_single_batch(s, sk, uid, worker_id, paths,
                                                 args.delay)
            dur = time.monotonic() - start
            total_files += files
            total_bytes += bytes_
            print(f"[w{worker_id}] batch {batch_idx}: ✓ {files}/{len(paths)} files / "
                  f"{bytes_/1e6:.0f} MB ({bytes_/1e6/max(0.1, dur):.1f} MB/s), "
                  f"всего {total_files}")
            if rc or (args.limit and total_files >= args.limit):
                break
            continue

        ws_set(worker_id, state="running", file_id=f"ZIP: пачка {batch_idx} ({len(paths)} файлов)",
               file_started=time.time(), attempt=prepare_fails + 1)
        try:
            url = prepare_zip(s, sk, uid, paths)
        except Exception as e:
            prepare_fails += 1
            ws_set(worker_id, last_error=f"prepare: {str(e)[:300]}", last_error_at=time.time())
            logging.warning(f"w{worker_id} b{batch_idx} prepare: {e}")
            print(f"[w{worker_id}] batch {batch_idx}: prepare FAIL "
                  f"({prepare_fails}/{MAX_PREPARE_FAILS}) → pending: {str(e)[:300]}")
            mark_pending(paths, f"prepare: {e}")
            if '"code":394' in str(e):
                # "Archive downloads are temporarily disabled" — повторы только
                # продлевают блокировку, ждать надо часами, а не секундами
                print(f"[w{worker_id}] Яндекс временно отключил скачивание архивами "
                      f"(code 394) — выхожу. Подожди и запусти позже с 1 воркером")
                rc = 4
                break
            if " HTTP 429" in str(e) and prepare_fails < MAX_PREPARE_FAILS:
                time.sleep(60 * prepare_fails)  # rate limit, сессия тут ни при чём
                continue
            if prepare_fails >= MAX_PREPARE_FAILS:
                print(f"[w{worker_id}] {prepare_fails} ошибок prepare подряд — выхожу")
                rc = 3
                break
            # Частая причина — протухшая сессия: перечитываем sk
            try:
                sk, uid = get_sk_uid(s, logging)
            except AuthError as e2:
                logging.error(f"w{worker_id} reauth: {e2}")
                print(f"[w{worker_id}] ✗ {e2}")
                rc = 2
                break
            time.sleep(10)
            continue
        prepare_fails = 0

        start = time.monotonic()
        extracted, bytes_ = download_and_extract(s, url, worker_id, batch_idx)
        dur = time.monotonic() - start

        if extracted > 0:
            mark_done(paths)
            ws_set(worker_id, files_done=f"+={extracted}", bytes_done=f"+={bytes_}",
                   last_ok=time.time())
            total_files += extracted
            total_bytes += bytes_
            mbps = (bytes_ / 1e6) / max(0.1, dur)
            print(f"[w{worker_id}] batch {batch_idx}: ✓ {extracted} files / "
                  f"{bytes_/1e9:.2f} GB ({mbps:.1f} MB/s, {int(dur)}s)")
        else:
            print(f"[w{worker_id}] batch {batch_idx}: ✗ FAIL → pending")
            ws_set(worker_id, last_error=f"ZIP пачка {batch_idx}: download/extract failed",
                   last_error_at=time.time())
            mark_pending(paths, "download/extract failed")

    # Закрываем HTTP-сессию (освобождаем sockets)
    try:
        s.close()
    except Exception:
        pass

    # Чистим пустую папку для ZIP'ов
    try:
        if os.path.isdir(ZIP_TMP_DIR) and not os.listdir(ZIP_TMP_DIR):
            os.rmdir(ZIP_TMP_DIR)
    except OSError:
        pass

    print(f"[w{worker_id}] ИТОГ: {total_files} files / {total_bytes/1e9:.2f} GB")
    ws_set(worker_id, state="finished" if rc == 0 else f"failed (rc={rc})",
           file_id=None, file_started=None)
    return rc


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
