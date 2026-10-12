"""Общие HTTP-хелперы безлимита: сессия из cookies.json, sk/uid, вызовы models-v2.

Используется и unlim_download.py, и unlim_worker.py. При импорте ничего не
настраивает (логгер передаётся снаружи), чтобы не перебить чужой logging.
"""

from __future__ import annotations

import json
import os
import random
import re
import time

import requests

PROJECT = os.path.dirname(os.path.abspath(__file__))
COOKIES_FILE = os.path.join(PROJECT, "cookies.json")
PAGE_DUMP = os.path.join(PROJECT, "logs", "unlim_page.html")

BASE = "https://disk.yandex.ru"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")

# "sk":"..." — бывает и экранированным внутри JSON-строки в <script>
SK_RE = re.compile(r'\\?"sk\\?"\s*:\s*\\?"([^"\\]+)')
UID_RE = re.compile(r'\\?"uid\\?"\s*:\s*\\?"?(\d+)')


# Как webui.CATEGORIES["photos"] (webui не импортируем — тянет fastapi)
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp", ".gif",
              ".bmp", ".tiff", ".tif", ".raw", ".arw", ".cr2", ".nef",
              ".dng", ".orf"}


class AuthError(RuntimeError):
    """Куки не дают сессию: истекли, капча, не тот домен и т.п."""


class ApiError(RuntimeError):
    """models-v2 ответил ошибкой (HTTP != 200 или error в теле)."""


def load_cookies(path: str = COOKIES_FILE) -> list[dict]:
    if not os.path.exists(path):
        raise AuthError("Нет cookies.json — сначала экспортируй куки из браузера "
                        "(см. README раздел про куки)")
    raw = json.loads(open(path, encoding="utf-8").read())
    if not isinstance(raw, list):
        raise AuthError("cookies.json должен быть массивом (формат Cookie-Editor)")
    return [c for c in raw
            if isinstance(c, dict) and c.get("name") and c.get("value") is not None]


def describe_cookies(cookies: list[dict]) -> str:
    """Короткая сводка для лога: какие домены и есть ли ключевые куки."""
    domains = sorted({c.get("domain") or "?" for c in cookies})
    key = [n for n in ("Session_id", "sessionid2", "yandexuid")
           if any(c["name"] == n for c in cookies)]
    return f"{len(cookies)} кук, домены: {', '.join(domains)}; ключевые: {', '.join(key) or 'НЕТ'}"


def make_session(cookies: list[dict]) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA, "Accept": "*/*",
        "Accept-Language": "ru,en;q=0.9",
        "Origin": BASE, "Referer": f"{BASE}/client/photo",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json",
    })
    # Ставим куки с их настоящим доменом и путём: одноимённые куки с разных
    # доменов больше не затирают друг друга, а чужие домены не уходят на .ru
    for c in cookies:
        s.cookies.set(c["name"], c["value"],
                      domain=c.get("domain") or ".yandex.ru",
                      path=c.get("path") or "/")
    return s


def _dump_page(r: requests.Response) -> str:
    os.makedirs(os.path.dirname(PAGE_DUMP), exist_ok=True)
    with open(PAGE_DUMP, "w", encoding="utf-8") as f:
        f.write(f"<!-- {r.status_code} {r.url} -->\n{r.text}")
    return PAGE_DUMP


def get_sk_uid(s: requests.Session, log) -> tuple[str, str]:
    r = s.get(f"{BASE}/client/disk", timeout=30)
    chain = " -> ".join([h.url for h in r.history] + [r.url])
    log.info(f"GET /client/disk: HTTP {r.status_code}, {chain}")
    if "passport.yandex" in r.url:
        raise AuthError("Куки невалидны (редирект на passport) — "
                        "залогинься в браузере и заэкспортируй cookies.json заново")
    low = r.text.lower()
    if "showcaptcha" in r.url or "checkcaptcha" in low or "smartcaptcha" in low:
        raise AuthError(f"Яндекс показал капчу (антибот) на {r.url}. "
                        f"Страница сохранена в {_dump_page(r)}")
    if r.status_code != 200:
        raise AuthError(f"/client/disk вернул HTTP {r.status_code} ({r.url}). "
                        f"Страница сохранена в {_dump_page(r)}")
    sk, uid = SK_RE.search(r.text), UID_RE.search(r.text)
    if not sk or not uid:
        raise AuthError(f"Не нашёл {'sk' if not sk else 'uid'} в HTML — "
                        f"Яндекс изменил формат страницы. "
                        f"Страница сохранена в {_dump_page(r)}")
    return sk.group(1), uid.group(1)


def make_cid(uid: str) -> str:
    return f"{uid}{int(time.time() * 1000)}{random.randint(100, 999)}"


def call_model(s: requests.Session, sk: str, uid: str, method: str,
               params=None, timeout: int = 120):
    """Один вызов models-v2. Возвращает разобранный JSON или бросает
    AuthError/ApiError с телом ответа — чтобы причина была видна в логе."""
    body = {"sk": sk, "connection_id": make_cid(uid), "apiMethod": method}
    if params is not None:
        body["requestParams"] = params
    r = s.post(f"{BASE}/models-v2", params={"m": method}, json=body, timeout=timeout)
    if r.status_code in (401, 403):
        raise AuthError(f"{method}: сессия истекла (HTTP {r.status_code}): {r.text[:300]}")
    if r.status_code != 200:
        raise ApiError(f"{method}: HTTP {r.status_code}: {r.text[:500]}")
    try:
        data = r.json()
    except ValueError:
        raise ApiError(f"{method}: ответ не JSON: {r.text[:500]}") from None
    if isinstance(data, dict) and data.get("error"):
        raise ApiError(f"{method}: {json.dumps(data, ensure_ascii=False)[:500]}")
    return data
