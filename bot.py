import asyncio
import ast
import copy
import gzip
import html as html_lib
import io
import json
import re
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import urllib.error
import urllib.request
import zlib
import hashlib
import base64
import hmac
import secrets
import socket
import ipaddress
import http.client
import ssl
from urllib.parse import quote
from decimal import Decimal, InvalidOperation

from telegram import Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, ApplicationHandlerStop, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, TypeHandler, filters
from telegram.request import HTTPXRequest
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut
from types import SimpleNamespace

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
VERSION = "11"
MCP_MODERN_VERSION = "2026-07-28"
MCP_LEGACY_VERSION = "2025-11-25"

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.getenv("TELEGRAM_JOURNAL_DATA_DIR", os.path.join(BASE_DIR, "data"))
os.makedirs(DATA_DIR, exist_ok=True)
SOURCE_STORE = os.path.join(DATA_DIR, "sources.json")
ACCESS_STORE = os.getenv("ACCESS_STORE", os.path.join(DATA_DIR, "access.json"))
USER_STATE_STORE = os.path.join(DATA_DIR, "user-data.pickle")
TOPIC_LOOP_STORE = os.path.join(DATA_DIR, "topic-loops.json")
LEGACY_SOURCE_STORE = os.path.join(BASE_DIR, "sources.json")
LEGACY_ACCESS_STORE = os.path.join(BASE_DIR, "access.json")

def migrate_legacy_store(source, destination):
    if os.path.abspath(source) == os.path.abspath(destination):
        return
    if os.path.exists(destination) or not os.path.isfile(source):
        return
    with open(source, "rb") as old_file:
        raw = old_file.read()
    temporary_path = destination + ".migration.tmp"
    with open(temporary_path, "wb") as new_file:
        new_file.write(raw)
        new_file.flush()
        os.fsync(new_file.fileno())
    os.replace(temporary_path, destination)

migrate_legacy_store(LEGACY_SOURCE_STORE, SOURCE_STORE)
migrate_legacy_store(LEGACY_ACCESS_STORE, ACCESS_STORE)

def atomic_write_json(path, data):
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    temporary_path = path + ".tmp"
    backup_path = path + ".bak"
    if os.path.isfile(path):
        with open(path, "rb") as current:
            previous = current.read()
        json.loads(previous.decode("utf-8"))
        backup_tmp = backup_path + ".tmp"
        with open(backup_tmp, "wb") as backup:
            backup.write(previous)
            backup.flush()
            os.fsync(backup.fileno())
        os.replace(backup_tmp, backup_path)
    try:
        with open(temporary_path, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
        try:
            directory_fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
    finally:
        if os.path.exists(temporary_path):
            os.remove(temporary_path)

def load_json_store(path, default):
    errors = []
    for candidate in (path, path + ".bak"):
        try:
            with open(candidate, "r", encoding="utf-8") as source:
                return json.load(source)
        except FileNotFoundError:
            continue
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as error:
            errors.append(f"{candidate}: {type(error).__name__}")
    if errors:
        raise RuntimeError("ذخیره سازی JSON خراب است و نسخه پشتیبان قابل خواندن نیست: " + "; ".join(errors))
    return default
ACCESS_CHANNEL_ID = -1004301287348
WIDGET_ARCHIVE_GROUP_ID = -1004301287348
LEGACY_ACCESS_CHANNEL_ID = -1003734187598
STORAGE_TOPIC_NAMES = ("تنظیمات", "کاربران", "منابع", "شمارنده ها", "حلقه ها", "پشتیبان", "آرشیو ویجت ها", "گزارش ها")
ACCESS_STORAGE_MARKER = "TELEGRAM_JOURNAL_ACCESS_V1"
# Owner ID is fixed in the source so no hosting variable is needed.
OWNER_ID = 1881579954
MAX_RICH_BLOCKS = 500
MAX_RICH_TEXT_BYTES = 32768
MAX_MEDIA_ATTACHMENTS = 50
WIDGET_API_BASE = os.getenv("TELEGRAM_WIDGET_API_BASE", "https://api.telegram.org/beta").rstrip("/")
MAX_WIDGET_HTML_BYTES = 32 * 1024
MAX_WIDGET_COMPRESSED_BYTES = 4 * 1024

def load_source_texts():
    data = load_json_store(SOURCE_STORE, {})
    if not isinstance(data, dict):
        raise RuntimeError("ساختار فایل منابع معتبر نیست")
    return data

def save_source_texts(sources):
    if not isinstance(sources, dict):
        raise ValueError("فهرست منابع باید یک شیء JSON باشد")
    atomic_write_json(SOURCE_STORE, sources)

def load_access_users():
    data = load_json_store(ACCESS_STORE, [])
    if not isinstance(data, list):
        raise RuntimeError("ساختار فایل کاربران مجاز معتبر نیست")
    return {int(user_id) for user_id in data if str(user_id).isdigit() and int(user_id) > 0}

def save_access_users(users):
    atomic_write_json(ACCESS_STORE, sorted(users))

def access_storage_text(users, ai_config_message_id=None):
    payload = {"users": sorted(users), "ai_config_message_id": ai_config_message_id}
    return ACCESS_STORAGE_MARKER + "\n" + json.dumps(payload, ensure_ascii=False)

AI_CONFIG_MARKER = "TELEGRAM_JOURNAL_AI_CONFIG_V1"

def ai_keys():
    master = hashlib.sha256(("telegram-journal-ai-config:" + TOKEN).encode("utf-8")).digest()
    encryption_key = hmac.new(master, b"encryption", hashlib.sha256).digest()
    authentication_key = hmac.new(master, b"authentication", hashlib.sha256).digest()
    return encryption_key, authentication_key

def ai_xor_stream(data, encryption_key, nonce):
    output = bytearray()
    for counter, offset in enumerate(range(0, len(data), 32)):
        block = hmac.new(
            encryption_key,
            nonce + counter.to_bytes(8, "big"),
            hashlib.sha256,
        ).digest()
        chunk = data[offset:offset + 32]
        output.extend(a ^ b for a, b in zip(chunk, block))
    return bytes(output)

STORAGE_V2_MARKER = "TELEGRAM_JOURNAL_STORAGE_V2"

def encrypt_marked_payload(payload):
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    compressed = zlib.compress(raw, 9)
    encryption_key, authentication_key = ai_keys()
    nonce = secrets.token_bytes(16)
    ciphertext = ai_xor_stream(compressed, encryption_key, nonce)
    tag = hmac.new(authentication_key, STORAGE_V2_MARKER.encode() + nonce + ciphertext, hashlib.sha256).digest()
    encrypted = base64.urlsafe_b64encode(nonce + ciphertext + tag).decode("ascii")
    result = STORAGE_V2_MARKER + "\n" + encrypted
    if len(result) > 4096:
        raise ValueError("اطلاعات ذخیره سازی از ظرفیت یک پیام تلگرام بیشتر شده")
    return result

def decrypt_marked_payload(message_text):
    lines = (message_text or "").splitlines()
    if not lines or lines[0].strip() != STORAGE_V2_MARKER or len(lines) < 2:
        raise ValueError("پیام ذخیره سازی معتبر نیست")
    payload = base64.urlsafe_b64decode(lines[1].strip().encode("ascii"))
    if len(payload) < 48:
        raise ValueError("داده ذخیره سازی ناقص است")
    nonce, ciphertext, supplied_tag = payload[:16], payload[16:-32], payload[-32:]
    encryption_key, authentication_key = ai_keys()
    expected_tag = hmac.new(authentication_key, STORAGE_V2_MARKER.encode() + nonce + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(supplied_tag, expected_tag):
        raise ValueError("اعتبارسنجی ذخیره سازی ناموفق بود")
    decoded = json.loads(zlib.decompress(ai_xor_stream(ciphertext, encryption_key, nonce)).decode("utf-8"))
    if not isinstance(decoded, dict) or not isinstance(decoded.get("users"), list) or not isinstance(decoded.get("ai_config"), dict):
        raise ValueError("ساختار اطلاعات ذخیره شده معتبر نیست")
    if not isinstance(decoded["ai_config"].get("services"), dict):
        raise ValueError("فهرست سرویس های هوش مصنوعی معتبر نیست")
    return decoded

PLAIN_STORAGE_MARKER = "TELEGRAM_JOURNAL_STORAGE_PLAIN_V1"

def validate_storage_payload(data):
    if not isinstance(data, dict):
        raise ValueError("ساختار اطلاعات ذخیره شده معتبر نیست")
    if "users" in data and not isinstance(data.get("users"), list):
        raise ValueError("فهرست کاربران معتبر نیست")
    if "ai_config" in data:
        if not isinstance(data.get("ai_config"), dict):
            raise ValueError("تنظیمات هوش مصنوعی معتبر نیست")
        if not isinstance(data["ai_config"].get("services"), dict):
            raise ValueError("فهرست سرویس های هوش مصنوعی معتبر نیست")
    for key in ("widget_index_file_id", "runtime_snapshot_file_id", "users_file_id", "ai_config_file_id", "runtime_snapshot_sha256"):
        if data.get(key) is not None and not isinstance(data.get(key), str):
            raise ValueError(f"شناسه فایل ذخیره سازی {key} معتبر نیست")
    if "topics" in data and not isinstance(data.get("topics"), dict):
        raise ValueError("فهرست تاپیک های ذخیره سازی معتبر نیست")
    if "storage_readable_message_ids" in data and not isinstance(data.get("storage_readable_message_ids"), dict):
        raise ValueError("شناسه پیام های خوانای ذخیره سازی معتبر نیست")
    return data

def plain_storage_text(payload):
    validate_storage_payload(payload)
    result = PLAIN_STORAGE_MARKER + "\n" + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(result) > 4096:
        raise ValueError("اطلاعات ذخیره سازی از ظرفیت یک پیام تلگرام بیشتر شده")
    return result

def parse_plain_storage(message_text):
    lines = (message_text or "").splitlines()
    if not lines or lines[0].strip() != PLAIN_STORAGE_MARKER or len(lines) < 2:
        raise ValueError("پیام ذخیره سازی ساده معتبر نیست")
    return validate_storage_payload(json.loads("\n".join(lines[1:])))

def storage_message_text(context, users=None, ai_config=None):
    bot_data = context.application.bot_data
    data = {
        "users_file_id": bot_data.get("users_file_id"),
        "ai_config_file_id": bot_data.get("ai_config_file_id"),
        "widget_index_file_id": bot_data.get("widget_index_file_id"),
        "runtime_snapshot_file_id": bot_data.get("runtime_snapshot_file_id"),
        "runtime_snapshot_sha256": bot_data.get("runtime_snapshot_sha256"),
        "topics": bot_data.get("storage_topics", {}),
        "storage_readable_message_ids": bot_data.get("storage_readable_message_ids", {}),
    }
    return plain_storage_text(data)




def _storage_text_value(value, limit=240):
    if value is None:
        return "تنظیم نشده"
    if isinstance(value, (dict, list)):
        rendered = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    else:
        rendered = str(value)
    rendered = rendered.strip() or "خالی"
    if len(rendered) > limit:
        rendered = rendered[:limit] + "…"
    return rendered


def readable_storage_texts(application):
    bot_data = application.bot_data
    users = sorted(bot_data.get("authorized_users", set()))
    user_data = application.user_data
    ai_config = bot_data.get("ai_config") or ai_default_config()
    services = ai_config.get("services", {}) if isinstance(ai_config, dict) else {}
    sources = bot_data.get("source_texts") or {}
    loops = bot_data.get("topic_loop_states") or {}
    user_audit_topics = bot_data.get("user_audit_topics") or {}
    widget_index = bot_data.get("widget_index") or empty_widget_index()
    widgets = widget_index.get("widgets", {}) if isinstance(widget_index, dict) else {}
    widget_messages = widget_index.get("messages", {}) if isinstance(widget_index, dict) else {}
    connectors = ai_config.get("connectors", {}) if isinstance(ai_config, dict) else {}

    settings_lines = [
        "⚙️ تنظیمات ربات",
        f"وضعیت ذخیره سازی: {'فعال' if bot_data.get('access_channel_ready') else 'در انتظار اتصال'}",
        f"سرویس پیش فرض: {services.get(ai_config.get('default_service'), {}).get('name', 'انتخاب نشده') if isinstance(services, dict) else 'انتخاب نشده'}",
        f"تعداد سرویس های هوش مصنوعی: {len(services) if isinstance(services, dict) else 0}",
    ]
    if isinstance(services, dict):
        for service_id, service in services.items():
            if not isinstance(service, dict):
                continue
            settings_lines.extend([
                "",
                f"سرویس: {service.get('name') or service_id}",
                f"مدل: {_storage_text_value(service.get('model'))}",
                f"آدرس API: {_storage_text_value(service.get('url'))}",
                f"کلید API: {_storage_text_value(service.get('api_key'), 700)}",
                f"هدرها: {_storage_text_value(service.get('headers') or {}, 900)}",
                f"پیش فرض: {'بله' if ai_config.get('default_service') == service_id else 'خیر'}",
            ])
    if isinstance(connectors, dict):
        settings_lines.append("")
        settings_lines.append(f"تعداد اتصال های جانبی: {len(connectors)}")
        for connector_id, connector in connectors.items():
            if not isinstance(connector, dict):
                continue
            settings_lines.extend([
                f"اتصال: {connector.get('name') or connector_id}",
                f"نوع: {_storage_text_value(connector.get('type'))}",
                f"آدرس: {_storage_text_value(connector.get('url'))}",
                f"وضعیت: {'فعال' if connector.get('enabled') else 'غیرفعال'}",
            ])
            for secret_name in ("api_key", "token", "secret", "headers"):
                if connector.get(secret_name):
                    settings_lines.append(f"{secret_name}: {_storage_text_value(connector.get(secret_name), 700)}")

    users_lines = [
        "👥 کاربران",
        f"تعداد کاربران مجاز: {len(users)}",
        f"تعداد کاربرانی که اطلاعات ذخیره شده دارند: {len(user_data)}",
        f"مدیر اصلی: {OWNER_ID}",
    ]
    if users:
        users_lines.append("")
        users_lines.append("شناسه کاربران مجاز:")
        users_lines.extend(str(user_id) for user_id in users[:100])
        if len(users) > 100:
            users_lines.append(f"و {len(users) - 100} کاربر دیگر")
    if user_data:
        users_lines.append("")
        users_lines.append("اطلاعات نشست کاربران:")
        for user_id, data in list(user_data.items())[:40]:
            if isinstance(data, dict):
                keys = "، ".join(str(key) for key in list(data.keys())[:12]) or "بدون مورد"
                users_lines.append(f"کاربر {user_id}: {keys}")
            else:
                users_lines.append(f"کاربر {user_id}: اطلاعات ثبت شده")

    sources_lines = [
        "📚 منابع ذخیره شده",
        f"تعداد منابع: {len(sources) if isinstance(sources, dict) else 0}",
    ]
    if isinstance(sources, dict):
        for source_id, source_text in list(sources.items())[:50]:
            sources_lines.extend([
                "",
                f"شناسه: {source_id}",
                _storage_text_value(source_text, 220),
            ])
        if len(sources) > 50:
            sources_lines.append(f"و {len(sources) - 50} منبع دیگر")

    loop_lines = ["🔁 حلقه ها و ساخت تاپیک"]
    if isinstance(loops, dict) and loops:
        for chat_id, state in loops.items():
            if not isinstance(state, dict):
                continue
            loop_lines.append(
                f"گروه {chat_id} | شماره بعدی: {state.get('next_number', 1)} | "
                f"وضعیت: {'فعال' if state.get('active') else 'متوقف'}"
            )
    else:
        loop_lines.append("حلقه ای ثبت نشده")

    counter_lines = [
        "🔢 شمارنده ها",
        f"کاربران مجاز: {len(users)}",
        f"کاربران دارای اطلاعات: {len(user_data)}",
        f"منابع: {len(sources) if isinstance(sources, dict) else 0}",
        f"سرویس های هوش مصنوعی: {len(services) if isinstance(services, dict) else 0}",
        f"اتصال های جانبی: {len(connectors) if isinstance(connectors, dict) else 0}",
        f"ویجت ها: {len(widgets) if isinstance(widgets, dict) else 0}",
        f"پیام های ثبت شده در آرشیو ویجت: {len(widget_messages) if isinstance(widget_messages, dict) else 0}",
        f"تاپیک های اختصاصی کاربران: {len(user_audit_topics)}",
        f"حلقه های فعال: {sum(1 for item in loops.values() if isinstance(item, dict) and item.get('active')) if isinstance(loops, dict) else 0}",
    ]

    backup_lines = [
        "💾 پشتیبان",
        f"وضعیت نسخه پشتیبان فنی: {'موجود' if bot_data.get('runtime_snapshot_file_id') else 'هنوز ساخته نشده'}",
        f"شناسه فایل پشتیبان: {bot_data.get('runtime_snapshot_file_id') or 'ثبت نشده'}",
        f"اثر انگشت نسخه: {(bot_data.get('runtime_snapshot_sha256') or '')[:16] or 'ثبت نشده'}",
        "نسخه فنی برای بازیابی خودکار استفاده می شود",
        "اطلاعات اصلی این بخش ها در پیام های خوانا نمایش داده می شود",
    ]

    widget_lines = [
        "🧩 آرشیو ویجت ها",
        f"تعداد ویجت ها: {len(widgets) if isinstance(widgets, dict) else 0}",
        f"تعداد پیام های آرشیو: {len(widget_messages) if isinstance(widget_messages, dict) else 0}",
    ]
    report_lines = [
        "📊 گزارش ها",
        f"تعداد تاپیک های اختصاصی کاربران: {len(user_audit_topics)}",
        "گزارش های جدید هر کاربر در تاپیک اختصاصی خودش ثبت می شود",
    ]

    sections = {
        "تنظیمات": settings_lines,
        "کاربران": users_lines,
        "منابع": sources_lines,
        "شمارنده ها": counter_lines,
        "حلقه ها": loop_lines,
        "پشتیبان": backup_lines,
        "آرشیو ویجت ها": widget_lines,
        "گزارش ها": report_lines,
    }
    result = {}
    for name, lines in sections.items():
        rendered = "\n".join(lines)
        if len(rendered) > 3800:
            rendered = rendered[:3650].rsplit("\n", 1)[0] + "\n… ادامه جزئیات در نسخه پشتیبان فنی"
        result[name] = rendered
    return result


async def telegram_storage_call(method, *args, **kwargs):
    """Retry transient Telegram storage failures without discarding local state."""
    network_attempt = 0
    rate_limit_attempt = 0
    while True:
        try:
            return await method(*args, **kwargs)
        except RetryAfter as error:
            if rate_limit_attempt >= 5:
                raise
            retry_after = getattr(error, "retry_after", 1)
            if hasattr(retry_after, "total_seconds"):
                retry_after = retry_after.total_seconds()
            delay = max(1.0, float(retry_after)) + 1.0
            rate_limit_attempt += 1
            print(
                f"TELEGRAM STORAGE RATE LIMIT: waiting {delay:.1f}s "
                f"before retry {rate_limit_attempt}/5",
                flush=True,
            )
            await asyncio.sleep(delay)
        except (TimedOut, NetworkError) as error:
            if network_attempt >= 2:
                raise
            delay = 2 ** (network_attempt + 1)
            network_attempt += 1
            print(
                f"TELEGRAM STORAGE NETWORK RETRY: {type(error).__name__}; "
                f"waiting {delay}s before retry {network_attempt}/2",
                flush=True,
            )
            await asyncio.sleep(delay)


async def sync_readable_storage_topics(context):
    bot_data = context.application.bot_data
    topics = bot_data.get("storage_topics") or {}
    message_ids = bot_data.setdefault("storage_readable_message_ids", {})
    sections = readable_storage_texts(context.application)
    for name, text in sections.items():
        thread_id = topics.get(name)
        if not thread_id:
            continue
        message_id = message_ids.get(name)
        if message_id:
            try:
                await telegram_storage_call(context.bot.edit_message_text,
                    chat_id=ACCESS_CHANNEL_ID,
                    message_id=message_id,
                    text=text,
                    disable_web_page_preview=True,
                )
                continue
            except Exception as error:
                error_text = str(error).lower()
                if "message is not modified" in error_text:
                    continue
                if not any(marker in error_text for marker in (
                    "message to edit not found",
                    "message_id_invalid",
                    "message can't be edited",
                    "message can't be found",
                )):
                    print(
                        f"READABLE STORAGE UPDATE FAILED topic={name}: "
                        f"{type(error).__name__}: {error}",
                        flush=True,
                    )
                    continue
        try:
            sent = await telegram_storage_call(context.bot.send_message,
                chat_id=ACCESS_CHANNEL_ID,
                message_thread_id=thread_id,
                text=text,
                disable_notification=True,
                disable_web_page_preview=True,
            )
            message_ids[name] = sent.message_id
        except Exception as error:
            print(
                f"READABLE STORAGE CREATE FAILED topic={name}: "
                f"{type(error).__name__}: {error}",
                flush=True,
            )

def is_message_not_modified_error(error):
    return isinstance(error, BadRequest) and "message is not modified" in str(error).lower()


async def safe_edit_message_text(bot, **kwargs):
    try:
        return await telegram_storage_call(bot.edit_message_text, **kwargs)
    except BadRequest as error:
        if not is_message_not_modified_error(error):
            raise
        return None


def _rich_storage_digest(payload):
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


RICH_STORAGE_PLAIN_MARKER = "HSTK-RICH-PLAIN-1:"

def encode_rich_storage_payload(payload):
    # New backups are deliberately plain JSON so they can be read in Telegram.
    return RICH_STORAGE_PLAIN_MARKER + json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), default=str
    )


def decode_rich_storage_payload(encoded):
    if not isinstance(encoded, str):
        raise ValueError("داده پشتیبان Rich Message معتبر نیست")
    if encoded.startswith(RICH_STORAGE_PLAIN_MARKER):
        payload = json.loads(encoded[len(RICH_STORAGE_PLAIN_MARKER):])
    elif encoded.startswith("HSTK-RICH-1:"):
        # One-time compatibility with the previous encrypted format.
        packed = base64.urlsafe_b64decode(encoded.split(":", 1)[1].encode("ascii"))
        if len(packed) < 48:
            raise ValueError("داده پشتیبان Rich Message ناقص است")
        nonce, ciphertext, supplied_tag = packed[:16], packed[16:-32], packed[-32:]
        encryption_key, authentication_key = ai_keys()
        expected_tag = hmac.new(
            authentication_key,
            b"HASHTAK-RICH-STORAGE-V1" + nonce + ciphertext,
            hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(supplied_tag, expected_tag):
            raise ValueError("اعتبارسنجی نسخه قدیمی پشتیبان ناموفق بود")
        decoded = zlib.decompress(
            ai_xor_stream(ciphertext, encryption_key, nonce)
        ).decode("utf-8")
        try:
            payload = ast.literal_eval(decoded)
        except (SyntaxError, ValueError):
            payload = json.loads(decoded)
    else:
        raise ValueError("نشانگر داده پشتیبان Rich Message معتبر نیست")
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise ValueError("نسخه داده پشتیبان Rich Message معتبر نیست")
    return payload


def rich_storage_uses_legacy_encryption(message):
    if not isinstance(message, dict):
        return False
    rich = message.get("rich_message")
    if not isinstance(rich, dict):
        return False

    def walk(blocks):
        if not isinstance(blocks, list):
            return False
        for block in blocks:
            if not isinstance(block, dict):
                continue
            text = block.get("text")
            if isinstance(text, str) and text.startswith("HSTK-RICH-1:"):
                return True
            if walk(block.get("blocks")):
                return True
        return False

    return walk(rich.get("blocks"))


def extract_rich_storage_payload(message):
    if not isinstance(message, dict):
        return None
    rich = message.get("rich_message")
    if not isinstance(rich, dict):
        return None
    found = []

    def walk(blocks):
        if not isinstance(blocks, list):
            return
        for block in blocks:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "pre" and isinstance(block.get("text"), str):
                text = block["text"]
                if text.startswith((RICH_STORAGE_PLAIN_MARKER, "HSTK-RICH-1:")):
                    found.append(text)
            walk(block.get("blocks"))

    walk(rich.get("blocks"))
    if not found:
        return None
    if len(found) != 1:
        raise ValueError("پیام ذخیره سازی بیش از یک داده پشتیبان دارد")
    return decode_rich_storage_payload(found[0])


def build_rich_storage_message(context):
    application = context.application
    bot_data = application.bot_data
    snapshot = current_runtime_snapshot(application)
    storage = {
        "authorized_users": sorted(bot_data.get("authorized_users", set())),
        "ai_config": bot_data.get("ai_config") or ai_default_config(),
        "widget_index_file_id": bot_data.get("widget_index_file_id"),
        "runtime_snapshot_file_id": bot_data.get("runtime_snapshot_file_id"),
        "runtime_snapshot_sha256": bot_data.get("runtime_snapshot_sha256"),
        "topics": bot_data.get("storage_topics", {}),
        "storage_readable_message_ids": bot_data.get("storage_readable_message_ids", {}),
    }
    payload = {"version": 1, "storage": storage, "runtime": snapshot}
    encoded = encode_rich_storage_payload(payload)

    users = storage["authorized_users"]
    ai_config = storage["ai_config"]
    services = ai_config.get("services", {}) if isinstance(ai_config, dict) else {}
    sources = snapshot.get("sources", {})
    loops = snapshot.get("topic_loop_states", {})
    user_data = snapshot.get("user_data", {})
    audits = snapshot.get("user_audit_topics", {})
    widgets = (bot_data.get("widget_index") or {}).get("widgets", {})

    def json_block(value):
        return {"type": "pre", "text": json.dumps(
            value, ensure_ascii=False, indent=2, default=str
        )}

    rich_message = {"blocks": [
        {"type": "heading", "size": 1, "text": "🗄️ مرکز ذخیره سازی HASHTAK"},
        {"type": "paragraph", "text": "اطلاعات به صورت متن ساده ذخیره میشن و همین پیام با تغییر اطلاعات ویرایش میشه"},
        {"type": "details", "summary": f"👥 کاربران و دسترسی ها · {len(users)} کاربر مجاز",
         "blocks": [json_block({"owner_id": OWNER_ID, "authorized_users": users, "user_data": user_data})]},
        {"type": "details", "summary": f"🤖 تنظیمات هوش مصنوعی · {len(services)} سرویس",
         "blocks": [json_block(ai_config)]},
        {"type": "details", "summary": f"📚 منابع ذخیره شده · {len(sources)} منبع",
         "blocks": [json_block(sources)]},
        {"type": "details", "summary": f"🔁 تاپیک ها و حلقه ها · {len(loops)} حلقه",
         "blocks": [json_block({"topic_loop_states": loops, "user_audit_topics": audits, "storage_topics": storage["topics"]})]},
        {"type": "details", "summary": f"🧩 آرشیو ویجت ها · {len(widgets)} ویجت",
         "blocks": [json_block({"widget_index_file_id": storage["widget_index_file_id"], "widget_count": len(widgets)})]},
        {"type": "divider"},
        {"type": "details", "summary": "📦 نسخه کامل قابل بازیابی · JSON ساده و خوانا",
         "blocks": [{"type": "pre", "text": encoded}]},
        {"type": "footer", "text": f"آخرین بروزرسانی: {persian_timestamp()}"},
    ], "is_rtl": True}
    serialized_size = len(json.dumps(
        rich_message, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8"))
    if serialized_size > MAX_RICH_TEXT_BYTES:
        raise ValueError(
            f"حجم نسخه پشتیبان Rich Message از حد مجاز بیشتر شد ({serialized_size} بایت)"
        )
    return rich_message, payload


async def persist_storage(context, users=None, ai_config=None):
    bot_data = context.application.bot_data
    lock = bot_data.setdefault("storage_lock", asyncio.Lock())
    async with lock:
        message_id = bot_data.get("storage_message_id")
        if not message_id:
            raise RuntimeError("پیام اصلی ذخیره سازی گروه پیدا نشد")
        previous_authorized_users = bot_data.get("authorized_users")
        previous_ai_config = bot_data.get("ai_config")
        try:
            if users is not None:
                bot_data["authorized_users"] = set(users)
            if ai_config is not None:
                bot_data["ai_config"] = ai_config
            await sync_readable_storage_topics(context)
            rich_message, payload = build_rich_storage_message(context)
            digest = _rich_storage_digest(payload)
            if digest == bot_data.get("rich_storage_sha256"):
                return
            await api_async("editMessageText", {
                "chat_id": ACCESS_CHANNEL_ID,
                "message_id": message_id,
                "rich_message": rich_message,
            })
            bot_data["rich_storage_sha256"] = digest
            print("Telegram plain-text Rich Message storage updated", flush=True)
        except Exception:
            if previous_authorized_users is not None:
                bot_data["authorized_users"] = previous_authorized_users
            if previous_ai_config is not None:
                bot_data["ai_config"] = previous_ai_config
            raise


async def persist_ai_config(config, context):
    bot_data = context.application.bot_data
    previous_ai_config = copy.deepcopy(bot_data.get("ai_config"))
    bot_data["ai_config"] = config
    try:
        await persist_runtime_state(context.application)
        await persist_storage(context, ai_config=config)
    except Exception:
        if previous_ai_config is not None:
            bot_data["ai_config"] = previous_ai_config
        raise

def empty_widget_index():
    return {"version": 1, "widgets": {}, "messages": {}}

async def load_widget_index(application, file_id):
    if not file_id:
        application.bot_data["widget_index"] = empty_widget_index()
        return
    remote = await application.bot.get_file(file_id)
    content = await remote.download_as_bytearray()
    downloaded = bytes(content)
    try:
        # New archives are UTF-8 text containing a Python literal structure.
        index = ast.literal_eval(downloaded.decode("utf-8"))
    except (SyntaxError, ValueError, UnicodeDecodeError):
        # Backward compatibility with old JSON.GZ widget indexes.
        try:
            index = json.loads(gzip.decompress(downloaded).decode("utf-8"))
        except Exception:
            index = json.loads(downloaded.decode("utf-8"))
    if (
        not isinstance(index, dict)
        or index.get("version") != 1
        or not isinstance(index.get("widgets"), dict)
        or not isinstance(index.get("messages"), dict)
    ):
        raise ValueError("ساختار آرشیو ویجت معتبر نیست")
    application.bot_data["widget_index"] = index

async def save_widget_index(context):
    bot_data = context.application.bot_data
    index = bot_data.get("widget_index") or empty_widget_index()
    raw = repr(index).encode("utf-8")
    archived = await telegram_storage_call(context.bot.send_document,
        chat_id=ACCESS_CHANNEL_ID,
        message_thread_id=(bot_data.get("storage_topics") or {}).get("آرشیو ویجت ها"),
        document=InputFile(io.BytesIO(raw), filename="widget-index.txt"),
        caption="فهرست آرشیو ویجت ها · متن ساده",
        disable_notification=True,
    )
    # Upload first, then move the plain-text root pointer. An interrupted update
    # leaves the previous index intact and the new file as a harmless orphan.
    previous_file_id = bot_data.get("widget_index_file_id")
    bot_data["widget_index_file_id"] = archived.document.file_id
    try:
        await persist_storage(context)
    except Exception:
        bot_data["widget_index_file_id"] = previous_file_id
        raise

async def record_widget_message(context, token, chat_id, message_id, entry=None):
    bot_data = context.application.bot_data
    lock = bot_data.setdefault("widget_index_lock", asyncio.Lock())
    async with lock:
        previous_index = bot_data.get("widget_index")
        index = copy.deepcopy(previous_index or empty_widget_index())
        if entry is not None:
            index["widgets"][token] = entry
        index["messages"][f"{chat_id}:{message_id}"] = token
        bot_data["widget_index"] = index
        try:
            await save_widget_index(context)
        except Exception:
            bot_data["widget_index"] = previous_index or empty_widget_index()
            raise

async def ensure_storage_topics(application, saved_topics=None):
    topics = {}
    if isinstance(saved_topics, dict):
        for name, thread_id in saved_topics.items():
            if name in STORAGE_TOPIC_NAMES and isinstance(thread_id, int) and thread_id > 0:
                topics[name] = thread_id
    for name in STORAGE_TOPIC_NAMES:
        if name in topics:
            continue
        created = await telegram_storage_call(application.bot.create_forum_topic,
            chat_id=ACCESS_CHANNEL_ID,
            name=name,
        )
        topics[name] = created.message_thread_id
        await telegram_storage_call(application.bot.send_message,
            chat_id=ACCESS_CHANNEL_ID,
            message_thread_id=created.message_thread_id,
            text=f"📁 بخش ذخیره سازی: {name}\nاطلاعات این بخش به صورت متن نگهداری می شود.",
            disable_notification=True,
        )
    application.bot_data["storage_topics"] = topics

    display_names = {
        "تنظیمات": "⚙️ تنظیمات",
        "کاربران": "👥 کاربران",
        "منابع": "📚 منابع",
        "شمارنده ها": "🔢 شمارنده ها",
        "حلقه ها": "🔁 حلقه ها",
        "پشتیبان": "💾 پشتیبان",
        "آرشیو ویجت ها": "🧩 آرشیو ویجت ها",
        "گزارش ها": "📊 گزارش ها",
    }
    for key, display_name in display_names.items():
        thread_id = topics.get(key)
        if not thread_id:
            continue
        try:
            await telegram_storage_call(application.bot.edit_forum_topic,
                chat_id=ACCESS_CHANNEL_ID,
                message_thread_id=thread_id,
                name=display_name,
            )
        except Exception as error:
            # Telegram returns TOPIC_NOT_MODIFIED when the topic already has
            # the requested title. This is a successful no-op, not a failure.
            if "topic_not_modified" not in str(error).lower():
                raise
    return topics


def apply_runtime_snapshot(application, snapshot, raw_for_hash=None):
    if not isinstance(snapshot, dict) or snapshot.get("version") != 1:
        raise ValueError("ساختار نسخه پشتیبان تلگرام معتبر نیست")
    sources = snapshot.get("sources", {})
    loops = snapshot.get("topic_loop_states", {})
    users = snapshot.get("user_data", {})
    user_audit_topics = snapshot.get("user_audit_topics", {})
    authorized_users = snapshot.get("authorized_users")
    ai_config = snapshot.get("ai_config")
    if (
        not isinstance(sources, dict)
        or not isinstance(loops, dict)
        or not isinstance(users, dict)
        or not isinstance(user_audit_topics, dict)
        or (authorized_users is not None and not isinstance(authorized_users, list))
        or (ai_config is not None and (
            not isinstance(ai_config, dict)
            or not isinstance(ai_config.get("services", {}), dict)
        ))
    ):
        raise ValueError("بخش های نسخه پشتیبان معتبر نیستند")
    application.bot_data["source_texts"] = sources
    application.bot_data["topic_loop_states"] = loops
    if authorized_users is not None:
        application.bot_data["authorized_users"] = {
            int(user_id) for user_id in authorized_users
            if str(user_id).isdigit() and int(user_id) > 0
        }
    if ai_config is not None:
        application.bot_data["ai_config"] = ai_config
    application.bot_data["user_audit_topics"] = {
        str(user_id): thread_id
        for user_id, thread_id in user_audit_topics.items()
        if str(user_id).isdigit() and isinstance(thread_id, int) and thread_id > 0
    }
    for user_id, data in users.items():
        if str(user_id).isdigit() and isinstance(data, dict):
            application.user_data[int(user_id)].update(data)
    save_source_texts(sources)
    save_topic_loop_states(loops)
    raw = raw_for_hash if raw_for_hash is not None else repr(snapshot).encode("utf-8")
    application.bot_data["runtime_snapshot_sha256"] = hashlib.sha256(raw).hexdigest()
    print(
        f"Telegram runtime snapshot restored: {len(sources)} sources, "
        f"{len(users)} user states, {len(loops)} topic loops",
        flush=True,
    )
    return True


async def restore_runtime_state(application, file_id):
    if not file_id:
        return False
    remote = await application.bot.get_file(file_id)
    raw = bytes(await remote.download_as_bytearray())
    try:
        snapshot = ast.literal_eval(raw.decode("utf-8"))
    except (SyntaxError, ValueError, UnicodeDecodeError):
        snapshot = json.loads(raw.decode("utf-8"))
    return apply_runtime_snapshot(application, snapshot, raw)


def current_runtime_snapshot(application):
    bot_data = application.bot_data
    return {
        "version": 1,
        "sources": bot_data.get("source_texts", {}),
        "topic_loop_states": bot_data.get("topic_loop_states", {}),
        "user_audit_topics": bot_data.get("user_audit_topics", {}),
        "authorized_users": sorted(bot_data.get("authorized_users", set())),
        "ai_config": bot_data.get("ai_config") or ai_default_config(),
        "user_data": {str(user_id): data for user_id, data in application.user_data.items()},
    }


async def persist_runtime_state(application):
    bot_data = application.bot_data
    lock = bot_data.setdefault("runtime_storage_lock", asyncio.Lock())
    async with lock:
        snapshot = current_runtime_snapshot(application)
        raw = repr(snapshot).encode("utf-8")
        snapshot_hash = hashlib.sha256(raw).hexdigest()
        if snapshot_hash == bot_data.get("runtime_snapshot_sha256") and bot_data.get("rich_storage_sha256"):
            return
        previous_hash = bot_data.get("runtime_snapshot_sha256")
        bot_data["runtime_snapshot_sha256"] = snapshot_hash
        try:
            context = type("StorageContext", (), {"application": application, "bot": application.bot})()
            await persist_storage(context)
        except Exception:
            bot_data["runtime_snapshot_sha256"] = previous_hash
            raise


async def runtime_snapshot_worker(application):
    while True:
        await asyncio.sleep(300)
        try:
            await persist_runtime_state(application)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            print(f"TELEGRAM RUNTIME BACKUP FAILED: {type(error).__name__}: {error}", flush=True)


async def initialize_access_storage(application):
    try:
        chat = await application.bot.get_chat(ACCESS_CHANNEL_ID)
        pinned = chat.pinned_message
        needs_widget_copy = False
        pinned_text = ""
        users = set()
        ai_config = {"services": {}, "default_service": None}
        widget_index_file_id = None
        runtime_snapshot_file_id = None
        runtime_snapshot_sha256 = None
        users_file_id = None
        ai_config_file_id = None
        saved_topics = {}
        legacy_decryption_failed = False
        rich_saved = None
        if pinned:
            raw_chat = await api_async("getChat", {"chat_id": ACCESS_CHANNEL_ID})
            raw_pinned = raw_chat.get("pinned_message") if isinstance(raw_chat, dict) else None
            rich_saved = extract_rich_storage_payload(raw_pinned)
            pinned_text = pinned.text or pinned.caption or ""
            if rich_saved:
                rich_storage = rich_saved.get("storage", {})
                rich_runtime = rich_saved.get("runtime")
                if not isinstance(rich_storage, dict) or not isinstance(rich_runtime, dict):
                    raise ValueError("ساختار داده ذخیره سازی Rich Message معتبر نیست")
                users = {
                    int(value) for value in rich_storage.get("authorized_users", rich_runtime.get("authorized_users", []))
                    if str(value).isdigit() and int(value) > 0
                }
                ai_config = rich_storage.get("ai_config", rich_runtime.get("ai_config", ai_config))
                widget_index_file_id = rich_storage.get("widget_index_file_id")
                runtime_snapshot_file_id = rich_storage.get("runtime_snapshot_file_id")
                runtime_snapshot_sha256 = rich_storage.get("runtime_snapshot_sha256")
                saved_topics = rich_storage.get("topics", {})
                application.bot_data["storage_readable_message_ids"] = rich_storage.get("storage_readable_message_ids", {})
                application.bot_data["storage_message_id"] = pinned.message_id
                application.bot_data["rich_runtime_snapshot"] = rich_runtime
                application.bot_data["rich_storage_sha256"] = (
                    None if rich_storage_uses_legacy_encryption(raw_pinned)
                    else _rich_storage_digest(rich_saved)
                )
            elif not pinned_text:
                raise RuntimeError("پیام پین شده متن قابل خواندن ندارد و داده Rich Message هم دریافت نشد؛ برای جلوگیری از بازنویسی اطلاعات، راه اندازی متوقف شد")

            lines = pinned_text.splitlines()
            first_line = lines[0].strip() if lines else ""
            if rich_saved:
                pass
            elif first_line == PLAIN_STORAGE_MARKER:
                saved = parse_plain_storage(pinned_text)
                if "users" in saved:
                    users = {int(value) for value in saved["users"] if str(value).isdigit() and int(value) > 0}
                ai_config = saved.get("ai_config", ai_config)
                users_file_id = saved.get("users_file_id")
                ai_config_file_id = saved.get("ai_config_file_id")
                widget_index_file_id = saved.get("widget_index_file_id")
                runtime_snapshot_file_id = saved.get("runtime_snapshot_file_id")
                runtime_snapshot_sha256 = saved.get("runtime_snapshot_sha256")
                saved_topics = saved.get("topics", {})
                application.bot_data["storage_readable_message_ids"] = saved.get("storage_readable_message_ids", {})
                application.bot_data["storage_message_id"] = pinned.message_id
            elif first_line == STORAGE_V2_MARKER:
                saved = decrypt_marked_payload(pinned_text)
                users = {int(value) for value in saved["users"] if str(value).isdigit() and int(value) > 0}
                ai_config = saved["ai_config"]
                widget_index_file_id = saved.get("widget_index_file_id")
                application.bot_data["storage_message_id"] = pinned.message_id
            elif first_line == ACCESS_STORAGE_MARKER:
                old_payload = json.loads("\n".join(lines[1:]))
                if isinstance(old_payload, dict):
                    users = {int(value) for value in old_payload.get("users", []) if str(value).isdigit() and int(value) > 0}
                elif isinstance(old_payload, list):
                    users = {int(value) for value in old_payload if str(value).isdigit() and int(value) > 0}
                application.bot_data["storage_message_id"] = pinned.message_id
            else:
                print("Pinned group message is not a bot storage record; checking legacy storage.", flush=True)

        # If this is the first run in the new group, import the old pinned settings.
        if not application.bot_data.get("storage_message_id"):
            needs_widget_copy = True
            try:
                old_chat = await application.bot.get_chat(LEGACY_ACCESS_CHANNEL_ID)
                old_pinned = old_chat.pinned_message
                if old_pinned:
                    old_text = old_pinned.text or old_pinned.caption or ""
                    old_lines = old_text.splitlines()
                    old_marker = old_lines[0].strip() if old_lines else ""
                    if old_marker == STORAGE_V2_MARKER:
                        old_saved = decrypt_marked_payload(old_text)
                        users = {int(v) for v in old_saved.get("users", []) if str(v).isdigit() and int(v) > 0}
                        ai_config = old_saved.get("ai_config", ai_config)
                        widget_index_file_id = old_saved.get("widget_index_file_id")
                    elif old_marker == PLAIN_STORAGE_MARKER:
                        old_saved = parse_plain_storage(old_text)
                        users = {int(v) for v in old_saved.get("users", []) if str(v).isdigit() and int(v) > 0}
                        ai_config = old_saved.get("ai_config", ai_config)
                        widget_index_file_id = old_saved.get("widget_index_file_id")
                        users_file_id = old_saved.get("users_file_id")
                        ai_config_file_id = old_saved.get("ai_config_file_id")
                        runtime_snapshot_file_id = old_saved.get("runtime_snapshot_file_id")
                        saved_topics = old_saved.get("topics", {})
                        application.bot_data["storage_readable_message_ids"] = old_saved.get("storage_readable_message_ids", {})
                    elif old_marker == ACCESS_STORAGE_MARKER:
                        old_payload = json.loads("\n".join(old_lines[1:]))
                        raw_users = old_payload.get("users", []) if isinstance(old_payload, dict) else old_payload
                        users = {int(v) for v in raw_users if str(v).isdigit() and int(v) > 0}
            except Exception as legacy_error:
                legacy_decryption_failed = True
                print(f"Legacy storage import unavailable: {type(legacy_error).__name__}: {legacy_error}", flush=True)

        if users_file_id:
            remote_users = await application.bot.get_file(users_file_id)
            decoded_users = json.loads(bytes(await remote_users.download_as_bytearray()).decode("utf-8"))
            if not isinstance(decoded_users, list):
                raise ValueError("فایل کاربران در گروه تلگرام معتبر نیست")
            users = {int(value) for value in decoded_users if str(value).isdigit() and int(value) > 0}
        elif not users:
            try:
                users = load_access_users()
            except Exception:
                users = set()

        if ai_config_file_id:
            remote_ai = await application.bot.get_file(ai_config_file_id)
            decoded_ai = json.loads(bytes(await remote_ai.download_as_bytearray()).decode("utf-8"))
            if not isinstance(decoded_ai, dict) or not isinstance(decoded_ai.get("services"), dict):
                raise ValueError("فایل تنظیمات هوش مصنوعی در گروه تلگرام معتبر نیست")
            ai_config = decoded_ai

        # Legacy files are read for migration only. New backups keep all state
        # together in the technical snapshot inside the backup topic.
        application.bot_data["users_file_id"] = None
        application.bot_data["ai_config_file_id"] = None
        application.bot_data["widget_index_file_id"] = widget_index_file_id
        application.bot_data["runtime_snapshot_file_id"] = runtime_snapshot_file_id
        application.bot_data["runtime_snapshot_sha256"] = runtime_snapshot_sha256
        application.bot_data.setdefault("storage_readable_message_ids", {})
        application.bot_data["authorized_users"] = users
        application.bot_data["ai_config"] = ai_config

        await ensure_storage_topics(application, saved_topics)
        # Keep the main backup in General so it is visible as soon as the group opens.
        # If an older backup is pinned inside a forum topic, create a new General copy.
        if (
            pinned
            and application.bot_data.get("storage_message_id")
            and getattr(pinned, "message_thread_id", None) is not None
        ):
            application.bot_data.pop("storage_message_id", None)
            application.bot_data["rich_storage_sha256"] = None

        payload = {
            "users_file_id": users_file_id,
            "ai_config_file_id": ai_config_file_id,
            "widget_index_file_id": widget_index_file_id,
            "runtime_snapshot_file_id": runtime_snapshot_file_id,
            "runtime_snapshot_sha256": runtime_snapshot_sha256,
            "topics": application.bot_data["storage_topics"],
        }
        desired_text = plain_storage_text(payload)
        storage_message_id = application.bot_data.get("storage_message_id")
        if not storage_message_id:
            message = await application.bot.send_message(
                chat_id=ACCESS_CHANNEL_ID,
                text=desired_text,
                disable_notification=True,
            )
            await application.bot.pin_chat_message(
                chat_id=ACCESS_CHANNEL_ID,
                message_id=message.message_id,
                disable_notification=True,
            )
            application.bot_data["storage_message_id"] = message.message_id
        elif pinned_text != desired_text:
            await safe_edit_message_text(
                application.bot,
                chat_id=ACCESS_CHANNEL_ID,
                message_id=storage_message_id,
                text=desired_text,
            )

        application.bot_data["access_message_id"] = application.bot_data["storage_message_id"]
        application.bot_data["access_channel_ready"] = True
        save_access_users(users)
        storage_context = type("StorageContext", (), {"application": application, "bot": application.bot})()
        # Access users and AI settings are embedded in the Rich Message now.
        # Restore any legacy runtime file before creating the first Rich Message.

        restored = False
        rich_runtime = application.bot_data.pop("rich_runtime_snapshot", None)
        if rich_runtime is not None:
            restored = apply_runtime_snapshot(application, rich_runtime)
        elif runtime_snapshot_file_id:
            restored = await restore_runtime_state(application, runtime_snapshot_file_id)
        if not restored:
            application.bot_data["source_texts"] = load_source_texts()
            application.bot_data["topic_loop_states"] = load_topic_loop_states()

        try:
            await load_widget_index(application, widget_index_file_id)
            if not widget_index_file_id or needs_widget_copy:
                context = type("StartupContext", (), {"application": application, "bot": application.bot})()
                await save_widget_index(context)
            application.bot_data["widget_archive_ready"] = True
        except Exception as archive_error:
            application.bot_data["widget_archive_ready"] = False
            application.bot_data["widget_archive_error"] = f"{type(archive_error).__name__}: {archive_error}"
            print(f"WIDGET ARCHIVE LOAD FAILED: {application.bot_data['widget_archive_error']}", flush=True)

        await resume_topic_loops(application)
        await persist_runtime_state(application)
        application.create_task(runtime_snapshot_worker(application), name="telegram-runtime-snapshot")
        print(
            f"Telegram storage ready in group {ACCESS_CHANNEL_ID}: users={len(users)}, "
            f"runtime_snapshot={bool(runtime_snapshot_file_id)}, topics={len(application.bot_data['storage_topics'])}",
            flush=True,
        )
        if legacy_decryption_failed:
            print("WARNING: legacy storage could not be fully imported; old group remains untouched.", flush=True)
    except Exception as error:
        # A Telegram storage outage must not erase the locally cached access list.
        # The local JSON file is the last-known-good fallback when the group is unreachable.
        try:
            local_users = load_access_users()
        except Exception as local_error:
            local_users = set()
            print(
                f"LOCAL ACCESS FALLBACK UNAVAILABLE: {type(local_error).__name__}: {local_error}",
                flush=True,
            )
        application.bot_data["authorized_users"] = local_users
        application.bot_data.setdefault("ai_config", ai_default_config())
        application.bot_data["access_channel_ready"] = False
        startup_error = f"{type(error).__name__}: {error}"
        application.bot_data["access_storage_error"] = startup_error
        print(
            "CRITICAL: Telegram group storage unavailable; keeping local access data "
            f"and refusing to overwrite stored data: {startup_error}",
            flush=True,
        )


def load_topic_loop_states():
    data = load_json_store(TOPIC_LOOP_STORE, {})
    if not isinstance(data, dict):
        raise RuntimeError("ساختار ذخیره وضعیت ساخت تاپیک ها معتبر نیست")
    states = {}
    for chat_id, state in data.items():
        if not str(chat_id).lstrip("-").isdigit() or not isinstance(state, dict):
            continue
        next_number = state.get("next_number", 1)
        active = state.get("active", False)
        if isinstance(next_number, int) and next_number > 0 and isinstance(active, bool):
            normalized = {"next_number": next_number, "active": active}
            pending_number = state.get("pending_number")
            if isinstance(pending_number, int) and pending_number > 0:
                normalized["pending_number"] = pending_number
            states[str(int(chat_id))] = normalized
    return states


def save_topic_loop_states(states):
    atomic_write_json(TOPIC_LOOP_STORE, states)


async def create_numbered_topic(application, chat_id):
    states = application.bot_data["topic_loop_states"]
    key = str(chat_id)
    locks = application.bot_data.setdefault("topic_creation_locks", {})
    lock = locks.setdefault(key, asyncio.Lock())
    async with lock:
        state = states.setdefault(key, {"next_number": 1, "active": False})
        if state.get("pending_number") is not None:
            raise RuntimeError(
                f"نتیجه ساخت تاپیک شماره {state['pending_number']} نامشخص است؛ "
                "برای جلوگیری از ساخت تکراری، حلقه متوقف شده"
            )
        number = int(state.get("next_number", 1))
        # Persist an in-flight marker before calling Telegram. If the request
        # times out after Telegram created the topic, a restart won't duplicate it.
        state["pending_number"] = number
        save_topic_loop_states(states)
        try:
            topic = await application.bot.create_forum_topic(
                chat_id=chat_id,
                name=f"تاپیک شماره ({number})",
            )
        except Exception:
            state["active"] = False
            save_topic_loop_states(states)
            raise

        # Advance immediately after Telegram confirms topic creation. A later
        # failure while sending the timestamp must never recreate the same topic.
        state["next_number"] = number + 1
        state.pop("pending_number", None)
        save_topic_loop_states(states)
        try:
            await application.bot.send_message(
                chat_id=chat_id,
                message_thread_id=topic.message_thread_id,
                text=persian_timestamp(),
            )
        except Exception as error:
            print(
                f"TOPIC INITIAL MESSAGE FAILED chat_id={chat_id} "
                f"thread_id={topic.message_thread_id}: {type(error).__name__}: {error}",
                flush=True,
            )
        if chat_id == ACCESS_CHANNEL_ID:
            await persist_runtime_state(application)
        return topic.message_thread_id


async def topic_loop_worker(application, chat_id):
    key = str(chat_id)
    while application.bot_data["topic_loop_states"].get(key, {}).get("active"):
        await asyncio.sleep(60)
        if not application.bot_data["topic_loop_states"].get(key, {}).get("active"):
            break
        try:
            await create_numbered_topic(application, chat_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            state = application.bot_data["topic_loop_states"].get(key, {})
            state["active"] = False
            save_topic_loop_states(application.bot_data["topic_loop_states"])
            print(
                f"TOPIC LOOP PAUSED chat_id={chat_id}: {type(error).__name__}: {error}",
                flush=True,
            )
            break


async def resume_topic_loops(application):
    try:
        states = application.bot_data.get("topic_loop_states")
        if not isinstance(states, dict):
            states = load_topic_loop_states()
    except Exception as error:
        print(
            f"WARNING: topic loop state could not be loaded: {type(error).__name__}: {error}",
            flush=True,
        )
        states = {}
    # The storage group must not start its own topic loop on boot.
    # Topic creation is controlled only by an explicit command in that group.
    storage_group_key = str(ACCESS_CHANNEL_ID)
    storage_group_state = states.setdefault(
        storage_group_key, {"next_number": 1096, "active": False}
    )
    storage_group_state["next_number"] = max(
        int(storage_group_state.get("next_number", 1096)), 1096
    )
    storage_group_state["active"] = False
    save_topic_loop_states(states)

    for key, state in states.items():
        if state.get("pending_number") is not None:
            state["active"] = False
            print(
                f"TOPIC LOOP RECOVERY REQUIRED chat_id={key}: "
                f"creation of topic {state['pending_number']} may have completed; "
                "automatic retry was disabled to prevent duplicates",
                flush=True,
            )
    save_topic_loop_states(states)
    application.bot_data["topic_loop_states"] = states
    application.bot_data["topic_loop_tasks"] = {}
    for key, state in states.items():
        if state.get("active"):
            task = application.create_task(
                topic_loop_worker(application, int(key)),
                name=f"topic-loop-{key}",
            )
            application.bot_data["topic_loop_tasks"][key] = task


async def topic_loop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    command = (message.text or "").strip()
    match = re.fullmatch(r"شرووع(?:\s+([0-9۰-۹٠-٩]+))?|توقف", command)
    if not match:
        return
    requested_number = match.group(1) if command.startswith("شرووع") else None
    if requested_number:
        requested_number = int(requested_number.translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")))
        if requested_number < 1:
            await message.reply_text("شماره باید بزرگتر از صفر باشه.")
            raise ApplicationHandlerStop

    if not chat or chat.type not in ("group", "supergroup"):
        await message.reply_text("این دستور فقط داخل گروه قابل استفاده است.")
        raise ApplicationHandlerStop

    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status not in ("administrator", "creator"):
            await message.reply_text("⛔ فقط مدیران گروه می توانند از این دستور استفاده کنند.")
            raise ApplicationHandlerStop

        bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
        if bot_member.status != "administrator" or not getattr(bot_member, "can_manage_topics", False):
            await message.reply_text("⚠️ ربات باید مدیر گروه باشد و دسترسی مدیریت تاپیک ها را داشته باشد.")
            raise ApplicationHandlerStop

        chat_info = await context.bot.get_chat(chat.id)
        if not getattr(chat_info, "is_forum", False):
            await message.reply_text("⚠️ قابلیت Topics یا انجمن باید در گروه فعال باشد.")
            raise ApplicationHandlerStop

        states = context.application.bot_data.setdefault("topic_loop_states", {})
        tasks = context.application.bot_data.setdefault("topic_loop_tasks", {})
        key = str(chat.id)

        if command.startswith("شرووع"):
            state = states.setdefault(key, {"next_number": 1, "active": False})
            if state.get("pending_number") is not None and requested_number is None:
                await message.reply_text(
                    f"⚠️ نتیجه تلاش قبلی برای ساخت تاپیک شماره {state['pending_number']} مشخص نیست. "
                    "اول گروه رو بررسی کن و بعد شماره بعدی رو با دستور «شرووع عدد» مشخص کن."
                )
                raise ApplicationHandlerStop
            if requested_number is not None:
                state["next_number"] = requested_number
                state.pop("pending_number", None)
            if state.get("active"):
                task = tasks.pop(key, None)
                if task and not task.done():
                    task.cancel()
            state["active"] = True
            save_topic_loop_states(states)
            await persist_runtime_state(context.application)
            try:
                await create_numbered_topic(context.application, chat.id)
            except Exception:
                state["active"] = False
                save_topic_loop_states(states)
                try:
                    await persist_runtime_state(context.application)
                except Exception as rollback_error:
                    print(
                        f"TOPIC LOOP ROLLBACK PERSIST FAILED: {type(rollback_error).__name__}: {rollback_error}",
                        flush=True,
                    )
                raise
            task = context.application.create_task(
                topic_loop_worker(context.application, chat.id),
                name=f"topic-loop-{key}",
            )
            tasks[key] = task
            await message.reply_text("✅ ساخت تاپیک ها شروع شد. هر ۱ دقیقه یک تاپیک جدید ساخته میشه.")
        else:
            state = states.get(key)
            if not state or not state.get("active"):
                await message.reply_text("ساخت تاپیک ها فعلا متوقفه.")
                raise ApplicationHandlerStop
            state["active"] = False
            save_topic_loop_states(states)
            await persist_runtime_state(context.application)
            task = tasks.pop(key, None)
            if task and not task.done():
                task.cancel()
            await message.reply_text("⏹ ساخت تاپیک ها متوقف شد.")
    except ApplicationHandlerStop:
        raise
    except Exception as error:
        await message.reply_text(f"⚠️ اجرای دستور ناموفق بود: {type(error).__name__}: {error}")
    raise ApplicationHandlerStop



def persian_digits(value):
    # Keep system-generated numbers in English digits.
    return str(value)


def gregorian_to_jalali(gy, gm, gd):
    g_days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    gy2 = gy + 1 if gm > 2 else gy
    days = 355666 + 365 * gy + (gy2 + 3) // 4 - (gy2 + 99) // 100 + (gy2 + 399) // 400 + gd + g_days[gm - 1]
    jy = -1595 + 33 * (days // 12053)
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461
    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365
    if days < 186:
        jm = 1 + days // 31
        jd = 1 + days % 31
    else:
        jm = 7 + (days - 186) // 30
        jd = 1 + (days - 186) % 30
    return jy, jm, jd


def persian_timestamp():
    now = datetime.now(ZoneInfo("Asia/Tehran"))
    jy, jm, jd = gregorian_to_jalali(now.year, now.month, now.day)
    date_text = f"{jy:04d}/{jm:02d}/{jd:02d}"
    time_text = now.strftime("%H:%M:%S")
    return persian_digits(f"{date_text} {time_text}")


def audit_callback_label(query):
    """نام قابل نمایش دکمه را از خود پیام پیدا می کند."""
    data = query.data or ""
    markup = getattr(query.message, "reply_markup", None)
    for row in getattr(markup, "inline_keyboard", []) or []:
        for button in row:
            if getattr(button, "callback_data", None) == data:
                return button.text or "دکمه بدون عنوان"

    known = {
        "access:menu": "مدیریت دسترسی", "access:grant": "افزودن کاربر",
        "access:back": "بازگشت", "access:noop": "بدون تغییر",
        "access:revoke": "لغو دسترسی", "main:home": "منوی اصلی",
        "main:rich": "ریچ مسیج", "main:widget": "ویجت",
        "preview": "پیش نمایش", "send": "ارسال",
        "clear": "حذف آخرین بخش", "cancel": "لغو",
        "back:editor": "بازگشت به ویرایشگر",
        "collection:more": "افزودن رسانه",
        "collection:finish": "پایان مجموعه",
        "collection:cancel": "لغو مجموعه",
        "parts:finish_text": "ثبت متن چندبخشی",
        "parts:finish_code": "ثبت کد چندبخشی",
    }
    if data in known:
        return known[data]
    if data.startswith("access:revoke:"):
        return "لغو دسترسی کاربر"
    if data.startswith("add:"):
        labels = {
            "media": "فایل", "p": "متن", "lists": "لیست",
            "h": "تیتر", "d": "بخش تاشو", "btn": "دکمه ها",
            "t": "جدول", "m": "فرمول", "a": "لینک داخلی",
            "src": "منبع", "c": "کد", "div": "جداکننده",
            "f": "پاورقی", "map": "نقشه", "more": "بیشتر", "less": "کمتر",
        }
        return labels.get(data.split(":", 1)[1], "انتخاب قابلیت")
    if data.startswith("list:"):
        return "نوع لیست"
    if data.startswith("heading:"):
        return "اندازه تیتر"
    if data.startswith("checklist:"):
        return "گزینه فهرست کارها"
    if data.startswith("src_show:"):
        return "نمایش منبع"
    if data.startswith("anchor_target:"):
        return "انتخاب مقصد لینک"
    return "دکمه دیگر"


async def ensure_user_audit_topic(context, user):
    """برای هر کاربر یک تاپیک اختصاصی بساز و شناسه آن را در نسخه پشتیبان نگه دار."""
    application = context.application
    bot_data = application.bot_data
    lock = bot_data.setdefault("user_audit_topics_lock", asyncio.Lock())
    async with lock:
        topics = bot_data.setdefault("user_audit_topics", {})
        user_key = str(user.id)
        existing = topics.get(user_key)
        if isinstance(existing, int) and existing > 0:
            return existing

        full_name = " ".join(
            part for part in [user.first_name, user.last_name] if part
        ).strip() or "کاربر"
        topic_name = f"👤 {full_name} · {user.id}"[:128]
        created = await context.bot.create_forum_topic(
            chat_id=ACCESS_CHANNEL_ID,
            name=topic_name,
        )
        thread_id = created.message_thread_id
        topics[user_key] = thread_id
        try:
            await persist_runtime_state(application)
        except Exception as error:
            # Keep the in-memory mapping; the periodic snapshot worker retries it.
            print(
                f"USER AUDIT TOPIC MAP SAVE FAILED user_id={user.id}: "
                f"{type(error).__name__}: {error}",
                flush=True,
            )
        return thread_id


async def audit_log(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ثبت هر رویداد در تاپیک اختصاصی همان کاربر، همراه با رسانه اصلی."""
    try:
        user = update.effective_user
        if not user:
            return

        message = update.effective_message
        chat = update.effective_chat
        if chat and chat.id in (ACCESS_CHANNEL_ID, WIDGET_ARCHIVE_GROUP_ID):
            return

        full_name = " ".join(part for part in [user.first_name, user.last_name] if part).strip() or "ندارد"
        username = f"@{user.username}" if user.username else "ندارد"
        timestamp = persian_timestamp()
        media_block = None
        upload_file = None
        content = ""
        content_label = "محتوای پیام"
        clicked_button = None
        if update.callback_query:
            clicked_button = audit_callback_label(update.callback_query)
            content = ""
        elif message:
            action = "ویرایش پیام" if update.edited_message else "ارسال پیام"
            content = message.text or message.caption or ""
            if context.user_data.get("pending") in ("ai_admin_key", "ai_admin_headers", "connector_api_key", "connector_tool_args"):
                content = "[اطلاعات حساس اتصال یا ورودی ابزار مخفی شد]"

            # رسانه های پشتیبانی شده مستقیما داخل همان Rich Message قرار می گیرند.
            media_specs = [
                ("photo", "photo", "عکس", "photo"),
                ("video", "video", "ویدیو", "video"),
                ("animation", "animation", "گیف", "animation"),
                ("voice", "voice_note", "پیام صوتی", "voice_note"),
                ("audio", "audio", "فایل صوتی", "audio"),
                ("document", "document", "فایل", "document"),
            ]
            for attr, block_type, label, media_type in media_specs:
                value = getattr(message, attr, None)
                if value:
                    media_obj = value[-1] if attr == "photo" else value
                    file_id = media_obj.file_id
                    media_block = {
                        "type": block_type,
                        block_type: {"type": media_type, "media": file_id},
                    }
                    if message.caption:
                        media_block["caption"] = {"text": message.caption}
                    content_label = label
                    break

            # استیکر و ویدیوی گرد بلوک اختصاصی در Rich Message ندارند؛
            # آن ها را به صورت فایل داخل همان پیام Rich Message می فرستیم.
            if not media_block and (message.sticker or message.video_note):
                media_obj = message.sticker or message.video_note
                remote_file = await context.bot.get_file(media_obj.file_id)
                file_bytes = bytes(await remote_file.download_as_bytearray())
                remote_path = remote_file.file_path or ""
                filename = os.path.basename(remote_path) or (
                    "sticker.webp" if message.sticker else "video_note.mp4"
                )
                mimetype = "application/octet-stream"
                media_block = {
                    "type": "document",
                    "document": {"type": "document", "media": "attach://audit_media"},
                    "caption": {"text": "استیکر دریافتی" if message.sticker else "ویدیوی گرد دریافتی"},
                }
                upload_file = ("audit_media", filename, file_bytes, mimetype)
                content_label = "استیکر" if message.sticker else "ویدیوی گرد"

            if not content and not media_block:
                if message.contact:
                    c = message.contact
                    content = f"مخاطب: {c.first_name or ''} {c.last_name or ''} | شماره: {c.phone_number or 'ندارد'} | شناسه: {c.user_id or 'ندارد'}"
                elif message.location:
                    loc = message.location
                    content = f"موقعیت مکانی: عرض جغرافیایی {loc.latitude} ، طول جغرافیایی {loc.longitude}"
                elif message.venue:
                    v = message.venue
                    content = f"مکان: {v.title} | نشانی: {v.address} | مختصات: {v.location.latitude}, {v.location.longitude}"
                elif message.poll:
                    p = message.poll
                    content = "نظرسنجی: " + p.question + " | گزینه ها: " + "، ".join(o.text for o in p.options)
                elif message.dice:
                    content = f"تاس یا ایموجی: {message.dice.emoji} | نتیجه: {message.dice.value}"
                elif message.game:
                    content = f"بازی: {message.game.title}"
                else:
                    media_types = [
                        ("sticker", "استیکر"), ("video_note", "ویدیوی گرد"),
                        ("contact", "مخاطب"), ("location", "موقعیت مکانی"),
                        ("venue", "مکان"), ("poll", "نظرسنجی"),
                    ]
                    content = next((label for attr, label in media_types if getattr(message, attr, None)), "محتوای غیرمتنی")
        else:
            action = "فعالیت دیگر"
            content = "رویداد دیگری از تلگرام دریافت شد"
            content_label = "جزئیات"

        def cell(value, header=False):
            return {
                "text": str(value),
                "is_header": header,
                "align": "center",
                "valign": "middle",
            }

        table_rows = [
            [cell("مشخصات", True), cell("مقدار", True)],
            [cell("کاربر"), cell(full_name)],
            [cell("نام کاربری"), cell(username)],
            [cell("شناسه کاربر"), cell(user.id)],
            [cell("زمان"), cell(timestamp)],
        ]
        blocks = [
            {"type": "heading", "text": "📋 گزارش فعالیت", "size": 2},
            {
                "type": "table",
                "cells": table_rows,
                "is_bordered": True,
                "is_striped": True,
                "is_compact": True,
            },
        ]

        if clicked_button:
            blocks.append({"type": "paragraph", "text": clicked_button})
        elif content or media_block:
            detail_blocks = []
            if content:
                safe_content = str(content)
                if len(safe_content) > 24000:
                    safe_content = safe_content[:24000] + "\n… ادامه محتوا به دلیل محدودیت طول ثبت نشد"
                detail_blocks.append({"type": "paragraph", "text": safe_content})
            if media_block:
                detail_blocks.append(media_block)
            blocks.append({
                "type": "details",
                "summary": f"📨 {content_label}",
                "blocks": detail_blocks,
                "is_open": False,
            })

        audit_thread_id = await ensure_user_audit_topic(context, user)
        rich_payload = {
            "chat_id": ACCESS_CHANNEL_ID,
            "message_thread_id": audit_thread_id,
            "rich_message": {"blocks": blocks, "is_rtl": True},
            "disable_notification": True,
        }
        if upload_file:
            await api_multipart_async("sendRichMessage", rich_payload, [upload_file])
        else:
            await api_async("sendRichMessage", rich_payload)

    except Exception as error:
        print(f"AUDIT LOG FAILED: {type(error).__name__}: {error}", flush=True)
        try:
            user = update.effective_user
            message = update.effective_message
            if user:
                fallback = (
                    "📋 گزارش فعالیت ربات\n"
                    f"👤 {user.full_name} (@{user.username or 'ندارد'})\n"
                    f"🆔 شناسه: {user.id}\n"
                    f"🕒 {persian_timestamp()}\n"
                    f"⚠️ قالب ریچ مسیج ارسال نشد: {type(error).__name__}: {error}"
                )
                if message and (message.text or message.caption):
                    if context.user_data.get("pending") in ("ai_admin_key", "ai_admin_headers"):
                        fallback += "\n\n📨 [اطلاعات محرمانه مخفی شد]"
                    else:
                        fallback += "\n\n📨 محتوا:\n" + (message.text or message.caption)
                audit_thread_id = await ensure_user_audit_topic(context, user)
                await context.bot.send_message(
                    chat_id=ACCESS_CHANNEL_ID,
                    message_thread_id=audit_thread_id,
                    text=fallback[:4096],
                    disable_notification=True,
                )
        except Exception as fallback_error:
            print(f"AUDIT FALLBACK FAILED: {type(fallback_error).__name__}: {fallback_error}", flush=True)


async def persist_access_users(users, context):
    if not context.application.bot_data.get("access_channel_ready"):
        startup_error = context.application.bot_data.get("access_storage_error")
        detail = f"\nعلت اولیه: {startup_error}" if startup_error else ""
        raise RuntimeError("ذخیره سازی کانال آماده نیست؛ اطلاعات قبلی دست نخورده باقی ماند." + detail)
    try:
        await persist_runtime_state(context.application)
        await persist_storage(context, users=users)
        save_access_users(users)
    except Exception as error:
        print(f"PERSISTENT STORAGE UPDATE FAILED: {type(error).__name__}: {error}", flush=True)
        raise


def ai_default_config():
    return {"services": {}, "default_service": None}

def get_default_ai_service(context):
    config = context.application.bot_data.get("ai_config") or ai_default_config()
    services = config.get("services", {})
    selected = config.get("default_service")
    if selected and selected in services:
        return services[selected]
    return next(iter(services.values()), None)

def ai_endpoint(service):
    url = (service.get("url") or "").strip().rstrip("/")
    if not url:
        raise ValueError("آدرس API خالی است.")
    if url.endswith("/chat/completions"):
        return url
    if url.endswith("/v1"):
        return url + "/chat/completions"
    return url + "/v1/chat/completions"

def call_ai_service(service, messages, timeout=120):
    headers = {"Content-Type": "application/json"}
    custom_headers = service.get("headers") or {}
    if not isinstance(custom_headers, dict):
        raise ValueError("ساختار هدرهای سفارشی معتبر نیست.")
    for key, value in custom_headers.items():
        if isinstance(key, str) and isinstance(value, str):
            headers[key] = value
    if service.get("api_key") and not any(k.lower() == "authorization" for k in headers):
        headers["Authorization"] = "Bearer " + service["api_key"]
    body = {
        "model": service.get("model", ""),
        "messages": messages,
        "temperature": 0.7,
    }
    request = urllib.request.Request(
        ai_endpoint(service),
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"سرویس پاسخ خطا داد (HTTP {error.code}). آدرس، مدل، کلید و هدرها رو بررسی کن.") from error
    except urllib.error.URLError as error:
        raise RuntimeError("اتصال به سرویس برقرار نشد. آدرس API یا اینترنت هاست رو بررسی کن.") from error
    except TimeoutError as error:
        raise RuntimeError("پاسخ سرویس هوش مصنوعی بیشتر از 120 ثانیه طول کشید. وضعیت سرویس، مدل و محدودیت سرعتش رو بررسی کن.") from error
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise RuntimeError("پاسخ سرویس JSON معتبر نبود.") from None
    try:
        answer = result["choices"][0]["message"]["content"]
        if isinstance(answer, list):
            answer = "".join(part.get("text", "") if isinstance(part, dict) else str(part) for part in answer)
        if not isinstance(answer, str) or not answer.strip():
            raise ValueError()
        return answer.strip()
    except (KeyError, IndexError, TypeError, ValueError):
        raise RuntimeError("قالب پاسخ این سرویس با API سازگار نیست.") from None

async def request_ai(service, messages):
    return await asyncio.to_thread(call_ai_service, service, messages)

def ai_main_menu(owner=False):
    rows = [
        [InlineKeyboardButton("🧱 ساخت ریچ مسیج با هوش مصنوعی", callback_data="ai:rich")],
        [InlineKeyboardButton("✍️ نوشتن و ویرایش متن", callback_data="ai:text")],
        [InlineKeyboardButton("💻 تولید کد", callback_data="ai:code")],
        [InlineKeyboardButton("💬 گفتگوی مستقیم", callback_data="ai:chat")],
    ]
    if owner:
        rows.append([InlineKeyboardButton("⚙️ مدیریت سرویس های هوش مصنوعی", callback_data="ai:admin")])
    rows.append([InlineKeyboardButton("↩️ منوی اصلی", callback_data="main:home")])
    return InlineKeyboardMarkup(rows)

def ai_admin_menu(config):
    services = config.get("services", {})
    rows = [[InlineKeyboardButton("➕ افزودن سرویس", callback_data="ai:add")]]
    for service_id, service in services.items():
        mark = " ⭐" if config.get("default_service") == service_id else ""
        rows.append([InlineKeyboardButton(
            f"{service.get('name', service_id)[:35]}{mark}",
            callback_data=f"ai:service:{service_id}",
        )])
    rows.append([InlineKeyboardButton("↩️ بازگشت", callback_data="ai:back")])
    return InlineKeyboardMarkup(rows)

def ai_admin_text(config):
    services = config.get("services", {})
    default_id = config.get("default_service")
    default_name = services.get(default_id, {}).get("name", "انتخاب نشده")
    return (
        "⚙️ مدیریت سرویس های هوش مصنوعی\n\n"
        f"تعداد سرویس ها: {len(services)}\n"
        f"سرویس پیش فرض: {default_name}\n\n"
        "برای افزودن سرویس جدید، دکمه افزودن رو بزن.\n"
        "برای تغییر یا حذف سرویس، روی اسمش بزن."
    )

def ai_service_rich_message(service, is_default=False):
    headers = service.get("headers") or {}
    api_key = service.get("api_key") or ""
    fields = [
        ("نام سرویس", service.get("name") or "بدون نام"),
        ("آدرس کامل API", service.get("url") or "تنظیم نشده"),
        ("مدل کامل", service.get("model") or "تنظیم نشده"),
        ("کلید کامل API", api_key or "تنظیم نشده"),
        ("هدرهای سفارشی", json.dumps(headers, ensure_ascii=False, indent=2) if headers else "ندارد"),
    ]
    blocks = [{"type": "paragraph", "text": (
        f"🤖 سرویس: {service.get('name', 'بدون نام')}\n"
        f"وضعیت پیش فرض: {'بله ⭐' if is_default else 'خیر'}\n"
        f"تعداد هدرهای سفارشی: {len(headers)}\n\n"
        "مقدار هر بخش کامل نوشته شده و از بلوک کد میتونی کپی کنی"
    )}]
    for label, value in fields:
        blocks.append({"type": "paragraph", "text": label})
        blocks.append({"type": "pre", "text": value})
    return {"blocks": blocks, "is_rtl": True}

def ai_service_menu(service_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐ انتخاب به عنوان پیش فرض", callback_data=f"ai:default:{service_id}")],
        [InlineKeyboardButton("✏️ ویرایش سرویس", callback_data=f"ai:edit:{service_id}")],
        [InlineKeyboardButton("🗑 حذف سرویس", callback_data=f"ai:delete:{service_id}")],
        [InlineKeyboardButton("↩️ بازگشت", callback_data="ai:admin")],
    ])

def ai_wizard_prompt(step):
    prompts = {
        "name": "اسم این سرویس رو بفرست",
        "url": "آدرس API رو بفرست\n\nآدرس کامل یا آدرس پایه سرویس رو وارد کن",
        "key": "کلید API رو بفرست\n\nاگه کلید لازم نداره، فقط - بفرست",
        "model": "اسم دقیق مدل رو بفرست",
        "headers": "هدرهای سفارشی رو به شکل JSON بفرست\n\nمثال:\n{\"X-API-Key\":\"your-key\"}\n\nاگه هدر اضافه نداری، فقط - بفرست",
    }
    return "⚙️ تنظیم سرویس هوش مصنوعی\n\n" + prompts[step]

def sanitize_rich_blocks(blocks):
    """Recursively normalize Rich Message containers without dropping valid block fields."""
    def clean(value):
        if isinstance(value, list):
            return [clean(item) for item in value]
        if isinstance(value, dict):
            return {key: clean(item) for key, item in value.items()}
        return value
    return clean(blocks)

AI_RICH_BLOCK_TYPES = {
    "paragraph", "heading", "pre", "list", "blockquote",
    "expandable_blockquote", "pullquote", "footer", "divider",
    "mathematical_expression", "anchor", "details", "table", "buttons",
    "map", "collage", "slideshow", "photo", "video", "animation",
    "audio", "voice_note", "document",
}
AI_RICH_TEXT_TYPES = {
    "bold", "italic", "underline", "strikethrough", "spoiler",
    "date_time", "text_mention", "subscript", "superscript", "marked",
    "code", "custom_emoji", "mathematical_expression", "url",
    "email_address", "phone_number", "bank_card_number",
    "mention", "hashtag", "cashtag", "bot_command", "button",
    "anchor", "anchor_link", "reference", "reference_link",
}


def _normalize_ai_button(button):
    if not isinstance(button, dict) or not isinstance(button.get("text"), (str, list)):
        raise ValueError("متن یکی از دکمه ها معتبر نیست.")

    def normalize_button_text(value):
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            return [normalize_button_text(part) for part in value]
        if not isinstance(value, dict) or value.get("type") not in {"custom_emoji", "date_time"}:
            raise ValueError("متن دکمه فقط میتونه متن ساده، ایموجی سفارشی یا تاریخ باشه.")
        return _normalize_ai_rich_text(value)

    button = dict(button)
    button["text"] = normalize_button_text(button["text"])

    valid_actions = {
        "url", "callback_data", "web_app", "login_url",
        "switch_inline_query", "switch_inline_query_current_chat",
        "switch_inline_query_chosen_chat", "copy_text", "disabled",
    }
    actions = valid_actions.intersection(button)
    if len(actions) != 1:
        raise ValueError("هر دکمه باید دقیقا یک عملکرد داشته باشه.")

    action = next(iter(actions))
    value = button[action]
    if action in {"url", "callback_data", "switch_inline_query", "switch_inline_query_current_chat"}:
        if not isinstance(value, str) or not value:
            raise ValueError(f"مقدار عملکرد {action} باید متن معتبر باشه.")
        if action == "callback_data" and len(value.encode("utf-8")) > 64:
            raise ValueError("callback_data دکمه نباید بیشتر از 64 بایت باشه.")
    elif action == "disabled":
        if not isinstance(value, dict):
            raise ValueError("فیلد disabled باید یک آبجکت خالی یا معتبر باشه.")
    elif action == "copy_text":
        if not isinstance(value, dict) or not isinstance(value.get("text"), str) or not 1 <= len(value["text"]) <= 256:
            raise ValueError("متن دکمه کپی باید بین 1 تا 256 کاراکتر باشه.")
    elif action in {"web_app", "login_url"}:
        if not isinstance(value, dict) or not isinstance(value.get("url"), str) or not value["url"].startswith("https://"):
            raise ValueError(f"آدرس {action} باید یک URL معتبر HTTPS باشه.")
    elif action == "switch_inline_query_chosen_chat":
        if not isinstance(value, dict):
            raise ValueError("ساختار switch_inline_query_chosen_chat معتبر نیست.")

    style = button.get("style")
    if style is not None and style not in {"danger", "success", "primary", "link"}:
        raise ValueError("ظاهر دکمه معتبر نیست.")
    if style == "link" and action != "callback_data":
        raise ValueError("ظاهر link فقط برای دکمه callback مجازه.")
    return dict(button)


def _normalize_ai_rich_text(value, depth=0):
    if depth > 16:
        raise ValueError("عمق قالب بندی متن بیشتر از حد مجاز تلگرامه.")
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return [_normalize_ai_rich_text(item, depth + 1) for item in value]
    if not isinstance(value, dict):
        raise ValueError("ساختار متن قالب بندی شده معتبر نیست.")
    kind = value.get("type")
    if kind not in AI_RICH_TEXT_TYPES:
        raise ValueError(f"نوع قالب بندی متن پشتیبانی نمی شه: {kind}")
    item = dict(value)

    text_types = {
        "bold", "italic", "underline", "strikethrough", "spoiler",
        "date_time", "text_mention", "subscript", "superscript", "marked",
        "code", "url", "email_address", "phone_number", "bank_card_number",
        "mention", "hashtag", "cashtag", "bot_command",
        "anchor_link", "reference", "reference_link",
    }
    if kind in text_types:
        if "text" not in item:
            raise ValueError(f"فیلد text در قالب {kind} وجود نداره.")
        item["text"] = _normalize_ai_rich_text(item["text"], depth + 1)

    required = {
        "url": "url", "email_address": "email_address",
        "phone_number": "phone_number", "bank_card_number": "bank_card_number",
        "mention": "username", "hashtag": "hashtag", "cashtag": "cashtag",
        "bot_command": "bot_command", "anchor": "name",
        "anchor_link": "anchor_name", "reference": "name",
        "reference_link": "reference_name",
        "custom_emoji": "custom_emoji_id",
    }
    field = required.get(kind)
    if field and not isinstance(item.get(field), str):
        raise ValueError(f"فیلد {field} برای قالب {kind} اجباریه.")

    if kind == "date_time":
        try:
            item["unix_time"] = int(item["unix_time"])
        except (KeyError, TypeError, ValueError):
            raise ValueError("date_time به unix_time عددی نیاز داره.") from None
        if not isinstance(item.get("date_time_format"), str) or not item["date_time_format"]:
            raise ValueError("date_time به date_time_format نیاز داره.")
    elif kind == "text_mention":
        user = item.get("user")
        if not isinstance(user, dict) or not isinstance(user.get("id"), int):
            raise ValueError("text_mention به آبجکت user با شناسه عددی نیاز داره.")
    elif kind == "custom_emoji":
        if not isinstance(item.get("alternative_text"), str) or not item["alternative_text"]:
            raise ValueError("custom_emoji به alternative_text نیاز داره.")
    elif kind == "mathematical_expression":
        if not isinstance(item.get("expression"), str) or not item["expression"].strip():
            raise ValueError("قالب mathematical_expression به expression نیاز داره.")
    elif kind == "button":
        item["button"] = _normalize_ai_button(item.get("button"))

    return item

def _normalize_ai_rich_block(block, depth=0):
    if depth > 16:
        raise ValueError("تودرتویی بلوک ها بیشتر از حد مجاز تلگرامه.")
    if not isinstance(block, dict):
        raise ValueError("یکی از بلوک ها ساختار معتبری نداره.")
    kind = block.get("type")
    if kind not in AI_RICH_BLOCK_TYPES:
        raise ValueError(f"نوع بلوک پشتیبانی نمی شه: {kind}")
    item = dict(block)
    if kind in {"paragraph", "heading", "pre", "footer", "expandable_blockquote", "pullquote"}:
        if not isinstance(item.get("text"), (str, list, dict)):
            raise ValueError(f"متن بلوک {kind} خالی یا نامعتبره.")
        item["text"] = _normalize_ai_rich_text(item["text"])
    for rich_field in ("credit", "caption"):
        if rich_field in item:
            item[rich_field] = _normalize_ai_rich_text(item[rich_field])
    if kind == "heading":
        try:
            item["size"] = int(item.get("size", 2))
        except (TypeError, ValueError):
            item["size"] = 2
        if item["size"] < 1 or item["size"] > 6:
            item["size"] = 2
    if kind == "pre":
        item["text"] = str(item["text"])
        if "language" in item:
            item["language"] = str(item["language"])[:32]
    if kind in {"blockquote", "details", "collage", "slideshow"}:
        if kind == "details" and not item.get("summary"):
            item["summary"] = "جزئیات"
        children = item.get("blocks", [])
        if not isinstance(children, list):
            raise ValueError(f"فهرست بلوک های داخلی {kind} معتبر نیست.")
        if kind in {"collage", "slideshow"} and not children:
            raise ValueError(f"برای {kind} حداقل یک مدیای واقعی لازمه.")
        item["blocks"] = [_normalize_ai_rich_block(child, depth + 1) for child in children]
    if kind == "blockquote" and not item["blocks"]:
        raise ValueError("بلوک نقل قول خالیه.")
    if kind == "details":
        item["summary"] = _normalize_ai_rich_text(item["summary"])
        if not item["blocks"]:
            item["blocks"] = [{"type": "paragraph", "text": "محتوایی ثبت نشده است."}]
    if kind == "list":
        items = item.get("items")
        if not isinstance(items, list) or not items:
            raise ValueError("فهرست باید حداقل یک مورد داشته باشه.")
        normalized = []
        for entry in items:
            if isinstance(entry, str):
                entry = {"blocks": [{"type": "paragraph", "text": entry}]}
            if not isinstance(entry, dict):
                raise ValueError("یکی از موارد فهرست معتبر نیست.")
            children = entry.get("blocks")
            if not isinstance(children, list):
                label = entry.get("label")
                if not isinstance(label, str) or not label.strip():
                    raise ValueError("هر مورد فهرست باید متن داشته باشه.")
                children = [{"type": "paragraph", "text": label}]
            clean_entry = {key: value for key, value in entry.items() if key != "label"}
            if "has_checkbox" in clean_entry and not isinstance(clean_entry["has_checkbox"], bool):
                raise ValueError("has_checkbox در مورد فهرست باید true یا false باشه.")
            if "is_checked" in clean_entry and not isinstance(clean_entry["is_checked"], bool):
                raise ValueError("is_checked در مورد فهرست باید true یا false باشه.")
            if "value" in clean_entry:
                try:
                    clean_entry["value"] = int(clean_entry["value"])
                except (TypeError, ValueError):
                    raise ValueError("value در مورد فهرست باید عدد صحیح باشه.") from None
            if "type" in clean_entry and clean_entry["type"] not in {"a", "A", "i", "I", "1"}:
                raise ValueError("نوع شماره گذاری فهرست معتبر نیست.")
            normalized.append({
                **clean_entry,
                "blocks": [_normalize_ai_rich_block(x, depth + 1) for x in children],
            })
        item["items"] = normalized
    if kind == "mathematical_expression":
        if not isinstance(item.get("expression"), str) or not item["expression"].strip():
            raise ValueError("بلوک فرمول باید فیلد expression معتبر داشته باشه.")
    if kind == "anchor":
        if not isinstance(item.get("name"), str) or not item["name"].strip():
            raise ValueError("بلوک لنگر باید فیلد name معتبر داشته باشه.")
    if kind == "map":
        location = item.get("location")
        if not isinstance(location, dict):
            raise ValueError("نقشه به location شامل latitude و longitude نیاز داره.")
        try:
            location = {
                "latitude": float(location["latitude"]),
                "longitude": float(location["longitude"]),
            }
            if not (-90 <= location["latitude"] <= 90 and -180 <= location["longitude"] <= 180):
                raise ValueError()
        except (KeyError, TypeError, ValueError):
            raise ValueError("مختصات نقشه معتبر نیست.") from None
        item["location"] = location
        if "zoom" in item:
            try:
                item["zoom"] = int(item["zoom"])
            except (TypeError, ValueError):
                raise ValueError("zoom نقشه باید عددی بین 0 تا 24 باشه.") from None
            if not 0 <= item["zoom"] <= 24:
                raise ValueError("zoom نقشه باید عددی بین 0 تا 24 باشه.")
    media_fields = {
        "photo": "photo", "video": "video", "animation": "animation",
        "audio": "audio", "voice_note": "voice_note", "document": "document",
    }
    if kind in media_fields:
        field = media_fields[kind]
        media = item.get(field)
        if not isinstance(media, dict) or media.get("type") != kind:
            raise ValueError(f"بلوک {kind} به آبجکت {field} معتبر نیاز داره.")
        if not isinstance(media.get("media"), str) or not media["media"].strip():
            raise ValueError(f"بلوک {kind} باید شناسه فایل واقعی داشته باشه؛ شناسه ساختگی قابل قبول نیست.")
        if "caption" in item:
            item["caption"] = _normalize_ai_rich_text(item["caption"])
    if kind == "table":
        cells = item.get("cells")
        if not isinstance(cells, list) or not cells or len(cells) > 100:
            raise ValueError("ساختار جدول معتبر نیست.")
        rows = []
        for row in cells:
            if not isinstance(row, list) or not row or len(row) > 20:
                raise ValueError("هر ردیف جدول باید 1 تا 20 سلول داشته باشه.")
            normalized_row = []
            for cell in row:
                if not isinstance(cell, dict):
                    cell = {"text": str(cell)}
                else:
                    cell = dict(cell)
                if "text" in cell:
                    cell["text"] = _normalize_ai_rich_text(cell["text"])
                for dimension in ("colspan", "rowspan"):
                    if dimension in cell:
                        try:
                            cell[dimension] = int(cell[dimension])
                        except (TypeError, ValueError):
                            raise ValueError(f"{dimension} سلول باید عدد مثبت باشه.") from None
                        if cell[dimension] < 1:
                            raise ValueError(f"{dimension} سلول باید عدد مثبت باشه.")
                if cell.get("align") not in (None, "left", "center", "right"):
                    raise ValueError("چینش افقی سلول جدول معتبر نیست.")
                if cell.get("valign") not in (None, "top", "middle", "bottom"):
                    raise ValueError("چینش عمودی سلول جدول معتبر نیست.")
                normalized_row.append(cell)
            rows.append(normalized_row)
        item["cells"] = rows
    if kind == "buttons":
        buttons = item.get("buttons")
        if not isinstance(buttons, list) or not 1 <= len(buttons) <= 8:
            raise ValueError("هر ردیف دکمه باید بین 1 تا 8 دکمه داشته باشه.")
        item["buttons"] = [_normalize_ai_button(button) for button in buttons]
        if item.get("align") not in (None, "left", "center", "right"):
            item["align"] = "center"
    return item

def _is_ai_block_array(value):
    return (
        isinstance(value, list)
        and bool(value)
        and all(
            isinstance(block, dict)
            and block.get("type") in AI_RICH_BLOCK_TYPES
            for block in value
        )
    )


def _find_ai_blocks(value, depth=0):
    if depth > 20:
        return None
    if isinstance(value, str):
        candidate = value.strip()
        if candidate.startswith(("{", "[")):
            try:
                return _find_ai_blocks(json.loads(candidate), depth + 1)
            except (json.JSONDecodeError, TypeError):
                return None
        return None
    if isinstance(value, list):
        if _is_ai_block_array(value):
            return value
        for item in value:
            found = _find_ai_blocks(item, depth + 1)
            if found is not None:
                return found
        return None
    if not isinstance(value, dict):
        return None

    # Prefer explicit block containers before inspecting wrapper fields.
    for key in ("blocks", "rich_message", "message", "result", "data", "output", "content", "items"):
        if key in value:
            found = _find_ai_blocks(value[key], depth + 1)
            if found is not None:
                return found
    for key, item in value.items():
        if key not in {"cells", "rows", "columns", "buttons"}:
            found = _find_ai_blocks(item, depth + 1)
            if found is not None:
                return found
    return None


def _extract_ai_json(answer):
    if not isinstance(answer, str) or not answer.strip():
        raise ValueError("هوش مصنوعی پاسخ خالی فرستاد.")

    raw = answer.strip()
    decoder = json.JSONDecoder()
    candidates = [raw]

    # Prefer an explicit JSON code block when the provider wraps its response.
    for match in re.finditer(r"\x60{3}([^\n\x60]*)\n?(.*?)\x60{3}", raw, re.S):
        label = (match.group(1) or "").strip().lower()
        body = (match.group(2) or "").strip()
        if label in ("", "json", "javascript", "js"):
            candidates.insert(0, body)

    for candidate in candidates:
        try:
            found = _find_ai_blocks(json.loads(candidate))
            if found is not None:
                return found
        except json.JSONDecodeError:
            pass

    # Also recover a complete JSON object/array surrounded by explanatory text.
    for candidate in candidates:
        for index, char in enumerate(candidate):
            if char not in "[{":
                continue
            try:
                value, _ = decoder.raw_decode(candidate, index)
            except json.JSONDecodeError:
                continue
            found = _find_ai_blocks(value)
            if found is not None:
                return found

    raise ValueError(
        "خروجی هوش مصنوعی شامل بلوک های قابل استفاده ریچ مسیج نبود. "
        "ساختار پاسخ مدل قابل تشخیص نبود."
    )


def _rich_text_byte_count(value):
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    if isinstance(value, list):
        return sum(_rich_text_byte_count(part) for part in value)
    if not isinstance(value, dict):
        return 0

    total = 0
    if "text" in value:
        total += _rich_text_byte_count(value["text"])
    if "expression" in value:
        total += _rich_text_byte_count(value["expression"])
    if "alternative_text" in value:
        total += _rich_text_byte_count(value["alternative_text"])
    if "button" in value and isinstance(value["button"], dict):
        total += _rich_text_byte_count(value["button"].get("text", ""))
    return total


def _rich_message_stats(blocks):
    total_blocks = 0
    text_bytes = 0

    def visit(block):
        nonlocal total_blocks, text_bytes
        total_blocks += 1
        for field in ("text", "expression", "summary", "caption", "credit"):
            if field in block:
                text_bytes += _rich_text_byte_count(block[field])

        children = block.get("blocks")
        if isinstance(children, list):
            for child in children:
                visit(child)

        if block.get("type") == "list":
            for entry in block.get("items", []):
                total_blocks += 1  # Telegram counts every list item as a block.
                if isinstance(entry, dict):
                    if "label" in entry:
                        text_bytes += _rich_text_byte_count(entry["label"])
                    for child in entry.get("blocks", []):
                        visit(child)

        if block.get("type") == "table":
            for row in block.get("cells", []):
                total_blocks += 1  # Telegram counts every table row as a block.
                for cell in row:
                    if isinstance(cell, dict) and "text" in cell:
                        text_bytes += _rich_text_byte_count(cell["text"])

        if block.get("type") == "buttons":
            for button in block.get("buttons", []):
                if isinstance(button, dict):
                    text_bytes += _rich_text_byte_count(button.get("text", ""))

    for block in blocks:
        visit(block)
    return total_blocks, text_bytes


def parse_ai_blocks(answer):
    value = _extract_ai_json(answer)
    if isinstance(value, dict):
        value = value.get("blocks")
    if not isinstance(value, list) or not value or len(value) > MAX_RICH_BLOCKS:
        raise ValueError("ساختار بلوک های تولیدشده معتبر نیست.")

    normalized = []
    for index, block in enumerate(value, 1):
        if not isinstance(block, dict):
            raise ValueError(f"بلوک شماره {index} باید یک آبجکت باشد.")
        kind = block.get("type")
        if not isinstance(kind, str) or not kind.strip():
            keys = ", ".join(str(key) for key in list(block.keys())[:8]) or "خالی"
            raise ValueError(f"بلوک شماره {index} فیلد type معتبر ندارد. فیلدهای دریافتی: {keys}")
        try:
            normalized.append(_normalize_ai_rich_block(block))
        except (ValueError, TypeError, KeyError) as error:
            raise ValueError(f"بلوک شماره {index} ({kind}): {error}") from error
    total_blocks, text_bytes = _rich_message_stats(normalized)
    if total_blocks > MAX_RICH_BLOCKS:
        raise ValueError(
            f"تعداد کل بلوک ها، موارد لیست و ردیف های جدول {total_blocks} است؛ "
            f"حداکثر مجاز تلگرام {MAX_RICH_BLOCKS} مورد است."
        )
    if text_bytes > MAX_RICH_TEXT_BYTES:
        raise ValueError(
            f"حجم متن ریچ مسیج {text_bytes} بایت است؛ "
            f"حداکثر مجاز تلگرام {MAX_RICH_TEXT_BYTES} بایت UTF-8 است."
        )
    return sanitize_rich_blocks(normalized)

async def send_long_text(message, text):
    text = str(text)
    for start in range(0, len(text), 3900):
        await message.reply_text(text[start:start + 3900])

async def run_ai_one_shot(message, context, mode, prompt):
    service = get_default_ai_service(context)
    if not service:
        await message.reply_text("⚠️ هنوز سرویس هوش مصنوعی تنظیم نشده. از مدیر ربات بخواه سرویس پیش فرض رو فعال کنه.")
        return
    await message.reply_text("🧠 دارم آماده میکنم...")
    try:
        if mode == "rich":
            system = (
                "You are a senior composer for Telegram Bot API 10.3 Rich Messages. "
                "Return ONLY a JSON object containing one top-level field named blocks, whose value is a non-empty array. "
                "The blocks value must be a non-empty array. Never return Markdown, HTML, prose, or code fences. "
                "Use the user's language. Build a complete, polished message that follows the request, not a generic template. "
                "Supported block types: paragraph, heading, pre, footer, divider, mathematical_expression, anchor, list, "
                "blockquote, expandable_blockquote, pullquote, collage, slideshow, table, details, map, buttons, "
                "photo, video, animation, audio, voice_note, document. "
                "SCHEMA: paragraph/heading/pre/footer/expandable_blockquote/pullquote have text; heading also has integer size 1-6; "
                "pre may have language; mathematical_expression has expression; anchor has name; "
                "list has items array and each item has blocks array; blockquote/details/collage/slideshow have blocks array; "
                "details also has summary and optional is_open; table has cells as array of rows, each row is an array of cell objects; "
                "cell text can be string or RichText and may include is_header, colspan, rowspan, align, valign; "
                "map has location:{latitude,longitude} and optional zoom 0-24; "
                "buttons has 1-8 buttons and optional align left/center/right. "
                "Each RichMessageButton has text and exactly one action: url, callback_data, web_app, login_url, "
                "switch_inline_query, switch_inline_query_current_chat, switch_inline_query_chosen_chat, copy_text, or disabled. "
                "disabled must be {}. callback_data must be at most 64 UTF-8 bytes. copy_text.text must be 1-256 characters. "
                "web_app and login_url URLs must use HTTPS. Button style is danger, success, primary, or link; link is only valid with callback_data. "
                "Media blocks use a matching nested field whose object is {type:<same block type>,media:<real Telegram file_id>}. "
                "Never invent, guess, or fabricate file IDs, media IDs, callback handlers, user IDs, URLs, prices, or facts. "
                "Only use a media block when the user explicitly supplied a real usable file_id. Otherwise omit media blocks; "
                "the bot will let the user attach real photos, videos, audio, voice notes and documents in the editor after this draft. "
                "Collage and slideshow must contain real media blocks only; do not create empty placeholders. "
                "RichText may use: bold, italic, underline, strikethrough, spoiler, date_time, text_mention, subscript, "
                "superscript, marked, code, custom_emoji, mathematical_expression, url, email_address, phone_number, "
                "bank_card_number, mention, hashtag, cashtag, bot_command, button, anchor, anchor_link, reference, reference_link. "
                "For text mention, user.id must be an actual numeric Telegram user ID supplied by the user. "
                "Use exact JSON field names and correct value types. Keep nesting within 16 levels, total blocks/items/rows within 500, "
                "table width within 20 columns and text within Telegram limits. "
                "Do not add an invented publish button inside the message. The bot's editor provides the publish action. "
                "Return only the JSON object, no text before or after it."
            )
        elif mode == "code":
            system = "You are an expert software developer. Return the requested code in a fenced code block. Add a short explanation only when useful."
        else:
            system = "You are a careful writing assistant. Write or edit the text as requested. Return the final text directly."
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt[:12000]},
        ]
        answer = await request_ai(service, messages)
        if mode == "rich":
            parse_error = None
            for attempt in range(3):
                try:
                    candidate = answer if attempt == 0 else await request_ai(
                        service,
                        messages + [
                            {"role": "assistant", "content": answer[:24000]},
                            {"role": "user", "content": (
                                "The previous response was not accepted. Return a complete corrected JSON object "
                                "with exactly one top-level blocks array. Do not explain. "
                                "Use only documented Telegram Bot API 10.3 InputRichBlock fields. "
                                "Do not invent media IDs or URLs. "
                                + ("Previous validation error: " + str(parse_error) if parse_error else
                                   "Your previous output was not valid JSON or had no usable blocks.")
                            )},
                        ],
                    )
                    blocks = parse_ai_blocks(candidate)
                    break
                except (ValueError, TypeError, KeyError) as error:
                    parse_error = error
                    answer = candidate if isinstance(candidate, str) else answer
            else:
                raise ValueError(f"مدل بعد از 3 تلاش خروجی معتبر نداد: {parse_error}")
            context.user_data["blocks"] = blocks
            context.user_data["ai_rich_draft"] = True
            context.user_data["menu_expanded"] = True
            await send_editor_reply(message, context)
        elif mode == "code":
            match = re.search(r"\x60{3}(?:[A-Za-z0-9_+.-]*)\n?(.*?)\x60{3}", answer, re.S)
            code = match.group(1) if match else answer
            context.user_data["ai_code_result"] = code
            await message.reply_text(
                "✅ کد آماده شد\nبرای ثبت در ویرایشگر یا تبدیل به متن چندبخشی انتخاب کن",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💻 ثبت در ویرایشگر ریچ", callback_data="ai:code_rich")],
                    [InlineKeyboardButton("🧩 تبدیل به متن چندبخشی", callback_data="ai:code_parts")],
                    [InlineKeyboardButton("🏠 منوی اصلی", callback_data="main:home")],
                ]),
            )
            await send_long_text(message, answer[:12000])
        else:
            await send_long_text(message, answer[:12000])
    except Exception as error:
        if mode == "rich":
            print(f"AI RICH MESSAGE FAILED: {type(error).__name__}: {error}", flush=True)
            await message.reply_text(
                "⚠️ ساخت ریچ مسیج کامل نشد\n"
                "یه بار دیگه امتحان کن. اگه دوباره تکرار شد، متن درخواست یا نوع خروجی مدل رو باید بررسی کنیم."
            )
        else:
            await message.reply_text(f"⚠️ درخواست ناموفق بود\n{type(error).__name__}: {error}")


async def run_ai_widget(message, context, prompt):
    service = get_default_ai_service(context)
    if not service:
        await message.reply_text("⚠️ هنوز سرویس پیش فرض هوش مصنوعی تنظیم نشده.")
        return

    await message.reply_text("🧠 دارم ویجت رو میسازم...")
    system = (
        "You are an expert HTML widget developer for Telegram Bot API sendWidget. "
        "Return only one complete, self-contained HTML document. No Markdown fences and no explanation. "
        "Put all CSS inside a style tag and all JavaScript inside a script tag. "
        "Do not use external libraries, external fonts, network requests, fetch, XMLHttpRequest, or remote assets. "
        "Make the interface responsive, touch-friendly, accessible, and visually polished. "
        "Use the user's language and RTL direction when the request is Persian. "
        "Keep the raw HTML under 32 KB and make its zlib-compressed UTF-8 size under 4 KB. "
        "Avoid unnecessary whitespace, comments, and repeated code. Do not include code fences."
    )
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": prompt[:10000]},
    ]

    def extract_html(answer):
        if not isinstance(answer, str) or not answer.strip():
            raise ValueError("هوش مصنوعی کد HTML خالی فرستاد.")
        match = re.search(r"\x60{3}(?:html|htm)?\s*(.*?)\x60{3}", answer, re.S | re.I)
        html = match.group(1).strip() if match else answer.strip()
        if not re.search(r"<[a-zA-Z][^>]*>", html):
            raise ValueError("خروجی هوش مصنوعی کد HTML معتبر به نظر نمی رسه.")
        return html

    try:
        answer = await request_ai(service, messages)
        try:
            html = extract_html(answer)
            validate_widget_html(html)
        except ValueError as first_error:
            repair_messages = messages + [
                {"role": "assistant", "content": answer[:20000]},
                {"role": "user", "content": (
                    "Fix the HTML output so it passes these constraints. Return the complete HTML only. "
                    "Keep the requested features. Validation error: " + str(first_error)
                )},
            ]
            repaired = await request_ai(service, repair_messages)
            html = extract_html(repaired)
            validate_widget_html(html)

        await send_html_widget(message, html, context)
        context.user_data.pop("pending", None)
    except Exception as error:
        await message.reply_text(
            f"⚠️ ساخت یا ارسال ویجت ناموفق بود\n{type(error).__name__}: {error}"
        )


def parse_ai_chat_plan(raw):
    text = (raw or "").strip()
    if text.startswith(chr(96) * 3):
        text = re.sub(r"^\x60{3}(?:json)?\s*|\s*\x60{3}$", "", text, flags=re.I).strip()
    try:
        plan = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return {"type": "chat", "reply": (raw or "").strip()}
    if not isinstance(plan, dict) or plan.get("type") not in {"chat", "edit"}:
        return {"type": "chat", "reply": (raw or "").strip()}
    if plan["type"] == "chat":
        reply = plan.get("reply")
        return {"type": "chat", "reply": reply.strip() if isinstance(reply, str) else "متوجه شدم"}
    operations = plan.get("operations")
    if not isinstance(operations, list) or not operations or len(operations) > 20:
        raise ValueError("فهرست تغییرات هوش مصنوعی معتبر نیست.")
    reply = plan.get("reply")
    return {
        "type": "edit",
        "reply": reply.strip() if isinstance(reply, str) else "تغییرات پیشنهادی آماده است",
        "operations": operations,
    }


def prepare_ai_chat_edit(context, operations):
    current = context.user_data.get("blocks", [])
    if not isinstance(current, list):
        raise ValueError("پیش نویس فعلی ساختار معتبری نداره.")
    base = json.loads(json.dumps(current, ensure_ascii=False))
    updated = json.loads(json.dumps(base, ensure_ascii=False))
    media_types = {"photo", "video", "animation", "audio", "voice_note", "document", "collage", "slideshow"}

    for operation in operations:
        if not isinstance(operation, dict):
            raise ValueError("یکی از دستورهای تغییر معتبر نیست.")
        action = operation.get("op")
        if action == "append":
            additions = operation.get("blocks")
            if not isinstance(additions, list) or not additions:
                raise ValueError("برای افزودن، فهرست بلوک ها معتبر نیست.")
            for block in additions:
                if not isinstance(block, dict) or block.get("type") in media_types:
                    raise ValueError("هوش مصنوعی اجازه ساخت شناسه مدیای غیرواقعی رو نداره.")
                updated.append(_normalize_ai_rich_block(block))
        elif action == "replace":
            index = operation.get("index")
            block = operation.get("block")
            if not isinstance(index, int) or isinstance(index, bool) or not 1 <= index <= len(updated):
                raise ValueError("شماره بلوک برای جایگزینی معتبر نیست.")
            if not isinstance(block, dict) or block.get("type") in media_types:
                raise ValueError("بلوک جایگزین معتبر نیست یا شامل مدیای ساختگیه.")
            updated[index - 1] = _normalize_ai_rich_block(block)
        elif action == "delete":
            index = operation.get("index")
            if not isinstance(index, int) or isinstance(index, bool) or not 1 <= index <= len(updated):
                raise ValueError("شماره بلوک برای حذف معتبر نیست.")
            updated.pop(index - 1)
        elif action == "move":
            source = operation.get("from")
            target = operation.get("to")
            if (
                not isinstance(source, int) or isinstance(source, bool)
                or not isinstance(target, int) or isinstance(target, bool)
                or not 1 <= source <= len(updated) or not 1 <= target <= len(updated)
            ):
                raise ValueError("شماره بلوک برای جابه جایی معتبر نیست.")
            block = updated.pop(source - 1)
            updated.insert(target - 1, block)
        elif action == "set_text":
            index = operation.get("index")
            value = operation.get("text")
            if not isinstance(index, int) or isinstance(index, bool) or not 1 <= index <= len(updated):
                raise ValueError("شماره بلوک برای ویرایش معتبر نیست.")
            if not isinstance(value, str):
                raise ValueError("متن جایگزین باید رشته باشه.")
            block = updated[index - 1]
            if block.get("type") not in {"paragraph", "heading", "pre", "footer", "pullquote", "expandable_blockquote"}:
                raise ValueError("این نوع بلوک متن قابل ویرایش مستقیم نداره.")
            block["text"] = value
        else:
            raise ValueError(f"عملیات پشتیبانی نشده: {action}")

        if len(updated) > MAX_RICH_BLOCKS:
            raise ValueError("تعداد بلوک ها از سقف 500 بیشتر میشه.")

    total_blocks, text_bytes = _rich_message_stats(updated)
    if total_blocks > MAX_RICH_BLOCKS:
        raise ValueError(f"تعداد کل بلوک ها و موارد داخلی از سقف {MAX_RICH_BLOCKS} بیشتر میشه.")
    if text_bytes > MAX_RICH_TEXT_BYTES:
        raise ValueError(f"حجم متن از سقف {MAX_RICH_TEXT_BYTES} بایت UTF-8 بیشتر میشه.")
    return base, sanitize_rich_blocks(updated)


def ai_chat_edit_summary(operations):
    labels = {
        "append": "افزودن بخش جدید",
        "replace": "جایگزینی یک بخش",
        "delete": "حذف یک بخش",
        "move": "جابجایی بخش",
        "set_text": "ویرایش متن یک بخش",
    }
    return "\n".join(
        f"• {labels.get(item.get('op'), 'تغییر')} "
        + (f"(بخش {item.get('index')})" if item.get("index") else "")
        for item in operations
    )


async def run_ai_chat(message, context, prompt):
    service = get_default_ai_service(context)
    if not service:
        await message.reply_text("⚠️ هنوز سرویس پیش فرض هوش مصنوعی تنظیم نشده.")
        return
    history = context.user_data.setdefault("ai_chat_history", [])
    history.append({"role": "user", "content": prompt[:8000]})
    history[:] = history[-20:]
    system_prompt = (
        "You are the conversational assistant inside a Telegram Rich Message editor. "
        "Reply in the same language as the user. You can chat normally or propose edits to the current draft. "
        "The current draft is provided in the user context below. "
        "If the user clearly asks to change the draft, return ONLY valid JSON with this schema: "
        '{"type":"edit","reply":"short explanation in the user language","operations":[...]} . '
        "Allowed operations: "
        '{"op":"append","blocks":[Rich Message block objects]}, '
        '{"op":"replace","index":1,"block":Rich Message block object}, '
        '{"op":"delete","index":1}, '
        '{"op":"move","from":1,"to":2}, '
        '{"op":"set_text","index":1,"text":"replacement text"}. '
        "Indexes are 1-based. Operations run in order, and each index refers to the draft after earlier operations. "
        "Use only the smallest necessary operations. Never invent media file IDs. "
        "Do not modify or remove existing media unless the user explicitly asks. "
        "For normal questions or unclear requests, return ONLY JSON: "
        '{"type":"chat","reply":"your answer"}. '
        "Do not claim an edit was applied. The user must confirm it first. "
        "Current draft JSON: " + json.dumps(context.user_data.get("blocks", []), ensure_ascii=False)[:24000]
    )
    try:
        raw = await request_ai(service, [
            {"role": "system", "content": system_prompt},
            *history,
        ])
        plan = parse_ai_chat_plan(raw)
        if plan["type"] == "chat":
            answer = plan["reply"][:12000]
            history.append({"role": "assistant", "content": answer})
            history[:] = history[-20:]
            await send_long_text(message, answer)
            return

        base, updated = prepare_ai_chat_edit(context, plan["operations"])
        context.user_data["ai_chat_pending_edit"] = {
            "base": base,
            "blocks": updated,
            "reply": plan["reply"][:2000],
            "operations": plan["operations"],
        }
        history.append({"role": "assistant", "content": plan["reply"][:2000]})
        history[:] = history[-20:]
        summary = ai_chat_edit_summary(plan["operations"])
        await message.reply_text(
            "🛠 تغییرات پیشنهادی آماده شد\n\n"
            + plan["reply"][:2000] + "\n\n"
            + summary + "\n\n"
            + "اگه تایید کنی روی پیش نویس فعلی اعمال میشه",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ تایید و اعمال", callback_data="ai:chat_apply"),
                InlineKeyboardButton("❌ لغو", callback_data="ai:chat_cancel"),
            ]]),
        )
    except Exception as error:
        if history and history[-1].get("role") == "user":
            history.pop()
        context.user_data.pop("ai_chat_pending_edit", None)
        await message.reply_text(f"⚠️ گفتگو ناموفق بود\n{type(error).__name__}: {error}")


def is_owner(user):
    return bool(user and OWNER_ID > 0 and user.id == OWNER_ID)

def is_authorized(context, user):
    if not user:
        return False
    return is_owner(user) or user.id in context.bot_data.get("authorized_users", set())

def log_incoming_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    message_text = message.text or message.caption or "" if message else ""
    print(
        "INCOMING UPDATE: "
        f"update_id={update.update_id} "
        f"user_id={user.id if user else None} "
        f"chat_type={update.effective_chat.type if update.effective_chat else None} "
        f"message={bool(message)} "
        f"text_length={len(message_text)}",
        flush=True,
    )


async def access_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        raise ApplicationHandlerStop

    # Ignore bot-authored updates and Telegram service messages (for example,
    # the service update emitted when this bot creates a forum topic).
    if user.is_bot:
        return
    message = update.effective_message
    service_fields = (
        "forum_topic_created", "forum_topic_edited", "forum_topic_closed",
        "forum_topic_reopened", "general_forum_topic_hidden",
        "general_forum_topic_unhidden", "new_chat_members", "left_chat_member",
        "new_chat_title", "new_chat_photo", "delete_chat_photo",
        "group_chat_created", "supergroup_chat_created", "migrate_to_chat_id",
        "migrate_from_chat_id", "pinned_message", "video_chat_started",
        "video_chat_ended", "video_chat_scheduled", "video_chat_participants_invited",
        "write_access_allowed", "connected_website",
    )
    if message and any(getattr(message, field, None) for field in service_fields):
        return

    # Anyone may request their own numeric ID so the owner can grant access.
    message_parts = (update.message.text or "").split(maxsplit=1) if update.message else []
    if (message_parts and message_parts[0].split("@", 1)[0] in ("/myid", "/start")) or (
        update.message and (update.message.text or "").strip() == "م"
    ):
        return

    if OWNER_ID <= 0:
        notice = "🔒 دسترسی ربات هنوز تنظیم نشده است. مدیر باید متغیر OWNER_ID را در هاست تنظیم کند."
    elif is_authorized(context, user):
        return
    else:
        notice = "🔒 دسترسی شما به این ربات فعال نیست"

    if update.callback_query:
        callback_notice = (
            "🔒 دسترسی شما به این ربات فعال نیست"
            if OWNER_ID > 0
            else notice
        )
        await update.callback_query.answer(callback_notice, show_alert=True)
        if OWNER_ID > 0:
            draft_text = f"بهم دسترسی بده `{user.id}` ممنون"
            request_url = "https://t.me/M7M7777777?text=" + quote(draft_text, safe="")
            await api_async("sendRichMessage", {
                "chat_id": update.effective_chat.id,
                "rich_message": {
                    "blocks": [
                        {
                            "type": "paragraph",
                            "text": "برای درخواست دسترسی روی دکمه زیر بزن:",
                        },
                        {
                            "type": "buttons",
                            "buttons": [{
                                "text": "دسترسی میقام",
                                "style": "danger",
                                "url": request_url,
                            }],
                            "align": "center",
                        },
                    ],
                    "is_rtl": True,
                },
            })
    elif update.message:
        if OWNER_ID > 0 and not is_authorized(context, user):
            draft_text = f"بهم دسترسی بده `{user.id}` ممنون"
            request_url = "https://t.me/M7M7777777?text=" + quote(draft_text, safe="")
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "📩 درخواست دسترسی",
                    url=request_url,
                    api_kwargs={"style": "danger"},
                )
            ]])
            await update.message.reply_text(notice, reply_markup=keyboard)
        else:
            await update.message.reply_text(notice)
    raise ApplicationHandlerStop

async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(f"شناسه عددی تلگرام شما: {update.effective_user.id}")

async def version_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(f"نسخه فعلی ربات: {VERSION}")

async def grant_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user):
        await update.effective_message.reply_text("⛔ فقط مدیر اصلی می‌تواند دسترسی بدهد.")
        return
    if len(context.args) != 1 or not context.args[0].isdigit() or int(context.args[0]) <= 0:
        await update.effective_message.reply_text("فرمت درست: /grant 123456789")
        return
    user_id = int(context.args[0])
    if user_id == OWNER_ID:
        await update.effective_message.reply_text("مدیر اصلی از قبل دسترسی کامل دارد.")
        return
    users = context.bot_data.setdefault("authorized_users", set())
    if user_id in users:
        await update.effective_message.reply_text("این کاربر از قبل دسترسی دارد.")
        return
    users.add(user_id)
    try:
        await persist_access_users(users, context)
    except Exception as error:
        users.discard(user_id)
        print(f"ACCESS GRANT COMMAND FAILED: {type(error).__name__}: {error}", flush=True)
        await update.effective_message.reply_text(
            f"⚠️ ذخیره دسترسی ناموفق بود\n{type(error).__name__}: {error}"
        )
        return
    await update.effective_message.reply_text(f"✅ دسترسی برای کاربر {user_id} فعال شد.")

async def revoke_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user):
        await update.effective_message.reply_text("⛔ فقط مدیر اصلی می‌تواند دسترسی قطع کند.")
        return
    if len(context.args) != 1 or not context.args[0].isdigit() or int(context.args[0]) <= 0:
        await update.effective_message.reply_text("فرمت درست: /revoke 123456789")
        return
    user_id = int(context.args[0])
    if user_id == OWNER_ID:
        await update.effective_message.reply_text("دسترسی مدیر اصلی قابل لغو نیست.")
        return
    users = context.bot_data.setdefault("authorized_users", set())
    if user_id not in users:
        await update.effective_message.reply_text("این کاربر در فهرست دسترسی‌ها نیست.")
        return
    users.remove(user_id)
    try:
        await persist_access_users(users, context)
    except Exception:
        users.add(user_id)
        await update.effective_message.reply_text("⚠️ ذخیره تغییر ناموفق بود؛ دسترسی قبلی حفظ شد.")
        return
    await update.effective_message.reply_text(f"🚫 دسترسی کاربر {user_id} لغو شد.")

async def list_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user):
        await update.effective_message.reply_text("⛔ فقط مدیر اصلی می‌تواند فهرست دسترسی‌ها را ببیند.")
        return
    users = sorted(context.bot_data.get("authorized_users", set()))
    if not users:
        await update.effective_message.reply_text("فعلاً به‌جز مدیر اصلی، هیچ کاربری دسترسی ندارد.")
        return
    await update.effective_message.reply_text("👥 کاربران دارای دسترسی:\n" + "\n".join(str(user_id) for user_id in users))


def access_menu_markup(users):
    keyboard = [[InlineKeyboardButton("➕ افزودن کاربر", callback_data="access:grant")]]
    if users:
        for user_id in sorted(users):
            keyboard.append([InlineKeyboardButton(f"🚫 حذف دسترسی {user_id}", callback_data=f"access:revoke:{user_id}")])
    else:
        keyboard.append([InlineKeyboardButton("فعلاً کاربری اضافه نشده", callback_data="access:noop")])
    keyboard.append([InlineKeyboardButton("↩️ برگشت به ربات", callback_data="access:back")])
    return InlineKeyboardMarkup(keyboard)

def access_menu_text(users):
    return "🔐 مدیریت دسترسی کاربران\n\n" + (
        "برای افزودن کاربر روی «➕ افزودن کاربر» بزن و شناسه عددی‌اش را بفرست.\n"
        "برای قطع دسترسی، دکمه کنار شناسه همان کاربر را بزن.\n\n"
        if users else
        "فعلاً فقط خودت به ربات دسترسی داری. برای افزودن کاربر روی دکمه زیر بزن.\n\n"
    ) + f"تعداد کاربران مجاز: {len(users)}"

def get_source_texts(context):
    sources = context.bot_data.get("source_texts")
    if sources is None:
        sources = load_source_texts()
        context.bot_data["source_texts"] = sources
    return sources

def add_block(context, block):
    blocks = context.user_data.setdefault("blocks", [])
    if len(blocks) >= MAX_RICH_BLOCKS:
        raise ValueError("Rich Message حداکثر 500 بلوک دارد.")
    blocks.append(block)

def media_attachment_count(blocks):
    media_types = {"photo", "video", "animation", "audio", "voice_note", "document"}
    total = 0
    for block in blocks:
        if block.get("type") in media_types:
            total += 1
        elif block.get("type") in ("collage", "slideshow"):
            total += len(block.get("blocks", []))
    return total

def api(method, payload, base_url="https://api.telegram.org", timeout=30):
    if method in {"sendRichMessage", "sendRichMessageDraft"} or "rich_message" in payload:
        payload = sanitize_rich_blocks(payload)
    data = json.dumps(payload).encode()
    retry_safe = method in {"editMessageText", "sendRichMessageDraft"}
    attempts = 2 if retry_safe else 1

    for attempt in range(attempts):
        req = urllib.request.Request(
            f"{base_url.rstrip('/')}/bot{TOKEN}/{method}",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                result = json.loads(response.read())
            break
        except TimeoutError as error:
            if attempt + 1 < attempts:
                time.sleep(0.5)
                continue
            raise RuntimeError(
                f"مهلت پاسخ تلگرام برای {method} تمام شد. "
                "برای جلوگیری از ارسال تکراری، درخواست ارسال پیام خودکار تکرار نشد."
            ) from error
        except urllib.error.HTTPError as error:
            try:
                body = error.read().decode("utf-8", "replace")
                details = json.loads(body).get("description", body)
            except Exception:
                details = str(error)
            raise RuntimeError(f"Telegram API {error.code}: {details}") from error
        except (urllib.error.URLError, ConnectionError, OSError) as error:
            if attempt + 1 < attempts:
                time.sleep(0.5)
                continue
            reason = getattr(error, "reason", error)
            raise RuntimeError(
                f"ارتباط با سرور تلگرام قطع شد ({reason}). "
                "برای جلوگیری از ارسال تکراری، درخواست ارسال پیام خودکار تکرار نشد."
            ) from error

    if not result.get("ok"):
        raise RuntimeError(result.get("description", "Telegram error"))
    return result["result"]

def api_multipart(method, payload, files, base_url="https://api.telegram.org"):
    if method in {"sendRichMessage", "sendRichMessageDraft"} or "rich_message" in payload:
        payload = sanitize_rich_blocks(payload)
    boundary = "----TelegramJournal" + os.urandom(12).hex()
    chunks = []

    for key, value in payload.items():
        chunks.append(f"--{boundary}\r\n".encode())
        chunks.append(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        chunks.append(str(value).encode("utf-8"))
        chunks.append(b"\r\n")

    for field_name, filename, file_bytes, mimetype in files:
        chunks.append(f"--{boundary}\r\n".encode())
        chunks.append(
            f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'.encode()
        )
        chunks.append(f"Content-Type: {mimetype}\r\n\r\n".encode())
        chunks.append(file_bytes)
        chunks.append(b"\r\n")

    chunks.append(f"--{boundary}--\r\n".encode())
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/bot{TOKEN}/{method}",
        data=b"".join(chunks),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.loads(response.read())
    except urllib.error.HTTPError as error:
        try:
            body = error.read().decode("utf-8", "replace")
            details = json.loads(body).get("description", body)
        except Exception:
            details = str(error)
        raise RuntimeError(f"Telegram API {error.code}: {details}") from error
    except (urllib.error.URLError, ConnectionError, OSError, TimeoutError) as error:
        reason = getattr(error, "reason", error)
        raise RuntimeError(
            f"ارتباط با سرور تلگرام هنگام بارگذاری فایل قطع شد ({reason}). "
            "وضعیت ارسال فایل مشخص نیست؛ قبل از تلاش دوباره، نتیجه را بررسی کن."
        ) from error
    if not result.get("ok"):
        raise RuntimeError(result.get("description", "Telegram error"))
    return result["result"]


async def api_async(*args, **kwargs):
    return await asyncio.to_thread(api, *args, **kwargs)


async def api_multipart_async(*args, **kwargs):
    return await asyncio.to_thread(api_multipart, *args, **kwargs)

def menu(expanded=False, owner=False):
    def button(text, key, style=None):
        kwargs = {}
        if style:
            kwargs["api_kwargs"] = {"style": style}
        return InlineKeyboardButton(text, callback_data=f"add:{key}", **kwargs)

    keyboard = [
        [button("📎 فایل", "media", "primary"), button("📝 متن", "p", "primary")],
        [button("📋 لیست", "lists", "primary"), button("🔠 تیتر", "h", "primary"), button("📂 تاشو", "d", "primary")],
    ]
    if expanded:
        keyboard += [
            [button("︿ کمتر ︿", "less")],
            [button("🔘 دکمه‌ها", "btn"), button("📊 جدول", "t")],
            [button("🧮 فرمول", "m"), button("🔗 لینک داخلی", "a")],
            [button("📚 منبع", "src"), button("💻 کد", "c")],
            [button("➖ جداکننده", "div")],
            [button("🦶 پابرگ", "f"), button("🗺 نقشه", "map")],
        ]
    else:
        keyboard += [[button("﹀ بیشتر ﹀", "more")]]
    keyboard += [
        [
            InlineKeyboardButton("👀 پیش‌نمایش", callback_data="preview", api_kwargs={"style": "primary"}),
            InlineKeyboardButton("🗑 حذف آخرین", callback_data="clear", api_kwargs={"style": "danger"}),
        ],
        [InlineKeyboardButton("🚀 انتشار پیام", callback_data="send", api_kwargs={"style": "success"})],
        [InlineKeyboardButton("🏠 منوی اصلی", callback_data="main:home")],
    ]
    return InlineKeyboardMarkup(keyboard)
def button_type_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 باز کردن لینک", callback_data="btnwiz:url")],
        [InlineKeyboardButton("🎨 ظاهر دکمه", callback_data="btnwiz:style")],
        [InlineKeyboardButton("🚫 دکمه غیرفعال", callback_data="btnwiz:disabled")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ])


def button_style_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔵 آبی", callback_data="btnwizstyle:primary")],
        [InlineKeyboardButton("🟢 سبز", callback_data="btnwizstyle:success")],
        [InlineKeyboardButton("🔴 قرمز", callback_data="btnwizstyle:danger")],
        [InlineKeyboardButton("↩️ بازگشت", callback_data="btnwiz:back_type")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ])

def button_wizard_text(buttons):
    return (
        "🔘 ساخت دکمه‌ها\n\n"
        f"دکمه‌های ساخته‌شده: {len(buttons)} از 8\n"
        "برای افزودن دکمه بعدی یا پایان ساخت، انتخاب کن."
    )

def button_wizard_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ افزودن دکمه بعدی", callback_data="btnwiz:add")],
        [InlineKeyboardButton("✅ پایان ساخت", callback_data="btnwiz:done")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ])

def button_preview_text(button):
    names = {
        "url": "باز کردن لینک",
        "disabled": "غیرفعال",
        "web_app": "باز کردن وب اپ",
        "login_url": "ورود با تلگرام",
        "copy_text": "کپی متن",
        "callback_data": "اجرای دستور ربات",
        "switch_inline_query": "جستجوی درون خطی",
        "switch_inline_query_current_chat": "جستجو در همین گفتگو",
        "switch_inline_query_chosen_chat": "انتخاب گفتگو برای جستجو",
    }
    kind = next((key for key in names if key in button), "نامشخص")
    value = ""
    if kind == "url":
        value = button.get("url", "")
    elif kind in ("web_app", "login_url"):
        value = (button.get(kind) or {}).get("url", "")
    elif kind == "copy_text":
        value = (button.get(kind) or {}).get("text", "")
    elif kind == "callback_data":
        value = button.get("callback_data", "")
    elif kind.startswith("switch_inline_query"):
        item = button.get(kind)
        value = item if isinstance(item, str) else (item or {}).get("query", "")
    style_names = {"primary": "آبی", "success": "سبز", "danger": "قرمز"}
    return (
        "👀 پیش‌نمایش دکمه\n\n"
        f"متن دکمه: {button.get('text', 'بدون عنوان')}\n"
        f"عملکرد: {names.get(kind, 'نامشخص')}"
        + (f"\nمقدار: {value}" if value else "")
        + f"\nظاهر دکمه: {style_names.get(button.get('style', 'primary'), button.get('style', 'primary'))}"
        + "\n\nاگه درسته ثبتش کن"
    )

def button_preview_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ تایید و ثبت", callback_data="btnwiz:confirm")],
        [InlineKeyboardButton("↩️ انتخاب عملکرد دیگر", callback_data="btnwiz:discard")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ])

def media_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📎 فایل / مدیای تکی", callback_data="media:single")],
        [InlineKeyboardButton("🧩 کلاژ", callback_data="media:collage"),
         InlineKeyboardButton("🎞 اسلایدشو", callback_data="media:slideshow")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ])

def main_menu(owner=False, request_access=False, user_id=None):
    rows = [[
        InlineKeyboardButton("ریچ مسیج", callback_data="main:rich", api_kwargs={"style": "primary"}),
        InlineKeyboardButton("ویجت", callback_data="main:widget", api_kwargs={"style": "primary"}),
    ]]
    if owner:
        rows.append([InlineKeyboardButton("⚙️ مدیریت سرویس های هوش مصنوعی", callback_data="main:ai_admin")])
        rows.append([InlineKeyboardButton("🔌 اتصال ها", callback_data="conn:menu")])
        rows.append([InlineKeyboardButton("🔐 مدیریت دسترسی", callback_data="access:menu")])
    return InlineKeyboardMarkup(rows)


def rich_creation_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ ساخت دستی", callback_data="rich:manual")],
        [InlineKeyboardButton("🧠 ساخت با هوش مصنوعی", callback_data="rich:ai")],
        [InlineKeyboardButton("↩️ برگشت", callback_data="rich:back")],
    ])


def widget_creation_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🧠 ساخت با هوش مصنوعی", callback_data="widget:ai")],
        [InlineKeyboardButton("✍️ ساخت دستی", callback_data="widget:manual")],
        [InlineKeyboardButton("↩️ برگشت", callback_data="widget:back")],
    ])

def home_text(owner=False):
    text = "یکی از بخش های ربات رو انتخاب کن:"
    return text + (f"\n\nنسخه فعلی: {VERSION}" if owner else "")

def connector_menu(config):
    connectors = config.get("connectors", {})
    rows = [[InlineKeyboardButton("➕ افزودن اتصال", callback_data="conn:add")]]
    for connector_id, item in list(connectors.items())[:20]:
        rows.append([InlineKeyboardButton(f"🔗 {(item.get('name') or 'اتصال')[:32]}", callback_data=f"conn:item:{connector_id}")])
    rows.append([InlineKeyboardButton("↩️ منوی اصلی", callback_data="conn:back")])
    return InlineKeyboardMarkup(rows)

def connector_overview(config):
    connectors = config.get("connectors", {})
    if not connectors:
        return "🔌 هنوز اتصالی ثبت نشده"
    lines = ["🔌 اتصال های ثبت شده:"]
    for item in list(connectors.values())[:20]:
        lines.append(f"• {item.get('name', 'بدون نام')} | {item.get('type', 'custom')} | {item.get('url', '')}")
    return "\n".join(lines)

def connector_item_menu(connector_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🧪 آزمایش اتصال", callback_data=f"conn:test:{connector_id}")],
        [InlineKeyboardButton("🧰 فهرست ابزارهای MCP", callback_data=f"conn:tools:{connector_id}")],
        [InlineKeyboardButton("🔑 ثبت یا تغییر کلید", callback_data=f"conn:key:{connector_id}")],
        [InlineKeyboardButton("🗑 حذف اتصال", callback_data=f"conn:delete:{connector_id}")],
        [InlineKeyboardButton("↩️ برگشت", callback_data="conn:menu")],
    ])

class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

def validate_connector_target(url):
    from urllib.parse import urlsplit
    parsed = urlsplit((url or "").strip())
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("فقط آدرس HTTPS معتبر مجاز است")
    if parsed.port not in (None, 443):
        raise ValueError("فعلا فقط پورت استاندارد 443 مجاز است")
    try:
        addresses = socket.getaddrinfo(parsed.hostname, 443, type=socket.SOCK_STREAM)
    except OSError:
        raise ValueError("دامنه به IP تبدیل نشد") from None
    if not addresses:
        raise ValueError("IP مقصد پیدا نشد")
    public_ip = None
    for item in addresses:
        ip = ipaddress.ip_address(item[4][0])
        if not ip.is_global:
            raise ValueError("آدرس های شبکه داخلی یا رزرو شده مجاز نیستند")
        if public_ip is None:
            public_ip = str(ip)
    return parsed, public_ip

class PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, hostname, pinned_ip, timeout=10):
        super().__init__(hostname, port=443, timeout=timeout, context=ssl.create_default_context())
        self.pinned_ip = pinned_ip

    def connect(self):
        raw_socket = socket.create_connection((self.pinned_ip, self.port), self.timeout)
        self.sock = self._context.wrap_socket(raw_socket, server_hostname=self.host)

def test_connector_endpoint(connector):
    parsed, pinned_ip = validate_connector_target(connector.get("url"))
    headers = {"User-Agent": "TelegramJournal-ConnectorCheck/1.0", "Accept": "application/json", "Host": parsed.hostname}
    if connector.get("api_key"):
        headers["Authorization"] = "Bearer " + connector["api_key"]
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    connection = PinnedHTTPSConnection(parsed.hostname, pinned_ip, timeout=10)
    try:
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        status = response.status
        response.read(1024)
    except (OSError, http.client.HTTPException, TimeoutError):
        raise RuntimeError("ارتباط با endpoint برقرار نشد") from None
    finally:
        connection.close()
    if status in (401, 403):
        return False, f"سرور پاسخ داد ولی احراز هویت رد شد (HTTP {status})"
    if status in (301, 302, 303, 307, 308):
        return False, "سرور تغییر مسیر داد. برای امنیت تغییر مسیر خودکار دنبال نمیشه"
    if status == 405:
        return True, "سرور در دسترسه ولی GET پشتیبانی نمیشه. دست دادن پروتکل هنوز بررسی نشده"
    if 200 <= status < 500:
        return True, f"سرور پاسخ داد (HTTP {status})"
    return False, f"سرور خطا داد (HTTP {status})"

def mcp_response_payload(body, content_type):
    raw = body.decode("utf-8", errors="replace")
    if "text/event-stream" in (content_type or "").lower():
        data_lines = [line[5:].strip() for line in raw.splitlines() if line.startswith("data:")]
        for data in reversed(data_lines):
            if data and data != "[DONE]":
                return json.loads(data)
        raise RuntimeError("پاسخ MCP در قالب SSE معتبر نبود")
    if not raw.strip():
        return None
    return json.loads(raw)

def mcp_header_value(value):
    value = str(value)
    if value and all(0x21 <= ord(char) <= 0x7E for char in value) and not (
        value.startswith("=?base64?") and value.endswith("?=")
    ):
        return value
    encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
    return "=?base64?" + encoded + "?="

def mcp_request_meta():
    return {
        "io.modelcontextprotocol/protocolVersion": MCP_MODERN_VERSION,
        "io.modelcontextprotocol/clientInfo": {"name": "TelegramJournal", "version": VERSION},
        "io.modelcontextprotocol/clientCapabilities": {},
    }

def mcp_http_post(connector, payload, session_id=None, allow_empty=False, protocol_version=None, extra_headers=None):
    parsed, pinned_ip = validate_connector_target(connector.get("url"))
    headers = {
        "Host": parsed.hostname,
        "User-Agent": "TelegramJournal-MCP/1.0",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if protocol_version:
        headers["MCP-Protocol-Version"] = protocol_version
        if protocol_version == MCP_MODERN_VERSION:
            method = payload.get("method")
            if method:
                headers["Mcp-Method"] = method
            params = payload.get("params") or {}
            if method == "tools/call" and params.get("name"):
                headers["Mcp-Name"] = mcp_header_value(params["name"])
    if connector.get("api_key"):
        headers["Authorization"] = "Bearer " + connector["api_key"]
    if session_id:
        headers["MCP-Session-Id"] = session_id
    for header_name, header_value in (extra_headers or {}).items():
        headers[header_name] = header_value
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    connection = PinnedHTTPSConnection(parsed.hostname, pinned_ip, timeout=15)
    try:
        connection.request("POST", path, body=json.dumps(payload, separators=(",", ":")).encode("utf-8"), headers=headers)
        response = connection.getresponse()
        status = response.status
        content_type = response.getheader("Content-Type", "")
        new_session_id = response.getheader("Mcp-Session-Id")
        body = response.read(1024 * 1024 + 1)
    except (OSError, http.client.HTTPException, TimeoutError):
        raise RuntimeError("ارتباط HTTPS با سرور MCP برقرار نشد") from None
    finally:
        connection.close()
    if len(body) > 1024 * 1024:
        raise RuntimeError("پاسخ MCP بیش از 1 مگابایت بود")
    if status in (301, 302, 303, 307, 308):
        raise RuntimeError("سرور MCP تغییر مسیر داد و برای امنیت دنبال نشد")
    if status in (401, 403):
        raise RuntimeError(f"احراز هویت MCP رد شد (HTTP {status})")
    if status not in (200, 202):
        raise RuntimeError(f"سرور MCP پاسخ نامعتبر داد (HTTP {status})")
    if status == 202 or (allow_empty and not body.strip()):
        return None, new_session_id
    return mcp_response_payload(body, content_type), new_session_id

def mcp_connect(connector):
    modern_request = {
        "jsonrpc": "2.0",
        "id": "discover-1",
        "method": "server/discover",
        "params": {"_meta": mcp_request_meta()},
    }
    try:
        discovery, _ = mcp_http_post(
            connector, modern_request, protocol_version=MCP_MODERN_VERSION
        )
        supported = (discovery or {}).get("result", {}).get("supportedVersions", [])
        if MCP_MODERN_VERSION in supported:
            return "modern", MCP_MODERN_VERSION, None
    except Exception:
        pass

    initialize = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": MCP_LEGACY_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "TelegramJournal", "version": VERSION},
        },
    }
    result, session_id = mcp_http_post(connector, initialize)
    if not isinstance(result, dict) or result.get("error"):
        detail = (result or {}).get("error", {}) if isinstance(result, dict) else {}
        raise RuntimeError("راه اندازی MCP ناموفق بود: " + str(detail.get("message", "پاسخ نامعتبر")))
    if not isinstance(result.get("result"), dict):
        raise RuntimeError("سرور MCP نتیجه initialize معتبر برنگردوند")
    protocol_version = result["result"].get("protocolVersion") or MCP_LEGACY_VERSION
    return "legacy", protocol_version, session_id

def connect_mcp_and_list_tools(connector):
    era, protocol_version, session_id = mcp_connect(connector)
    if era == "modern":
        request = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {"_meta": mcp_request_meta()},
        }
        tools_result, _ = mcp_http_post(
            connector, request, protocol_version=protocol_version
        )
    else:
        mcp_http_post(
            connector,
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            session_id,
            allow_empty=True,
            protocol_version=protocol_version,
        )
        tools_result, _ = mcp_http_post(
            connector,
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            session_id,
            protocol_version=protocol_version,
        )
    if not isinstance(tools_result, dict) or tools_result.get("error"):
        detail = (tools_result or {}).get("error", {}) if isinstance(tools_result, dict) else {}
        raise RuntimeError("دریافت فهرست ابزارها ناموفق بود: " + str(detail.get("message", "پاسخ نامعتبر")))
    result = tools_result.get("result") or {}
    if result.get("resultType") == "input_required":
        raise RuntimeError("سرور برای فهرست ابزارها ورودی تعاملی درخواست کرد که هنوز پشتیبانی نمیشه")
    tools = result.get("tools", [])
    if not isinstance(tools, list):
        raise RuntimeError("فهرست ابزارهای MCP معتبر نیست")
    return tools

def connector_tools_menu(connector_id, tools):
    rows = []
    for index, tool in enumerate(tools[:20]):
        name = str(tool.get("name", "ابزار") if isinstance(tool, dict) else tool)[:42]
        rows.append([InlineKeyboardButton(f"▶️ {name}", callback_data=f"conn:tool:{connector_id}:{index}")])
    rows.append([InlineKeyboardButton("↩️ برگشت", callback_data=f"conn:item:{connector_id}")])
    return InlineKeyboardMarkup(rows)

def mcp_tool_parameter_headers(schema, arguments):
    headers = {}
    seen = set()
    reserved = {
        "host", "authorization", "content-type", "accept",
        "mcp-protocol-version", "mcp-method", "mcp-name", "mcp-session-id",
    }

    def walk(current_schema, current_args):
        if not isinstance(current_schema, dict) or not isinstance(current_args, dict):
            return
        properties = current_schema.get("properties", {})
        if not isinstance(properties, dict):
            return
        for property_name, property_schema in properties.items():
            if not isinstance(property_schema, dict):
                continue
            if property_name not in current_args:
                continue
            value = current_args[property_name]
            if value is None:
                continue
            header_suffix = property_schema.get("x-mcp-header")
            if header_suffix is not None:
                if not isinstance(header_suffix, str) or not re.fullmatch(r"[A-Za-z0-9!#$%&'*+.^_`|~-]+", header_suffix):
                    raise ValueError("نام x-mcp-header معتبر نیست")
                lowered = header_suffix.lower()
                if lowered in seen or ("mcp-param-" + lowered) in reserved:
                    raise ValueError("نام x-mcp-header تکراری یا رزرو شده است")
                seen.add(lowered)
                value_type = property_schema.get("type")
                if value_type == "string" and isinstance(value, str):
                    rendered = value
                elif value_type == "integer" and isinstance(value, int) and not isinstance(value, bool):
                    rendered = str(value)
                elif value_type == "boolean" and isinstance(value, bool):
                    rendered = "true" if value else "false"
                else:
                    raise ValueError("مقدار پارامتر x-mcp-header با نوع schema سازگار نیست")
                headers["Mcp-Param-" + header_suffix] = mcp_header_value(rendered)
            if isinstance(value, dict):
                walk(property_schema, value)

    walk(schema or {}, arguments or {})
    return headers

def call_mcp_tool(connector, tool_name, arguments, input_schema=None):
    era, protocol_version, session_id = mcp_connect(connector)
    if era == "modern":
        params = {
            "_meta": mcp_request_meta(),
            "name": tool_name,
            "arguments": arguments,
        }
        request = {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": params}
        custom_headers = mcp_tool_parameter_headers(input_schema or {}, arguments)
        response, _ = mcp_http_post(
            connector, request, protocol_version=protocol_version, extra_headers=custom_headers
        )
    else:
        mcp_http_post(
            connector,
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            session_id,
            allow_empty=True,
            protocol_version=protocol_version,
        )
        response, _ = mcp_http_post(
            connector,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": arguments},
            },
            session_id,
            protocol_version=protocol_version,
        )
    if not isinstance(response, dict) or response.get("error"):
        detail = (response or {}).get("error", {}) if isinstance(response, dict) else {}
        raise RuntimeError("اجرای ابزار MCP ناموفق بود: " + str(detail.get("message", "پاسخ نامعتبر")))
    result = response.get("result") or {}
    if result.get("resultType") == "input_required":
        raise RuntimeError("این ابزار برای ادامه به ورودی تعاملی نیاز داره که هنوز پشتیبانی نمیشه")
    if result.get("isError"):
        raise RuntimeError("ابزار MCP خطا برگردوند")
    content = result.get("content", [])
    output = []
    for item in content[:20] if isinstance(content, list) else []:
        if not isinstance(item, dict):
            output.append(str(item)[:1500])
        elif item.get("type") == "text":
            output.append(str(item.get("text", "")))
        else:
            output.append(json.dumps(item, ensure_ascii=False)[:1500])
    rendered = "\n".join(output) if output else json.dumps(result, ensure_ascii=False)
    return rendered[:3500]

def cancel_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ لغو", callback_data="cancel")]])

def list_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 لیست ساده", callback_data="list:l")],
        [InlineKeyboardButton("☑️ چک‌لیست", callback_data="list:x"),
         InlineKeyboardButton("🔢 لیست شماره‌ای", callback_data="list:ol")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ])

def heading_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1", callback_data="heading:1"),
         InlineKeyboardButton("2", callback_data="heading:2"),
         InlineKeyboardButton("3", callback_data="heading:3"),
         InlineKeyboardButton("4", callback_data="heading:4"),
         InlineKeyboardButton("5", callback_data="heading:5"),
         InlineKeyboardButton("6", callback_data="heading:6")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ])

def collection_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ افزودن مدیای دیگر", callback_data="collection:more"),
         InlineKeyboardButton("✅ پایان", callback_data="collection:finish")],
        [InlineKeyboardButton("❌ لغو", callback_data="collection:cancel")],
    ])

def parts_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 پایان و ثبت متن قالب‌دار", callback_data="parts:finish_text")],
        [InlineKeyboardButton("💻 پایان و ثبت به‌عنوان کد", callback_data="parts:finish_code")],
        [InlineKeyboardButton("❌ لغو", callback_data="parts:cancel")],
    ])

def editor_text(blocks, ai_draft=False):
    names = {
        "paragraph": "📝 متن", "heading": "🔠 تیتر", "pre": "💻 کد",
        "list": "📋 لیست", "blockquote": "💬 نقل‌قول",
        "expandable_blockquote": "📢 نقل‌قول بازشونده", "pullquote": "🎯 نقل‌قول وسط",
        "footer": "🦶 فوتر", "divider": "➖ جداکننده",
        "mathematical_expression": "🧮 فرمول", "details": "📂 تاشو", "table": "📊 جدول", "map": "🗺 نقشه",
        "buttons": "🔘 دکمه‌ها", "photo": "🖼 عکس", "video": "🎬 ویدیو",
        "animation": "🎞 انیمیشن", "audio": "🎵 صدا", "voice_note": "🎤 ویس",
        "document": "📄 فایل", "collage": "🧩 کلاژ", "slideshow": "🎞 اسلایدشو",
    }
    if not blocks:
        return "✨ پیام جدیدتو بساز\nاز دکمه های پایین هر تعداد قابلیتی که میخای اضافه کن."
    lines = [f"🧱 تعداد بلوک‌ها: {len(blocks)}", ""]
    for i, block in enumerate(blocks, 1):
        label = names.get(block.get("type"), block.get("type", "?"))
        lines.append(f"{i}. {label}")
    if ai_draft:
        lines += [
            "",
            "✨ پیش نویس اولیه آماده شد",
            "دیگه چی میخای اضافه کنی؟ عکس، ویدیو، فایل، دکمه یا هر قابلیت دیگه رو از دکمه های پایین انتخاب کن",
            "وقتی کارت تموم شد روی «🚀 انتشار پیام» بزن",
        ]
    else:
        lines += ["", "➕ هر تعداد قابلیت رو میتونی تو همین پیام ترکیب کنی."]
    return "\n".join(lines)

def editor_rich_message(blocks, ai_draft=False):
    return {
        "blocks": [
            {"type": "paragraph", "text": editor_text(blocks, ai_draft)},
        ],
        "is_rtl": True,
    }

async def send_editor_reply(message, context):
    ai_draft = bool(context.user_data.get("ai_rich_draft"))
    if context.user_data.get("control_widget"):
        await control_widget_set_screen(
            context,
            editor_text(context.user_data.get("blocks", []), ai_draft=ai_draft),
            menu(context.user_data.get("menu_expanded", False), is_owner(message.from_user)),
        )
        return
    await api_async("sendRichMessage", {
        "chat_id": message.chat_id,
        "rich_message": editor_rich_message(context.user_data.get("blocks", []), ai_draft=ai_draft),
        "reply_markup": menu(context.user_data.get("menu_expanded", False), is_owner(message.from_user)).to_dict(),
    })

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("control_widget", None)
    if context.args:
        payload = context.args[0]
        token = payload[7:] if payload.startswith("widget_") else payload
        index = context.application.bot_data.get("widget_index") or empty_widget_index()
        if token in index.get("widgets", {}):
            await send_shared_widget_token(update.effective_message, context, token)
            return

    context.user_data["blocks"] = []
    context.user_data.pop("pending", None)
    context.user_data.pop("collection", None)
    context.user_data.pop("widget_mode", None)
    context.user_data.pop("ai_rich_draft", None)
    context.user_data["menu_expanded"] = False
    owner = is_owner(update.effective_user)
    authorized = is_authorized(context, update.effective_user)
    await update.effective_message.reply_text(
        home_text(owner),
        reply_markup=main_menu(
            owner,
            request_access=not authorized,
            user_id=update.effective_user.id,
        ),
    )

async def open_rich_editor(query, context):
    context.user_data["blocks"] = []
    context.user_data.pop("pending", None)
    context.user_data.pop("collection", None)
    context.user_data.pop("widget_mode", None)
    context.user_data.pop("ai_rich_draft", None)
    context.user_data["menu_expanded"] = False
    if context.user_data.get("control_widget"):
        await control_widget_set_screen(
            context,
            editor_text([]),
            menu(False, is_owner(query.from_user)),
        )
        return
    try:
        await api_async("sendRichMessage", {
            "chat_id": query.message.chat_id,
            "rich_message": {
                "blocks": [
                    {"type": "paragraph", "text": editor_text([])},
                ],
                "is_rtl": True,
            },
            "reply_markup": menu(False, is_owner(query.from_user)).to_dict(),
        })
    except Exception:
        await query.message.reply_text(
            editor_text([]),
            reply_markup=menu(False, is_owner(query.from_user)),
        )

async def show_editor(query, context):
    expanded = context.user_data.get("menu_expanded", False)
    ai_draft = bool(context.user_data.get("ai_rich_draft"))
    if context.user_data.get("control_widget"):
        await control_widget_set_screen(
            context,
            editor_text(context.user_data.get("blocks", []), ai_draft=ai_draft),
            menu(expanded, is_owner(query.from_user)),
        )
        return
    await api_async("editMessageText", {
        "chat_id": query.message.chat_id,
        "message_id": query.message.message_id,
        "rich_message": editor_rich_message(context.user_data.get("blocks", []), ai_draft=ai_draft),
        "reply_markup": menu(expanded, is_owner(query.from_user)).to_dict(),
    })

def prompt_text(key):
    prompts = {
        "p": "📝 <b>افزودن متن به پیام</b>\n\nمتن یا محتوای موردنظرت رو بفرست یا فایل متنی ارسال کن.\n\n<b>قالب‌بندی با Markdown</b>\n<code>*متن بولد*</code>\n<code>_متن ایتالیک_</code>\n<code>__متن زیرخط‌دار__</code>\n<code>~متن خط‌خورده~</code>\n<code>==متن هایلایت شده==</code>\n<code>||متن مخفی||</code>\n<code>[عنوان لینک](https://example.com)</code>\n\nبرای نمایش کد از بک‌تیک و برای کد چندخطی از سه بک‌تیک استفاده کن.\n\n📄 فایل‌های TXT، MD و فایل‌های متنی UTF-8 هم قابل ارسال هستن.\n\n🧩 برای متن طولانی هم میتونی از گزینه متن چندبخشی استفاده کنی.",
        "h": "🔠 <b>تیتر</b>\n\nاول اندازه تیتر را از دکمه‌های 1 تا 6 انتخاب کن.\nبعد متن تیتر را بفرست.\n\n👀 با انتخاب هر شماره، متن نمایشی با همان اندازه نشان داده می‌شود.",
        "c": "💻 <b>کد</b> را بفرست.\nخط اول = زبان برنامه‌نویسی\nاز خط دوم = کد\n\nمثال:\n<code>python\nprint(&quot;Hello&quot;)</code>",
        "l": "📋 <b>لیست</b> را بفرست؛ هر مورد در یک خط.\n\nمثال:\n<code>سیب\nموز\nپرتقال</code>",
        "x": "☑️ <b>چک‌لیست</b> را بفرست؛ هر مورد در یک خط.\n\nمثال:\n<code>خرید نان\nتمرین امروز\nارسال پروژه</code>\n\n💡 فقط موارد را بنویس؛ لازم نیست [x] بگذاری.",
        "ol": "🔢 <b>لیست شماره‌ای</b> را بفرست؛ هر مورد در یک خط.\n\nمثال:\n<code>مرحله اول\nمرحله دوم\nمرحله سوم</code>",
        "pq": "🎯 <b>نقل‌قول وسط</b> را بفرست.\nخط اول = متن\nخط دوم = منبع اختیاری\n\nمثال:\n<code>یک جمله مهم وسط پیام\nمنبع: کتاب</code>",
        "f": "🦶 <b>فوتر</b> را بفرست.\n\nمثال:\n<code>پایان پیام — Rich Message Builder</code>",
        "div": "➖ جداکننده بدون ورودی اضافه می‌شود.",
        "m": "🧮 <b>فرمول ریاضی</b> را با LaTeX بفرست.\n\nمثال:\n<code>E = mc^2</code>",
        "a": "🔗 <b>لینک داخلی</b>\n\nیک متن یا تیتر را به‌عنوان مقصد انتخاب کن.\nبعد فقط متن لینک را بفرست.\n\nمثال: <code>مشاهده مشخصات</code>\n\n💡 لینک بدون اضافه‌کردن بلوک یا متن مخفی، مستقیم به مقصد می‌رود.",
        "d": "📂 <b>تاشو</b> را در دو بخش بفرست:\nخط اول = عنوان\nخط دوم به بعد = محتوا\n\nمثال:\n<code>جزئیات بیشتر\nاینجا توضیحات کامل قرار می‌گیرد.</code>",
        "t": "📊 <b>جدول</b> را بساز.\nهر ردیف یک خط و سلول‌ها با <code>|</code> جدا شوند.\nخط اول Header است.\n\nمثال:\n<code>نام | امتیاز\nعلی | 95\nسارا | 88</code>",
        "map": "🗺 <b>نقشه</b> را بفرست.\nفرمت: latitude,longitude,zoom\n\nمثال:\n<code>35.6892,51.3890,14</code>",
        "photo": "🖼 <b>عکس</b> را همین‌جا از گالری بفرست.\n\nمثال: 📸 یک عکس از گالری انتخاب کن و ارسال کن.\nربات خودش file_id را می‌گیرد؛ لازم نیست لینک یا file_id وارد کنی.",
        "video": "🎬 <b>ویدیو</b> را همین‌جا بفرست.\n\nمثال: 🎥 یک ویدیوی کوتاه از گالری انتخاب کن و ارسال کن.",
        "animation": "🎞 <b>GIF / Animation</b> را همین‌جا بفرست.\n\nمثال: 🎞 یک GIF ارسال کن.",
        "audio": "🎵 <b>فایل صوتی</b> را همین‌جا بفرست.\n\nمثال: 🎵 یک آهنگ یا فایل صوتی ارسال کن.",
        "voice": "🎤 <b>ویس</b> را همین‌جا بفرست.\n\nمثال: 🎤 یک Voice Message ضبط و ارسال کن.",
        "doc": "📄 <b>فایل</b> را همین‌جا بفرست.\n\nمثال: 📄 یک PDF یا فایل معمولی ارسال کن.",
        "collage": "🧩 <b>Collage</b> را بساز.\n\nمثال: 📸 عکس اول را بفرست، بعد عکس دوم و سوم را هم بفرست.\nبعد روی «✅ پایان» بزن تا همه داخل یک Collage قرار بگیرند.",
        "slideshow": "🎞 <b>Slideshow</b> را بساز.\n\nمثال: 📸 چند عکس یا 🎬 چند ویدیو را یکی‌یکی بفرست.\nبعد روی «✅ پایان» بزن تا همه داخل یک Slideshow قرار بگیرند.",
    }
    return prompts.get(key, "محتوا را بفرست.")

async def choose_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    key = query.data.split(":", 1)[1]

    if key == "more":
        context.user_data["menu_expanded"] = True
        await show_editor(query, context)
        return

    if key == "less":
        context.user_data["menu_expanded"] = False
        await show_editor(query, context)
        return

    if key == "btn":
        context.user_data.pop("pending", None)
        context.user_data["btn_draft"] = []
        context.user_data["btn_wizard_style"] = "primary"
        context.user_data["btn_wizard"] = "label"
        await query.edit_message_text(
            "🔘 ساخت دکمه\n\nاول متن دکمه رو بفرست",
            reply_markup=cancel_menu(),
        )
        return

    if key == "media":
        context.user_data.pop("pending", None)
        await query.edit_message_text(
            "📎 <b>فایل / مدیا</b>\n\nنوع مدیا را انتخاب کن:",
            parse_mode="HTML",
            reply_markup=media_menu(),
        )
        return

    if key == "parts":
        context.user_data["pending"] = "parts"
        context.user_data["multipart"] = {"chunks": [], "characters": 0}
        await query.edit_message_text(
            "🧩 <b>ورود متن چندبخشی</b>\n\n"
            "قطعه‌های متن یا کدت رو به ترتیب و در چند پیام بفرست\n"
            "هر پیام جداگانه ذخیره میشه و تا وقتی «پایان» نزنی چیزی به پیام اصلی اضافه نمیشه\n\n"
            "برای اتصال دقیق قطعه‌های کد، آخر کار گزینه «ثبت به‌عنوان کد» رو بزن\n"
            "هیچ فاصله یا خط جدیدی خودکار بین قطعه‌ها اضافه نمیشه",
            parse_mode="HTML",
            reply_markup=parts_menu(),
        )
        return

    if key == "a":
        blocks = context.user_data.setdefault("blocks", [])
        text_blocks = [(i, b) for i, b in enumerate(blocks)
                       if b.get("type") in ("paragraph", "heading") and isinstance(b.get("text"), (str, list, dict))]
        if not text_blocks:
            await query.answer("اول یک متن یا تیتر بساز تا مقصد لینک مشخص شود.", show_alert=True)
            return
        rows = []
        for visible_index, (_, block) in enumerate(text_blocks):
            label = rich_text_preview(block.get("text")) or "متن بدون عنوان"
            if len(label) > 45:
                label = label[:45] + "…"
            rows.append([InlineKeyboardButton(f"{visible_index + 1}. {label}", callback_data=f"anchor_target:{visible_index}")])
        rows.append([InlineKeyboardButton("❌ لغو", callback_data="cancel")])
        context.user_data["anchor_visible"] = [i for i, _ in text_blocks]
        await query.edit_message_text(
            "🔗 <b>مقصد لینک داخلی</b>\n\nبلوک حاوی عبارت مقصد را انتخاب کن. بعد خودِ عبارت را مشخص می‌کنی.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    context.user_data["pending"] = key

    if key == "h":
        await query.edit_message_text(
            prompt_text("h"),
            parse_mode="HTML",
            reply_markup=heading_menu(),
        )
        return

    if key == "lists":
        await query.edit_message_text(
            "📋 <b>انتخاب نوع لیست</b>\n\nیکی از سه نوع لیست را انتخاب کن:",
            parse_mode="HTML",
            reply_markup=list_menu(),
        )
        return

    if key == "div":
        add_block(context, {"type": "divider"})
        context.user_data.pop("pending", None)
        await show_editor(query, context)
        return

    if key in ("collage", "slideshow"):
        context.user_data["collection"] = {"type": key, "blocks": []}
        await query.edit_message_text(prompt_text(key), parse_mode="HTML", reply_markup=collection_menu())
        return

    if key == "p":
        text_keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🧩 متن چندبخشی", callback_data="add:parts", api_kwargs={"style": "primary"}),
            InlineKeyboardButton("❌ لغو", callback_data="cancel", api_kwargs={"style": "danger"}),
        ]])
        await query.edit_message_text(
            prompt_text(key),
            parse_mode="HTML",
            reply_markup=text_keyboard,
        )
        return

    await query.edit_message_text(prompt_text(key), parse_mode="HTML", reply_markup=cancel_menu())


def rich_text_preview(node):
    if isinstance(node, str):
        return node.replace("\n", " ")
    if isinstance(node, list):
        return "".join(rich_text_preview(x) for x in node)
    if isinstance(node, dict):
        if node.get("type") == "anchor":
            return ""
        if "text" in node:
            return rich_text_preview(node["text"])
    return ""

def link_phrase_to_anchor(node, phrase, anchor_name):
    """Insert a zero-width RichTextAnchor immediately before the exact phrase."""
    if isinstance(node, str):
        index = node.find(phrase)
        if index < 0:
            return node, False
        parts = []
        if index:
            parts.append(node[:index])
        parts.append({"type": "anchor", "name": anchor_name})
        parts.append(phrase)
        if index + len(phrase) < len(node):
            parts.append(node[index + len(phrase):])
        return parts, True

    if isinstance(node, list):
        out = []
        found = False
        for item in node:
            if found:
                out.append(item)
                continue
            updated, item_found = link_phrase_to_anchor(item, phrase, anchor_name)
            if item_found:
                found = True
                if isinstance(updated, list):
                    out.extend(updated)
                else:
                    out.append(updated)
            else:
                out.append(item)
        return out, found

    if isinstance(node, dict) and "text" in node:
        updated, found = link_phrase_to_anchor(node["text"], phrase, anchor_name)
        if found:
            clean = dict(node)
            clean["text"] = updated
            return clean, True

    return node, False

def utf16_map(text):
    points = [0]
    total = 0
    for ch in text:
        total += 2 if ord(ch) > 0xFFFF else 1
        points.append(total)
    return points

def entity_span(text, entity, offset_base=0):
    points = utf16_map(text)
    start_u16 = entity.offset - offset_base
    end_u16 = start_u16 + entity.length
    try:
        start = points.index(start_u16)
        end = points.index(end_u16)
        return start, end
    except ValueError:
        return None, None

def entity_to_rich(text, entity, children):
    etype = entity.type
    if etype in ("bold", "italic", "underline", "strikethrough", "spoiler", "code",
                  "subscript", "superscript", "marked"):
        return {"type": etype, "text": children}
    if etype == "text_link":
        return {"type": "url", "text": children, "url": entity.url}
    if etype == "custom_emoji":
        return {
            "type": "custom_emoji",
            "custom_emoji_id": entity.custom_emoji_id,
            "alternative_text": text,
        }
    if etype == "url":
        return {"type": "url", "text": children, "url": text}
    if etype == "email":
        return {"type": "email_address", "text": children, "email_address": text}
    if etype == "phone_number":
        return {"type": "phone_number", "text": children, "phone_number": text}
    if etype == "text_mention":
        user = getattr(entity, "user", None)
        return {"type": "text_mention", "text": children, "user": user.to_dict() if user else {}}
    if etype == "date_time":
        return {
            "type": "date_time",
            "text": children,
            "unix_time": entity.unix_time,
            "date_time_format": entity.date_time_format,
        }
    if etype == "bank_card_number":
        return {"type": "bank_card_number", "text": children, "bank_card_number": text}
    if etype in ("mention", "hashtag", "cashtag", "bot_command"):
        return {"type": etype, "text": children, etype: text}
    return children

def rich_text_from_entities(text, entities, offset_base=0):
    if not entities:
        return text

    spans = []
    for entity in entities:
        start, end = entity_span(text, entity, offset_base)
        if start is not None and start < end:
            spans.append((start, end, entity))
    spans.sort(key=lambda x: (x[0], -x[1]))

    def render(start, end):
        boundaries = {start, end}
        relevant = []
        for es, ee, entity in spans:
            if es >= start and ee <= end:
                relevant.append((es, ee, entity))
                boundaries.add(es)
                boundaries.add(ee)
        boundaries = sorted(boundaries)
        out = []

        for i in range(len(boundaries) - 1):
            a, b = boundaries[i], boundaries[i + 1]
            covering = [(es, ee, e) for es, ee, e in relevant if es <= a and b <= ee]
            if not covering:
                out.append(text[a:b])
                continue
            es, ee, entity = max(covering, key=lambda x: (x[0], -x[1]))
            if es == a and ee == b:
                nested = [(x[0], x[1], x[2]) for x in covering if x[2] is not entity]
                old = spans[:]
                spans[:] = nested
                child = render(a, b)
                spans[:] = old
                out.append(entity_to_rich(text[a:b], entity, child))
            else:
                out.append(text[a:b])
        return out

    result = render(0, len(text))
    return result[0] if len(result) == 1 else result

def markdown_inline_to_rich(text):
    pattern = re.compile(
        r"\*\*(.+?)\*\*|__(.+?)__|~(.+?)~|\|\|(.+?)\|\||"
        r"`([^`\n]+)`|\[([^\]\n]+)\]\((https?://[^)\s]+)\)|"
        r"\*([^*\n]+)\*|_([^_\n]+)_|==([^=\n]+)==",
        re.S,
    )
    result = []
    cursor = 0
    for match in pattern.finditer(text):
        if match.start() > cursor:
            result.append(text[cursor:match.start()])
        groups = match.groups()
        if groups[0] is not None:
            result.append({"type": "bold", "text": markdown_inline_to_rich(groups[0])})
        elif groups[1] is not None:
            result.append({"type": "underline", "text": markdown_inline_to_rich(groups[1])})
        elif groups[2] is not None:
            result.append({"type": "strikethrough", "text": markdown_inline_to_rich(groups[2])})
        elif groups[3] is not None:
            result.append({"type": "spoiler", "text": markdown_inline_to_rich(groups[3])})
        elif groups[4] is not None:
            result.append({"type": "code", "text": groups[4]})
        elif groups[5] is not None:
            result.append({"type": "url", "text": markdown_inline_to_rich(groups[5]), "url": groups[6]})
        elif groups[7] is not None:
            result.append({"type": "bold", "text": markdown_inline_to_rich(groups[7])})
        elif groups[8] is not None:
            result.append({"type": "italic", "text": markdown_inline_to_rich(groups[8])})
        elif groups[9] is not None:
            result.append({"type": "marked", "text": markdown_inline_to_rich(groups[9])})
        cursor = match.end()
    if cursor < len(text):
        result.append(text[cursor:])
    if not result:
        return text
    return result[0] if len(result) == 1 else result


def markdown_text_to_blocks(text):
    blocks = []
    pattern = re.compile(r"```([A-Za-z0-9_+.-]*)\n(.*?)```", re.S)
    cursor = 0
    for match in pattern.finditer(text):
        before = text[cursor:match.start()]
        if before:
            blocks.append({"type": "paragraph", "text": markdown_inline_to_rich(before)})
        code = match.group(2)
        block = {"type": "pre", "text": code}
        if match.group(1):
            block["language"] = match.group(1)
        blocks.append(block)
        cursor = match.end()
    tail = text[cursor:]
    if tail or not blocks:
        blocks.append({"type": "paragraph", "text": markdown_inline_to_rich(tail)})
    return blocks


def text_to_rich_blocks(message):
    text = message.text or ""
    entities = list(message.entities or [])
    if not entities:
        return markdown_text_to_blocks(text)

    blocks = []
    block_entities = []
    inline_entities = []
    for entity in entities:
        if entity.type in ("blockquote", "expandable_blockquote", "pre"):
            start, end = entity_span(text, entity)
            if start is not None and start < end:
                block_entities.append((start, end, entity))
        else:
            inline_entities.append(entity)

    block_entities.sort(key=lambda x: x[0])
    cursor = 0

    def add_paragraph(start, end):
        if start >= end:
            return
        segment = text[start:end]
        seg_entities = []
        for e in inline_entities:
            es, ee = entity_span(text, e)
            if es is not None and es >= start and ee <= end:
                seg_entities.append(e)
        blocks.append({"type": "paragraph", "text": rich_text_from_entities(segment, seg_entities, start)})

    for start, end, entity in block_entities:
        add_paragraph(cursor, start)
        segment = text[start:end]
        seg_entities = []
        for e in inline_entities:
            es, ee = entity_span(text, e)
            if es is not None and es >= start and ee <= end:
                seg_entities.append(e)

        if entity.type == "pre":
            block = {"type": "pre", "text": rich_text_from_entities(segment, seg_entities, start)}
            language = getattr(entity, "language", None)
            if language:
                block["language"] = language
        elif entity.type == "expandable_blockquote":
            block = {
                "type": "expandable_blockquote",
                "text": rich_text_from_entities(segment, seg_entities, start)
            }
        else:
            block = {
                "type": entity.type,
                "blocks": [{"type": "paragraph", "text": rich_text_from_entities(segment, seg_entities, start)}]
            }
        blocks.append(block)
        cursor = end

    add_paragraph(cursor, len(text))
    return blocks

def checklist_text(block):
    lines = ["☑️ <b>چک‌لیست</b>", "", "برای تیک‌زدن هر مورد، دکمه همان مورد را بزن.", ""]
    for i, item in enumerate(block.get("items", []), 1):
        checked = "☑️" if item.get("is_checked") else "⬜"
        text = item.get("blocks", [{}])[0].get("text", "")
        lines.append(f"{checked} {i}. {text}")
    lines += ["", "وقتی تمام شد، «✅ ثبت چک‌لیست» را بزن."]
    return "\n".join(lines)

def checklist_menu(block):
    rows = []
    for i, item in enumerate(block.get("items", [])):
        checked = "☑️" if item.get("is_checked") else "⬜"
        text = item.get("blocks", [{}])[0].get("text", "")
        rows.append([InlineKeyboardButton(f"{checked} {i + 1}. {text}"[:60], callback_data=f"checklist:toggle:{i}")])
    rows.append([InlineKeyboardButton("✅ ثبت چک‌لیست", callback_data="checklist:done")])
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="checklist:cancel")])
    return InlineKeyboardMarkup(rows)

def input_media(kind, file_id):
    mapping = {
        "photo": ("photo", "photo"), "video": ("video", "video"),
        "animation": ("animation", "animation"), "audio": ("audio", "audio"),
        "voice": ("voice_note", "voice_note"), "document": ("document", "document"),
    }
    block_type, field = mapping[kind]
    return {"type": block_type, field: {"type": field, "media": file_id}}

def media_from_message(update, key):
    if key in ("media", "photo") and update.message.photo:
        return "photo", update.message.photo[-1].file_id
    if key in ("media", "video") and update.message.video:
        return "video", update.message.video.file_id
    if key in ("media", "animation") and update.message.animation:
        return "animation", update.message.animation.file_id
    if key in ("media", "audio") and update.message.audio:
        return "audio", update.message.audio.file_id
    if key in ("media", "voice") and update.message.voice:
        return "voice", update.message.voice.file_id
    if key in ("media", "doc") and update.message.document:
        return "document", update.message.document.file_id
    return None, None

def is_text_document(document):
    filename = (getattr(document, "file_name", None) or "").lower()
    extension = os.path.splitext(filename)[1]
    text_extensions = {
        ".txt", ".md", ".markdown", ".mdown", ".csv", ".tsv", ".json", ".jsonl",
        ".xml", ".yaml", ".yml", ".log", ".ini", ".toml", ".py", ".js", ".ts",
        ".jsx", ".tsx", ".html", ".htm", ".css", ".sql", ".sh", ".bat", ".ps1",
        ".java", ".c", ".cpp", ".h", ".hpp", ".go", ".rs", ".php", ".rb",
        ".swift", ".kt", ".dart", ".vue", ".svelte", ".env", ".conf", ".cfg",
        ".properties", ".tex", ".rst", ".adoc",
    }
    mime_type = (getattr(document, "mime_type", None) or "").lower()
    text_mime_types = {
        "application/json", "application/ld+json", "application/xml",
        "application/yaml", "application/x-yaml", "application/toml",
        "application/javascript", "application/x-python-code",
    }
    return extension in text_extensions or mime_type.startswith("text/") or mime_type in text_mime_types


async def receive_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("control_widget"):
        update = control_widget_update_proxy(update, context)
    key = context.user_data.get("pending")
    collection = context.user_data.get("collection")

    document = update.message.document
    if key == "p" and document:
        if not is_text_document(document):
            await update.message.reply_text("⚠️ این فایل به‌عنوان فایل متنی شناخته نشد. فایل TXT، MD یا یک فایل متنی UTF-8 بفرست.")
            return True
        try:
            remote_file = await context.bot.get_file(document.file_id)
            raw_content = bytes(await remote_file.download_as_bytearray())
            file_text = raw_content.decode("utf-8-sig")
            if not file_text.strip():
                raise ValueError("فایل متنی خالیه.")
            new_blocks = markdown_text_to_blocks(file_text)
            blocks = context.user_data.setdefault("blocks", [])
            if len(blocks) + len(new_blocks) > MAX_RICH_BLOCKS:
                raise ValueError("Rich Message حداکثر 500 بلوک دارد.")
            blocks.extend(new_blocks)
            context.user_data.pop("pending", None)
            await update.message.reply_text(
                f"✅ متن فایل «{document.file_name or 'بدون نام'}» به پیام اضافه شد.",
                reply_markup=menu(context.user_data.get("menu_expanded", False), is_owner(update.effective_user)),
            )
            return True
        except UnicodeDecodeError:
            await update.message.reply_text("⚠️ فایل باید با کدگذاری UTF-8 ذخیره شده باشه.")
            return True
        except Exception as error:
            await update.message.reply_text(f"⚠️ تبدیل فایل به متن انجام نشد:\n{error}")
            return True

    if collection and key in ("collage", "slideshow"):
        if not update.message.photo and not update.message.video and not update.message.animation:
            return False
        if update.message.photo:
            kind, file_id = "photo", update.message.photo[-1].file_id
        elif update.message.video:
            kind, file_id = "video", update.message.video.file_id
        else:
            kind, file_id = "animation", update.message.animation.file_id
        if media_attachment_count(context.user_data.get("blocks", [])) + len(collection["blocks"]) >= MAX_MEDIA_ATTACHMENTS:
            await update.message.reply_text("⚠️ سقف 50 مدیا برای این Rich Message پر شده.", reply_markup=collection_menu())
            return True
        collection["blocks"].append(input_media(kind, file_id))
        count = len(collection["blocks"])
        await update.message.reply_text(
            f"✅ مدیای شماره {count} اضافه شد.\n\nهنوز می‌توانی موارد بیشتری بفرستی.",
            reply_markup=collection_menu(),
        )
        return True

    if key not in ("media", "photo", "video", "animation", "audio", "voice", "doc"):
        return False

    kind, file_id = media_from_message(update, key)
    if not file_id:
        return False

    context.user_data.pop("pending", None)
    blocks = context.user_data.setdefault("blocks", [])
    if media_attachment_count(blocks) >= MAX_MEDIA_ATTACHMENTS:
        context.user_data.pop("pending", None)
        await update.message.reply_text("⚠️ این پیام به سقف 50 مدیای Rich Message رسیده.")
        return True
    add_block(context, input_media(kind, file_id))
    await send_editor_reply(update.message, context)
    return True

def wallex_decimal(value):
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            return Decimal("0")
        return number
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def wallex_get(path, authenticated=True):
    api_key = os.getenv("WALLEX_API_KEY", "").strip()
    if authenticated and not api_key:
        raise RuntimeError("کلید والکس روی سرور تنظیم نشده")
    headers = {"Accept": "application/json"}
    if authenticated:
        headers["X-API-Key"] = api_key
    request = urllib.request.Request("https://api.wallex.ir" + path, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"والکس درخواست را نپذیرفت (HTTP {error.code})") from None
    except (urllib.error.URLError, TimeoutError):
        raise RuntimeError("ارتباط با سرور والکس برقرار نشد") from None
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise RuntimeError("والکس پاسخ معتبر برنگرداند")
    return payload.get("result") or {}


def parse_wallex_wallet_data(balances_result, markets_result):
    balances = balances_result.get("balances")
    if not isinstance(balances, dict):
        raise RuntimeError("ساختار موجودی کیف پول در پاسخ والکس معتبر نیست")
    markets = markets_result.get("symbols") or markets_result.get("markets") or markets_result
    if isinstance(markets, list):
        markets = {
            str(item.get("symbol")): item
            for item in markets
            if isinstance(item, dict) and item.get("symbol")
        }
    if not isinstance(markets, dict):
        raise RuntimeError("فهرست قیمت های والکس معتبر نیست")
    return balances, markets


async def fetch_wallex_wallet_data():
    balances_result, markets_result = await asyncio.gather(
        asyncio.to_thread(wallex_get, "/v1/account/balances"),
        asyncio.to_thread(wallex_get, "/v1/markets", authenticated=False),
    )
    return parse_wallex_wallet_data(balances_result, markets_result)


def wallex_total_value(balances, markets):
    total = Decimal("0")
    unpriced = []
    for symbol, item in balances.items():
        if not isinstance(item, dict):
            continue
        amount = wallex_decimal(item.get("value", "0"))
        if amount <= 0:
            continue
        asset = str(item.get("asset") or symbol).upper()
        price = wallex_tmn_price(asset, markets)
        if price <= 0:
            unpriced.append(asset)
            continue
        total += amount * price
    return total, unpriced


def wallex_format_amount(value, places=8):
    number = wallex_decimal(value)
    rendered = f"{number:,.{places}f}"
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def wallex_market_price(market):
    if not isinstance(market, dict):
        return Decimal("0")
    stats = market.get("stats") or {}
    return wallex_decimal(stats.get("lastPrice") or market.get("lastPrice"))


def wallex_tmn_price(asset, markets, usdt_tmn=None):
    asset = str(asset or "").upper()
    if asset == "TMN":
        return Decimal("1")

    # Build a conversion graph from every active Wallex market.
    # A market BASEQUOTE means 1 BASE = price QUOTE.
    graph = {}
    for symbol, market in markets.items():
        if not isinstance(market, dict):
            continue
        base = str(market.get("baseAsset") or "").upper()
        quote_asset = str(market.get("quoteAsset") or "").upper()
        if not base or not quote_asset:
            market_symbol = str(market.get("symbol") or symbol).upper()
            # Fall back to known quote suffixes when metadata is absent.
            for suffix in ("TMN", "USDT", "USDC", "BTC", "ETH"):
                if market_symbol.endswith(suffix) and len(market_symbol) > len(suffix):
                    base, quote_asset = market_symbol[:-len(suffix)], suffix
                    break
        price = wallex_market_price(market)
        if not base or not quote_asset or price <= 0:
            continue
        graph.setdefault(base, []).append((quote_asset, price))
        graph.setdefault(quote_asset, []).append((base, Decimal("1") / price))

    # Prefer a direct Toman market whenever it exists. A normal BFS can
    # otherwise take an earlier-listed BTC/ETH route before checking a direct
    # asset/TMN market, producing a less reliable valuation.
    direct = [
        rate for next_asset, rate in graph.get(asset, [])
        if next_asset == "TMN" and rate > 0
    ]
    if direct:
        return direct[0]

    # For indirect conversion, use the shortest route and give stablecoin
    # markets deterministic priority over volatile bridge assets.
    bridge_priority = {"USDT": 0, "USDC": 1, "TMN": -1, "BTC": 2, "ETH": 3}
    queue = [(asset, Decimal("1"))]
    visited = {asset}
    while queue:
        current, rate = queue.pop(0)
        edges = sorted(
            graph.get(current, []),
            key=lambda edge: (bridge_priority.get(edge[0], 4), edge[0]),
        )
        for next_asset, edge_rate in edges:
            if next_asset == "TMN":
                return rate * edge_rate
            if next_asset not in visited:
                visited.add(next_asset)
                queue.append((next_asset, rate * edge_rate))
    return Decimal("0")

def wallex_wallet_html(balances, markets):
    total, unpriced = wallex_total_value(balances, markets)
    if unpriced:
        total_text = "محاسبه کامل نیست"
        detail = "قیمت بعضی دارایی ها در بازار والکس در دسترس نیست"
    else:
        total_text = wallex_format_amount(total, 0)
        detail = "ارزش کل دارایی های کیف پول"
    updated = datetime.now(timezone.utc).isoformat()
    return (
        '<!doctype html><html lang="fa" dir="rtl"><head><meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta name="color-scheme" content="dark">'
        '<style>'
        '*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}'
        'html,body{width:100%;min-height:100%;margin:0}'
        'html{direction:rtl;background:#07151b}'
        'body{padding:13px;background:radial-gradient(ellipse at 8% 0%,rgba(0,198,168,.22),transparent 42%),radial-gradient(ellipse at 100% 100%,rgba(255,177,74,.12),transparent 38%),linear-gradient(145deg,#0b2028,#08151d 58%,#101c25);color:#f4fbfa;font-family:Tahoma,"Arial",sans-serif;direction:rtl;text-align:right}'
        'main{width:100%;max-width:640px;margin:0 auto}'
        '.card{position:relative;isolation:isolate;overflow:hidden;padding:21px 19px 17px;border-radius:28px;background:linear-gradient(150deg,rgba(26,62,69,.98),rgba(13,36,45,.98) 52%,rgba(13,28,40,.99));border:1px solid rgba(111,220,198,.22);box-shadow:0 20px 48px rgba(0,0,0,.38),inset 0 1px rgba(255,255,255,.1)}'
        '.card:before{content:"";position:absolute;z-index:-1;width:230px;height:230px;left:-115px;top:-140px;border-radius:50%;background:rgba(0,224,185,.2);filter:blur(12px)}'
        '.card:after{content:"";position:absolute;z-index:-1;width:150px;height:150px;right:-90px;bottom:-105px;border-radius:50%;background:rgba(255,185,88,.13);filter:blur(9px)}'
        '.top{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:30px;direction:rtl}'
        '.brand{display:flex;align-items:center;gap:11px;font-size:15px;font-weight:800;color:#f5fffc}'
        '.brand-icon{width:43px;height:43px;display:grid;place-items:center;border-radius:15px;background:linear-gradient(145deg,#37e0bd,#139b9b);box-shadow:0 8px 22px rgba(20,209,174,.24);color:#062b2c;font-size:22px}'
        '.live{display:flex;align-items:center;gap:7px;padding:8px 11px;border-radius:30px;background:rgba(88,225,178,.09);border:1px solid rgba(111,236,190,.2);color:#a4f2d0;font-size:10px;font-weight:700;white-space:nowrap}'
        '.dot{width:7px;height:7px;border-radius:50%;background:#64efb4;box-shadow:0 0 12px rgba(100,239,180,.9)}'
        '.fa{direction:rtl;unicode-bidi:embed;text-align:right;letter-spacing:normal;word-spacing:normal;font-feature-settings:normal;font-kerning:normal}'
        '.label{color:#a9c6c4;font-size:12px;margin-bottom:9px}'
        '.amount-row{display:flex;align-items:center;justify-content:space-between;gap:14px;direction:rtl;flex-wrap:wrap}'
        '.amount-wrap{display:flex;align-items:baseline;gap:9px;min-width:0;flex:1}'
        '.amount{direction:ltr;unicode-bidi:isolate;text-align:left;overflow-wrap:anywhere;font-size:clamp(27px,7.5vw,39px);font-weight:900;letter-spacing:-1.2px;line-height:1.35;color:#f1fffb;text-shadow:0 3px 22px rgba(49,225,185,.12);font-variant-numeric:tabular-nums}'
        '.currency{display:inline-block;direction:rtl;unicode-bidi:embed;color:#9edbd0;font-size:13px;font-weight:800;white-space:nowrap;letter-spacing:normal}'
        '.refresh{position:relative;flex:0 0 54px;width:54px;height:54px;display:grid;place-items:center;border:1px solid rgba(154,255,224,.55);border-radius:19px;background:linear-gradient(145deg,#ffca73,#ff9d4d 48%,#ed784d);color:#163238;cursor:pointer;box-shadow:0 9px 24px rgba(255,157,77,.2),inset 0 1px rgba(255,255,255,.55);transition:transform .2s,filter .2s,box-shadow .2s}'
        '.refresh:before{content:"";position:absolute;inset:4px;border:1px solid rgba(255,255,255,.35);border-radius:14px}'
        '.refresh:active{transform:scale(.92)}.refresh:hover{filter:brightness(1.08);box-shadow:0 12px 29px rgba(255,157,77,.32)}'
        '.refresh svg{position:relative;width:23px;height:23px;transition:transform .4s}.refresh.busy svg{animation:spin .75s linear infinite}.refresh:disabled{opacity:.7;cursor:wait}'
        '@keyframes spin{to{transform:rotate(360deg)}}'
        '.divider{height:1px;margin:22px 0 14px;background:linear-gradient(90deg,transparent,rgba(145,219,203,.28),transparent)}'
        '.bottom{display:flex;align-items:center;justify-content:space-between;gap:10px;color:#a4c0bf;font-size:11px;flex-wrap:wrap;direction:rtl}'
        '.updated{direction:ltr;unicode-bidi:isolate;font-variant-numeric:tabular-nums;color:#d1e6df;white-space:nowrap}'
        '.hint{display:flex;align-items:center;gap:7px;color:#8eaeaa;font-size:10px;margin-top:15px}'
        '.hint-mark{width:18px;height:18px;display:grid;place-items:center;border-radius:7px;background:rgba(79,213,178,.12);color:#83e5c7;font-size:11px}'
        '</style></head><body><main><section class="card">'
        '<div class="top"><div class="brand"><span class="brand-icon">◈</span><bdi class="fa" dir="rtl">کیف پول والکس</bdi></div>'
        '<span class="live"><i class="dot"></i><bdi class="fa" dir="rtl">موجودی حساب</bdi></span></div>'
        '<div class="label fa" dir="rtl">ارزش کل دارایی ها</div>'
        '<div class="amount-row"><div class="amount-wrap"><span class="amount" id="total" dir="auto">' + html_lib.escape(total_text) + '</span>'
        '<bdi class="currency" dir="rtl">&#x062A;&#x0648;&#x0645;&#x0627;&#x0646;</bdi></div>'
        '<button class="refresh" id="refresh" type="button" aria-label="بروزرسانی کیف پول" title="بروزرسانی کیف پول">'
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M20 7v5h-5"/><path d="M20 12a8 8 0 0 0-14.8-4L4 10"/><path d="M4 17v-5h5"/><path d="M4 12a8 8 0 0 0 14.8 4L20 14"/></svg></button></div>'
        '<div class="divider"></div>'
        '<div class="bottom"><bdi class="fa" id="detail" dir="rtl">' + html_lib.escape(detail) + '</bdi>'
        '<span class="updated" id="updated"></span></div>'
        '<div class="hint"><span class="hint-mark">↻</span><bdi class="fa" dir="rtl">برای دریافت آخرین موجودی، دکمه بروزرسانی رو بزن</bdi></div>'
        '</section></main><script>'
        'const W=window.TelegramWidget;let busy=false;'
        'const dateFmt=new Intl.DateTimeFormat("en-US-u-ca-persian-nu-latn",{timeZone:"Asia/Tehran",year:"numeric",month:"2-digit",day:"2-digit"});'
        'const timeFmt=new Intl.DateTimeFormat("en-GB",{timeZone:"Asia/Tehran",hour:"2-digit",minute:"2-digit",hourCycle:"h23"});'
        'function setUpdated(value){try{const d=value?new Date(value):new Date();const p=Object.fromEntries(dateFmt.formatToParts(d).map(x=>[x.type,x.value]));document.getElementById("updated").textContent=p.year+"/"+p.month+"/"+p.day+"  "+timeFmt.format(d);}catch(e){document.getElementById("updated").textContent="";}}'
        'function draw(d){document.getElementById("total").textContent=d.total||"نامشخص";document.getElementById("detail").textContent=d.detail||"";setUpdated(d.updated);}'
        'setUpdated(' + json.dumps(updated) + ');'
        'async function refresh(){if(busy)return;if(!W||typeof W.request!=="function"){document.getElementById("detail").textContent="رفرش در این نسخه تلگرام پشتیبانی نمیشه";return;}busy=true;'
        'const b=document.getElementById("refresh");b.disabled=true;b.classList.add("busy");'
        'try{const a=await W.request("wallex:refresh");const d=JSON.parse(a.result||"{}");if(d.error)throw Error(d.error);draw(d);}'
        'catch(e){document.getElementById("detail").textContent="بروزرسانی ناموفق بود · دوباره تلاش کن";}'
        'finally{busy=false;b.disabled=false;b.classList.remove("busy");}}'
        'document.getElementById("refresh").addEventListener("click",refresh);'
        '</script></body></html>'
    )


async def send_wallex_wallet_widget(message):
    if not os.getenv("WALLEX_API_KEY", "").strip():
        await message.reply_text("🔐 متغیر محرمانه WALLEX_API_KEY روی Railway تنظیم نشده")
        return
    try:
        balances, markets = await fetch_wallex_wallet_data()
        widget_html = wallex_wallet_html(balances, markets)
        validate_widget_html(widget_html)
        sent = await api_async(
            "sendWidget",
            {
                "chat_id": message.chat_id,
                "html": widget_html,
                "caption": "کیف پول والکس",
                "allow_queries": True,
            },
            base_url=WIDGET_API_BASE,
            timeout=60,
        )
        if not sent.get("message_id"):
            raise RuntimeError("تلگرام شناسه پیام ویجت را برنگرداند")
    except Exception as error:
        await message.reply_text(f"⚠️ دریافت کیف پول ناموفق بود\n{type(error).__name__}: {error}")


async def receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("control_widget"):
        update = control_widget_update_proxy(update, context)
    wizard = context.user_data.get("btn_wizard")
    text = update.message.text or ""

    if (
        text.strip() == "و"
        and not context.user_data.get("pending")
        and not context.user_data.get("btn_wizard")
        and not context.user_data.get("widget_mode")
        and not context.user_data.get("ai_chat_mode")
    ):
        if not is_owner(update.effective_user):
            await update.message.reply_text("⛔ نمایش کیف پول والکس فقط برای مدیر اصلی فعاله.")
            return
        await send_wallex_wallet_widget(update.message)
        return

    if context.user_data.get("pending") == "parts":
        draft = context.user_data.get("multipart")
        if not draft:
            context.user_data["multipart"] = {"chunks": [], "characters": 0}
            draft = context.user_data["multipart"]
        if not text:
            await update.message.reply_text("⚠️ فقط پیام متنی قابل ثبت است. قطعه بعدی را بفرست.")
            return
        if len(draft["chunks"]) >= 100:
            await update.message.reply_text("⚠️ حداکثر 100 قطعه میتونی وارد کنی.")
            return
        if draft["characters"] + len(text) > 200000:
            await update.message.reply_text("⚠️ حجم متن از سقف 200000 کاراکتر بیشتر میشه.")
            return
        draft["chunks"].append({
            "text": text,
            "has_entities": bool(update.message.entities),
            "blocks": text_to_rich_blocks(update.message),
        })
        draft["characters"] += len(text)
        await update.message.reply_text(
            f"✅ قطعه {len(draft['chunks'])} ثبت شد\n"
            f"📏 مجموع: {draft['characters']} کاراکتر\n\n"
            "قطعه بعدی رو بفرست یا از دکمه های زیر برای ثبت نهایی استفاده کن",
            reply_markup=parts_menu(),
        )
        return

    if wizard == "label":
        label = text.strip()
        if not label:
            await update.message.reply_text("متن دکمه خالیه. دوباره بفرست.")
            return
        if len(label) > 64:
            await update.message.reply_text("متن دکمه حداکثر 64 کاراکتر باشه.")
            return
        context.user_data["btn_wizard_label"] = label
        context.user_data["btn_wizard"] = "type"
        await update.message.reply_text(
            f"متن دکمه: {label}\n\nحالا بگو با زدن دکمه چه اتفاقی بیفته",
            reply_markup=button_type_menu(),
        )
        return

    if wizard == "value":
        value = text.strip()
        kind = context.user_data.get("btn_wizard_type")
        label = context.user_data.get("btn_wizard_label", "دکمه")
        if not value:
            await update.message.reply_text("مقدار خالیه. دوباره بفرست.")
            return
        button = {"text": label, "style": context.user_data.get("btn_wizard_style", "primary")}
        if kind in ("url", "web_app", "login_url"):
            # Accept domains with or without a scheme.
            if not value.lower().startswith(("http://", "https://")):
                value = "https://" + value.lstrip("/")
            # Mini Apps and Telegram Login require HTTPS.
            if kind in ("web_app", "login_url") and value.lower().startswith("http://"):
                value = "https://" + value[7:]
        if kind == "url":
            button["url"] = value
        elif kind == "web_app":
            button["web_app"] = {"url": value}
        elif kind == "login_url":
            button["login_url"] = {"url": value, "request_write_access": True}
        elif kind == "copy":
            if len(value) > 256:
                await update.message.reply_text("متن کپی حداکثر 256 کاراکتره. کوتاه‌تر بفرست.")
                return
            button["copy_text"] = {"text": value}
        elif kind == "switch_inline_query":
            button["switch_inline_query"] = value
        elif kind == "switch_inline_query_current_chat":
            button["switch_inline_query_current_chat"] = value
        elif kind == "switch_inline_query_chosen_chat":
            button["switch_inline_query_chosen_chat"] = {
                "query": value, "allow_user_chats": True, "allow_bot_chats": True,
                "allow_group_chats": True, "allow_channel_chats": True
            }
        context.user_data["btn_current"] = button
        context.user_data["btn_wizard"] = "confirm"
        await update.message.reply_text(
            button_preview_text(button),
            reply_markup=button_preview_menu(),
        )
        return

    key = context.user_data.pop("pending", None)

    if not key and context.user_data.get("ai_chat_mode"):
        await run_ai_chat(update.message, context, text)
        return

    if key and key.startswith("ai_admin_"):
        if not is_owner(update.effective_user):
            await update.message.reply_text("⛔ فقط مدیر اصلی می تونه سرویس هوش مصنوعی رو تنظیم کنه.")
            return
        draft = context.user_data.setdefault("ai_service_draft", {})
        if key == "ai_admin_name":
            value = text.strip()
            if not value:
                context.user_data["pending"] = key
                await update.message.reply_text("اسم سرویس خالیه. دوباره بفرست.")
                return
            draft["name"] = value[:60]
            context.user_data["pending"] = "ai_admin_url"
            await update.message.reply_text(ai_wizard_prompt("url"), reply_markup=cancel_menu())
            return
        if key == "ai_admin_url":
            value = text.strip()
            if not value.lower().startswith(("http://", "https://")):
                context.user_data["pending"] = key
                await update.message.reply_text("آدرس باید با http:// یا https:// شروع بشه. دوباره بفرست.")
                return
            draft["url"] = value
            context.user_data["pending"] = "ai_admin_key"
            await update.message.reply_text(ai_wizard_prompt("key"), reply_markup=cancel_menu())
            return
        if key == "ai_admin_key":
            draft["api_key"] = "" if text.strip() == "-" else text.strip()
            context.user_data["pending"] = "ai_admin_model"
            await update.message.reply_text(ai_wizard_prompt("model"), reply_markup=cancel_menu())
            return
        if key == "ai_admin_model":
            value = text.strip()
            if not value:
                context.user_data["pending"] = key
                await update.message.reply_text("اسم مدل خالیه. دوباره بفرست.")
                return
            draft["model"] = value[:120]
            context.user_data["pending"] = "ai_admin_headers"
            await update.message.reply_text(ai_wizard_prompt("headers"), reply_markup=cancel_menu())
            return
        if key == "ai_admin_headers":
            if text.strip() == "-":
                draft["headers"] = {}
            else:
                try:
                    headers = json.loads(text)
                    if not isinstance(headers, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in headers.items()):
                        raise ValueError()
                    draft["headers"] = headers
                except Exception:
                    context.user_data["pending"] = key
                    await update.message.reply_text("JSON هدرها معتبر نیست. یک آبجکت از نام و مقدار هدرها بفرست یا - بزن.")
                    return
            await update.message.reply_text(
                "اطلاعات ثبت شد. قبل از ذخیره، اتصال سرویس رو آزمایش می کنیم.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔌 آزمایش و ذخیره سرویس", callback_data="ai:test_save")],
                    [InlineKeyboardButton("❌ لغو", callback_data="ai:admin")],
                ]),
            )
            return

    if key == "connector_name":
        name = text.strip()
        if not name or len(name) > 60:
            context.user_data["pending"] = key
            await update.message.reply_text("اسم باید بین 1 تا 60 کاراکتر باشه")
            return
        context.user_data["connector_draft"]["name"] = name
        context.user_data["pending"] = "connector_url"
        await update.message.reply_text("آدرس کامل endpoint رو بفرست. مثلا https://example.com/mcp")
        return

    if key == "connector_tool_args":
        if not is_owner(update.effective_user) or update.effective_chat.type != "private":
            context.user_data.pop("pending", None)
            context.user_data.pop("connector_tool_draft", None)
            return
        if len(text) > 16000:
            await update.message.reply_text("ورودی خیلی طولانیه. حداکثر 16 هزار کاراکتر بفرست")
            return
        try:
            arguments = json.loads(text)
        except json.JSONDecodeError:
            await update.message.reply_text("JSON معتبر نیست. دوباره بفرست")
            return
        if not isinstance(arguments, dict):
            await update.message.reply_text("ورودی باید یک شیء JSON باشه. مثلا {}")
            return
        draft = context.user_data.get("connector_tool_draft") or {}
        draft["arguments"] = arguments
        context.user_data["connector_tool_draft"] = draft
        context.user_data.pop("pending", None)
        await update.message.reply_text(
            f"⚠️ آماده اجرای ابزار «{draft.get('tool_name', 'نامشخص')}» هستی؟\nورودی: {json.dumps(arguments, ensure_ascii=False)[:1200]}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ تایید و اجرا", callback_data=f"conn:execute:{draft.get('connector_id', '')}")],
                [InlineKeyboardButton("❌ لغو", callback_data="conn:canceltool")],
            ]),
        )
        return

    if key == "connector_api_key":
        if not is_owner(update.effective_user) or update.effective_chat.type != "private":
            context.user_data.pop("pending", None)
            return
        connector_id = context.user_data.get("connector_key_target")
        config = context.application.bot_data.get("ai_config") or ai_default_config()
        item = config.get("connectors", {}).get(connector_id)
        if not item:
            context.user_data.pop("pending", None)
            context.user_data.pop("connector_key_target", None)
            await update.message.reply_text("اتصال پیدا نشد")
            return
        secret_value = text.strip()
        if len(secret_value) > 4096:
            await update.message.reply_text("کلید خیلی طولانیه. دوباره بفرست")
            return
        try:
            await update.message.delete()
        except Exception:
            await update.message.reply_text("⚠️ نتونستم پیام حاوی کلید رو حذف کنم. کلید ذخیره نشد. پیام رو دستی حذف کن و دوباره تلاش کن")
            return
        old_key = item.get("api_key", "")
        item["api_key"] = "" if secret_value == "-" else secret_value
        try:
            await persist_ai_config(config, context)
        except Exception as error:
            item["api_key"] = old_key
            await update.message.reply_text(f"ذخیره کلید ناموفق بود: {type(error).__name__}")
            return
        context.user_data.pop("pending", None)
        context.user_data.pop("connector_key_target", None)
        await update.message.reply_text("✅ کلید به صورت متن ساده در گروه ذخیره شد" if secret_value != "-" else "✅ کلید اتصال پاک شد", reply_markup=connector_menu(config))
        return

    if key == "connector_url":
        from urllib.parse import urlsplit
        value = text.strip()
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or len(value) > 1500:
            context.user_data["pending"] = key
            await update.message.reply_text("فعلا فقط آدرس HTTPS معتبر قبول میشه. دوباره بفرست")
            return
        draft = context.user_data.get("connector_draft") or {}
        draft["url"] = value
        config = context.application.bot_data.get("ai_config") or ai_default_config()
        config.setdefault("connectors", {})
        connector_id = secrets.token_hex(6)
        config["connectors"][connector_id] = {
            "name": draft["name"], "type": draft["type"], "url": draft["url"],
            "enabled": False, "created_at": int(time.time()),
        }
        try:
            await persist_ai_config(config, context)
        except Exception as error:
            config["connectors"].pop(connector_id, None)
            context.user_data["pending"] = key
            await update.message.reply_text(f"ذخیره اتصال ناموفق بود: {type(error).__name__}")
            return
        context.user_data.pop("connector_draft", None)
        context.user_data.pop("pending", None)
        await update.message.reply_text(
            f"✅ آدرس اتصال «{draft['name']}» ثبت شد. برای افزودن کلید و آزمایش، اتصال رو از فهرست باز کن.",
            reply_markup=connector_menu(config),
        )
        return

    if key == "ai_widget":
        await run_ai_widget(update.message, context, text)
        return

    if key in ("ai_rich", "ai_text", "ai_code"):
        mode = {"ai_rich": "rich", "ai_text": "text", "ai_code": "code"}[key]
        await run_ai_one_shot(update.message, context, mode, text)
        return

    if not key:
        return

    if key == "access_grant":
        if not is_owner(update.effective_user):
            await update.message.reply_text("⛔ فقط مدیر اصلی می‌تواند دسترسی بدهد.")
            return
        raw_id = text.strip()
        if not raw_id.isdigit() or int(raw_id) <= 0:
            context.user_data["pending"] = "access_grant"
            await update.message.reply_text("شناسه باید یک عدد مثبت باشد. دوباره شناسه عددی کاربر را بفرست.")
            return
        user_id = int(raw_id)
        if user_id == OWNER_ID:
            await update.message.reply_text("خودت مدیر اصلی هستی و از قبل دسترسی کامل داری.")
            return
        users = context.bot_data.setdefault("authorized_users", set())
        if user_id in users:
            await update.message.reply_text("این کاربر از قبل دسترسی دارد.", reply_markup=access_menu_markup(users))
            return
        users.add(user_id)
        try:
            await persist_access_users(users, context)
        except Exception as error:
            users.discard(user_id)
            context.user_data["pending"] = "access_grant"
            print(f"ACCESS GRANT FAILED: {type(error).__name__}: {error}", flush=True)
            await update.message.reply_text(f"⚠️ ذخیره‌سازی ناموفق بود: {type(error).__name__}: {error}")
            return
        await update.message.reply_text(f"✅ دسترسی کاربر {user_id} فعال شد.", reply_markup=access_menu_markup(users))
        return

    blocks = context.user_data.setdefault("blocks", [])
    lines = [x.strip() for x in text.splitlines() if x.strip()]

    try:
        block = None
        if key == "p":
            new_blocks = text_to_rich_blocks(update.message)
            if len(blocks) + len(new_blocks) > MAX_RICH_BLOCKS:
                raise ValueError("Rich Message حداکثر 500 بلوک دارد.")
            blocks.extend(new_blocks)
            await update.message.reply_text(editor_text(blocks), reply_markup=menu(context.user_data.get("menu_expanded", False), is_owner(update.effective_user)))
            return
        elif key.startswith("h:"):
            try:
                size = int(key.split(":", 1)[1])
            except ValueError:
                raise ValueError("اندازه تیتر نامعتبر است.")
            if not 1 <= size <= 6:
                raise ValueError("اندازه تیتر باید بین 1 تا 6 باشد.")
            if not text.strip():
                raise ValueError("متن تیتر خالی است.")
            block = {"type": "heading", "text": text.strip(), "size": size}
        elif key == "c":
            parts = text.split("\n", 1)
            if len(parts) < 2:
                raise ValueError("دو خط لازم است: زبان و کد.")
            block = {"type": "pre", "text": parts[1], "language": parts[0].strip()}
        elif key in ("l", "x", "ol"):
            items = []
            for index, line in enumerate(lines, 1):
                item = {"blocks": [{"type": "paragraph", "text": line}]}
                if key == "x":
                    item["has_checkbox"] = True
                    item["is_checked"] = False
                if key == "ol":
                    item["value"] = index
                    item["type"] = "1"
                items.append(item)
            if not items:
                raise ValueError("حداقل یک مورد لازم است.")
            block = {"type": "list", "items": items}
            if key == "x":
                context.user_data["checklist_draft"] = block
                await update.message.reply_text(
                    checklist_text(block),
                    reply_markup=checklist_menu(block),
                    parse_mode="HTML",
                )
                return
        elif key == "f":
            block = {"type": "footer", "text": text}
        elif key == "m":
            block = {"type": "mathematical_expression", "expression": text}
        elif key == "src":
            source_text = text.strip()
            if not source_text:
                raise ValueError("متن منبع خالی است.")
            source_id = f"source-{int(time.time() * 1000)}"
            context.user_data["pending"] = f"src_link:{source_id}:{source_text}"
            await update.message.reply_text("📚 حالا متن قابل‌کلیک را بفرست.\n\nمثال: <code>مشاهده تست</code>", parse_mode="HTML", reply_markup=cancel_menu())
            return
        elif key.startswith("src_link:"):
            parts = key.split(":", 2)
            if len(parts) != 3:
                raise ValueError("اطلاعات منبع ناقص است.")
            source_id, source_text = parts[1], parts[2]
            link_text = text.strip()
            if not link_text:
                raise ValueError("متن لینک منبع خالی است.")
            sources = get_source_texts(context)
            sources[source_id] = source_text
            save_source_texts(sources)
            block = {
                "type": "paragraph",
                "text": [{
                    "type": "button",
                    "button": {
                        "text": link_text,
                        "style": "link",
                        "callback_data": f"src_show:{source_id}",
                    },
                }],
            }
            context.user_data.pop("pending", None)
            add_block(context, block)
            await update.message.reply_text(editor_text(context.user_data.get("blocks", [])), reply_markup=menu(context.user_data.get("menu_expanded", False), is_owner(update.effective_user)))
            return
        elif key.startswith("a_target:"):
            try:
                target_index = int(key.split(":", 1)[1])
            except ValueError:
                raise ValueError("مقصد لینک نامعتبر است.")
            if not 0 <= target_index < len(blocks):
                raise ValueError("مقصد لینک دیگر وجود ندارد؛ دوباره از «🔗 لینک داخلی» انتخابش کن.")
            target_block = blocks[target_index]
            target_text = target_block.get("text")
            if not isinstance(target_text, (str, list, dict)):
                raise ValueError("این بخش متن قابل لینک‌دادن ندارد.")
            phrase = text.strip()
            if not phrase:
                raise ValueError("عبارت مقصد خالی است.")
            anchor_name = f"section-{int(time.time() * 1000)}-{target_index}"
            updated_text, found = link_phrase_to_anchor(target_text, phrase, anchor_name)
            if not found:
                context.user_data["pending"] = f"a_target:{target_index}"
                raise ValueError("این عبارت داخل بلوک انتخاب‌شده پیدا نشد. عبارت دقیق را بفرست.")
            target_block["text"] = updated_text
            context.user_data["pending"] = f"a_link:{anchor_name}"
            await update.message.reply_text(
                "🔗 <b>حالا متن لینک را بفرست</b>\n\nمثال: <code>مشاهده مشخصات</code>\n\n💡 فقط همین عبارت به مقصد لینک می‌شود، نه کل بلوک.",
                parse_mode="HTML",
                reply_markup=cancel_menu(),
            )
            return
        elif key.startswith("a_link:"):
            anchor_name = key.split(":", 1)[1]
            link_text = text.strip()
            if not anchor_name or not link_text:
                raise ValueError("اطلاعات لینک داخلی ناقص است.")
            block = {
                "type": "paragraph",
                "text": [{
                    "type": "anchor_link",
                    "text": link_text,
                    "anchor_name": anchor_name
                }]
            }
        elif key == "d":
            parts = text.split("\n", 1)
            if len(parts) < 2:
                raise ValueError("عنوان و محتوای Details را در دو خط بفرست.")
            block = {
                "type": "details", "summary": parts[0].strip(),
                "blocks": [{"type": "paragraph", "text": parts[1].strip()}],
                "is_open": False,
            }
        elif key == "t":
            rows = []
            for row_index, line in enumerate(lines):
                cells = []
                for cell in line.split("|"):
                    cell_obj = {"text": cell.strip(), "align": "center"}
                    if row_index == 0:
                        cell_obj["is_header"] = True
                    cells.append(cell_obj)
                rows.append(cells)
            if not rows:
                raise ValueError("حداقل یک ردیف لازم است.")
            if max(len(row) for row in rows) > 20:
                raise ValueError("جدول حداکثر 20 ستون می‌تواند داشته باشد.")
            block = {"type": "table", "cells": rows, "is_bordered": True, "is_striped": True}
        elif key == "map":
            parts = [x.strip() for x in text.split(",")]
            if len(parts) != 3:
                raise ValueError("فرمت دقیق: latitude,longitude,zoom")
            lat, lon, zoom = parts
            zoom_value = int(zoom)
            if not 0 <= zoom_value <= 24:
                raise ValueError("zoom باید بین 0 تا 24 باشد.")
            block = {
                "type": "map",
                "location": {"latitude": float(lat), "longitude": float(lon)},
                "zoom": zoom_value,
            }
        elif key == "btn":
            buttons = []
            for line in lines:
                parts = [x.strip() for x in line.split("|")]
                if len(parts) < 2:
                    continue
                button = {"text": parts[0], "style": "primary"}
                button_type = parts[1]
                value = parts[2] if len(parts) > 2 else ""
                if button_type == "url":
                    if not value:
                        continue
                    button["url"] = value
                elif button_type == "callback_data":
                    if not value:
                        continue
                    if len(value.encode()) > 64:
                        raise ValueError("callback_data حداکثر 64 بایت است.")
                    button["callback_data"] = value
                elif button_type == "copy_text":
                    if not value:
                        continue
                    button["copy_text"] = {"text": value}
                elif button_type == "disabled":
                    button["disabled"] = {}
                else:
                    continue
                buttons.append(button)
            if not buttons:
                raise ValueError("فرمت دکمه درست نیست.")
            block = {"type": "buttons", "buttons": buttons[:8], "align": "center"}
        elif key in ("collage", "slideshow"):
            raise ValueError("برای این قابلیت عکس/ویدیو/انیمیشن را بفرست، نه متن.")
        else:
            raise ValueError("این قابلیت هنوز ورودی متنی ندارد.")

        add_block(context, block)
        await update.message.reply_text(editor_text(context.user_data.get("blocks", [])), reply_markup=menu(context.user_data.get("menu_expanded", False), is_owner(update.effective_user)))
    except Exception as e:
        context.user_data["pending"] = key
        await update.message.reply_text(f"⚠️ ورودی نامعتبر بود:\n{e}\n\nدوباره طبق مثال بالا بفرست.")

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not getattr(query, "is_control_widget", False):
        context.user_data.pop("control_widget", None)
    data = query.data
    answered = False

    async def answer(text=None, show_alert=False):
        nonlocal answered
        if answered:
            return
        await query.answer(text=text, show_alert=show_alert)
        answered = True

    try:
        # Enforce authorization inside the actual callback dispatcher as a second,
        # direct gate. This prevents restricted menu actions even if an earlier
        # guard handler fails to stop dispatch for any reason.
        if not is_authorized(context, query.from_user):
            await answer(
                "⛔ دسترسی شما به این ربات فعال نیست",
                show_alert=True,
            )
            draft_text = f"بهم دسترسی بده `{query.from_user.id}` ممنون"
            request_url = "https://t.me/M7M7777777?text=" + quote(draft_text, safe="")
            try:
                await api_async("sendRichMessage", {
                    "chat_id": query.message.chat_id,
                    "rich_message": {
                        "blocks": [
                            {
                                "type": "paragraph",
                                "text": "برای درخواست دسترسی روی دکمه زیر بزن:",
                            },
                            {
                                "type": "buttons",
                                "buttons": [{
                                    "text": "دسترسی میقام",
                                    "style": "danger",
                                    "url": request_url,
                                }],
                                "align": "center",
                            },
                        ],
                        "is_rtl": True,
                    },
                })
            except Exception as error:
                print(
                    f"ACCESS REQUEST RICH MESSAGE FAILED: {type(error).__name__}: {error}",
                    flush=True,
                )
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="ارسال دکمه درخواست دسترسی ناموفق بود. لطفا کمی بعد دوباره تلاش کن.",
                    disable_web_page_preview=True,
                )
            return

        if data.startswith("btnwizstyle:"):
            style = data.split(":", 1)[1]
            if style not in ("primary", "success", "danger"):
                await answer("ظاهر دکمه نامعتبره.", show_alert=True)
                return
            context.user_data["btn_wizard_style"] = style
            context.user_data["btn_wizard"] = "type"
            style_names = {"primary": "آبی", "success": "سبز", "danger": "قرمز"}
            await query.edit_message_text(
                f"ظاهر انتخاب‌شده: {style_names.get(style, style)}\n\nحالا عملکرد دکمه رو انتخاب کن",
                reply_markup=button_type_menu(),
            )
            return

        if data.startswith("btnwiz:"):
            action = data.split(":", 1)[1]
            if action == "add":
                if len(context.user_data.get("btn_draft", [])) >= 8:
                    await answer("حداکثر 8 دکمه میتونی بسازی.", show_alert=True)
                    return
                context.user_data["btn_wizard"] = "label"
                await query.edit_message_text("متن دکمه بعدی رو بفرست", reply_markup=cancel_menu())
                return
            if action == "style":
                await query.edit_message_text("🎨 ظاهر دکمه رو انتخاب کن", reply_markup=button_style_menu())
                return
            if action == "back_type":
                await query.edit_message_text("عملکرد دکمه رو انتخاب کن", reply_markup=button_type_menu())
                return
            if action in ("url", "disabled"):
                context.user_data["btn_wizard_type"] = action
                if action == "disabled":
                    button = {
                        "text": context.user_data.get("btn_wizard_label", "دکمه"),
                        "style": context.user_data.get("btn_wizard_style", "primary"),
                        "disabled": {},
                    }
                    context.user_data["btn_current"] = button
                    context.user_data["btn_wizard"] = "confirm"
                    await query.edit_message_text(button_preview_text(button), reply_markup=button_preview_menu())
                    return
                prompts = {
                    "url": "لینک رو بفرست\nمثال: https://telegram.org",
                }
                context.user_data["btn_wizard"] = "value"
                await query.edit_message_text(prompts[action], reply_markup=cancel_menu())
                return
            if action == "confirm":
                button = context.user_data.get("btn_current")
                buttons = context.user_data.setdefault("btn_draft", [])
                if not button:
                    await answer("دکمه‌ای برای ثبت وجود نداره.", show_alert=True)
                    return
                if len(buttons) >= 8:
                    await answer("حداکثر 8 دکمه میتونی بسازی.", show_alert=True)
                    return
                buttons.append(button)
                context.user_data.pop("btn_current", None)
                context.user_data.pop("btn_wizard_type", None)
                context.user_data.pop("btn_wizard_label", None)
                context.user_data["btn_wizard_style"] = "primary"
                context.user_data["btn_wizard"] = "menu"
                await query.edit_message_text(
                    button_wizard_text(buttons),
                    reply_markup=button_wizard_menu(),
                )
                return
            if action == "discard":
                context.user_data.pop("btn_current", None)
                context.user_data.pop("btn_wizard_type", None)
                context.user_data["btn_wizard"] = "type"
                await query.edit_message_text(
                    "عملکرد دیگه‌ای برای این دکمه انتخاب کن",
                    reply_markup=button_type_menu(),
                )
                return
            if action == "done":
                buttons = context.user_data.get("btn_draft", [])
                if not buttons:
                    await answer("هنوز دکمه‌ای نساختی.", show_alert=True)
                    return
                add_block(context, {"type": "buttons", "buttons": buttons[:8], "align": "center"})
                for key in ("btn_draft", "btn_current", "btn_wizard", "btn_wizard_type", "btn_wizard_label", "btn_wizard_style"):
                    context.user_data.pop(key, None)
                await show_editor(query, context)
                return

        if data.startswith("btn_auto:"):
            await answer("دکمه تعاملی فشرده شد")
            return

        if data == "conn:menu":
            if not is_owner(query.from_user) or query.message.chat.type != "private":
                await answer("⛔ فقط مدیر اصلی در گفتگوی خصوصی.", show_alert=True)
                return
            context.user_data.pop("pending", None)
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            config.setdefault("connectors", {})
            await query.edit_message_text(connector_overview(config), reply_markup=connector_menu(config))
            return

        if data == "conn:back":
            await query.edit_message_text(home_text(is_owner(query.from_user)), reply_markup=main_menu(is_owner(query.from_user)))
            return

        if data == "conn:add":
            if not is_owner(query.from_user) or query.message.chat.type != "private":
                await answer("⛔ فقط مدیر اصلی در گفتگوی خصوصی.", show_alert=True)
                return
            context.user_data["connector_draft"] = {}
            await query.edit_message_text("نوع اتصال رو انتخاب کن", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("تشخیص خودکار", callback_data="conn:type:auto")],
                [InlineKeyboardButton("MCP", callback_data="conn:type:mcp"), InlineKeyboardButton("REST API", callback_data="conn:type:rest")],
                [InlineKeyboardButton("مرورگر Playwright / CDP", callback_data="conn:type:browser")],
                [InlineKeyboardButton("سفارشی", callback_data="conn:type:custom")],
                [InlineKeyboardButton("❌ لغو", callback_data="conn:menu")],
            ]))
            return

        if data.startswith("conn:type:"):
            if not is_owner(query.from_user) or query.message.chat.type != "private":
                await answer("⛔ دسترسی مجاز نیست.", show_alert=True)
                return
            kind = data.split(":", 2)[2]
            if kind not in ("auto", "mcp", "rest", "browser", "custom"):
                await answer("نوع اتصال معتبر نیست.", show_alert=True)
                return
            context.user_data["connector_draft"] = {"type": kind}
            context.user_data["pending"] = "connector_name"
            await query.edit_message_text("یه اسم برای این اتصال بفرست", reply_markup=cancel_menu())
            return

        if data.startswith("conn:item:"):
            if not is_owner(query.from_user):
                await answer("دسترسی مجاز نیست.", show_alert=True)
                return
            connector_id = data.split(":", 2)[2]
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            item = config.get("connectors", {}).get(connector_id)
            if not item:
                await answer("اتصال پیدا نشد.", show_alert=True)
                return
            await query.edit_message_text(
                f"🔗 {item.get('name', 'اتصال')}\nنوع: {item.get('type', 'custom')}\nآدرس: {item.get('url', '')}\nکلید: {'ثبت شده' if item.get('api_key') else 'ندارد'}\nوضعیت اجرا: {'فعال' if item.get('enabled') else 'غیرفعال'}",
                reply_markup=connector_item_menu(connector_id),
            )
            return

        if data.startswith("conn:tools:"):
            if not is_owner(query.from_user) or query.message.chat.type != "private":
                await answer("دسترسی مجاز نیست.", show_alert=True)
                return
            connector_id = data.split(":", 2)[2]
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            item = config.get("connectors", {}).get(connector_id)
            if not item:
                await answer("اتصال پیدا نشد.", show_alert=True)
                return
            if item.get("type") not in ("mcp", "auto"):
                await answer("این اتصال از نوع MCP نیست.", show_alert=True)
                return
            await query.edit_message_text("🧰 در حال دریافت فهرست ابزارهای MCP...")
            try:
                tools = await asyncio.to_thread(connect_mcp_and_list_tools, item)
                context.user_data["connector_tool_connector_id"] = connector_id
                context.user_data["connector_tool_list"] = [
                    {
                        "name": str(tool.get("name", ""))[:120],
                        "description": str(tool.get("description", ""))[:500],
                        "inputSchema": tool.get("inputSchema", {}) if isinstance(tool.get("inputSchema", {}), dict) else {},
                    }
                    for tool in tools[:40]
                    if isinstance(tool, dict) and tool.get("name")
                ]
                if not context.user_data["connector_tool_list"]:
                    summary = "اتصال MCP برقرار شد ولی سرور هیچ ابزاری معرفی نکرد"
                else:
                    summary = "ابزارها رو انتخاب کن. اجرای هر ابزار نیاز به تایید جداگانه داره."
                await query.edit_message_text(
                    f"✅ ارتباط MCP برقرار شد\n{summary}",
                    reply_markup=connector_tools_menu(connector_id, context.user_data["connector_tool_list"]),
                )
            except Exception as error:
                await query.edit_message_text(
                    f"⚠️ اتصال MCP ناموفق بود | {type(error).__name__}: {error}",
                    reply_markup=connector_item_menu(connector_id),
                )
            return

        if data.startswith("conn:tool:"):
            if not is_owner(query.from_user) or query.message.chat.type != "private":
                await answer("دسترسی مجاز نیست.", show_alert=True)
                return
            parts = data.split(":")
            if len(parts) != 4:
                await answer("درخواست ابزار معتبر نیست.", show_alert=True)
                return
            connector_id = parts[2]
            try:
                tool_index = int(parts[3])
            except ValueError:
                await answer("شماره ابزار معتبر نیست.", show_alert=True)
                return
            tool_list = context.user_data.get("connector_tool_list", [])
            if context.user_data.get("connector_tool_connector_id") != connector_id:
                await answer("فهرست ابزار مربوط به این اتصال نیست. دوباره دریافتش کن.", show_alert=True)
                return
            if tool_index < 0 or tool_index >= len(tool_list):
                await answer("فهرست ابزارها منقضی شده. دوباره دریافتش کن.", show_alert=True)
                return
            selected_tool = tool_list[tool_index]
            tool_name = selected_tool.get("name", "ابزار") if isinstance(selected_tool, dict) else str(selected_tool)
            schema = selected_tool.get("inputSchema", {}) if isinstance(selected_tool, dict) else {}
            schema_text = json.dumps(schema, ensure_ascii=False)[:2500]
            description = selected_tool.get("description", "") if isinstance(selected_tool, dict) else ""
            context.user_data["connector_tool_draft"] = {
                "connector_id": connector_id,
                "tool_name": tool_name,
                "input_schema": schema if isinstance(schema, dict) else {},
            }
            context.user_data["pending"] = "connector_tool_args"
            await query.edit_message_text(
                f"ابزار «{tool_name}» انتخاب شد\n{description[:500]}\nساختار ورودی:\n{schema_text}\nورودی رو به شکل JSON بفرست. برای ورودی خالی فقط {{}} بفرست.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ لغو", callback_data="conn:canceltool")]]),
            )
            return

        if data == "conn:canceltool":
            context.user_data.pop("connector_tool_draft", None)
            context.user_data.pop("pending", None)
            await query.edit_message_text("اجرای ابزار لغو شد")
            return

        if data.startswith("conn:execute:"):
            if not is_owner(query.from_user) or query.message.chat.type != "private":
                await answer("دسترسی مجاز نیست.", show_alert=True)
                return
            connector_id = data.split(":", 2)[2]
            draft = context.user_data.get("connector_tool_draft") or {}
            if draft.get("connector_id") != connector_id or not isinstance(draft.get("arguments"), dict):
                await answer("درخواست اجرا پیدا نشد. دوباره ابزار رو انتخاب کن.", show_alert=True)
                return
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            item = config.get("connectors", {}).get(connector_id)
            if not item:
                await answer("اتصال پیدا نشد.", show_alert=True)
                return
            await query.edit_message_text(f"⏳ در حال اجرای «{draft['tool_name']}»...")
            try:
                output = await asyncio.to_thread(
                    call_mcp_tool, item, draft["tool_name"], draft["arguments"], draft.get("input_schema", {})
                )
                context.user_data.pop("connector_tool_draft", None)
                context.user_data.pop("pending", None)
                await query.edit_message_text(f"✅ نتیجه ابزار «{draft['tool_name']}»\n{output}")
            except Exception as error:
                await query.edit_message_text(
                    f"⚠️ اجرای ابزار ناموفق بود | {type(error).__name__}: {error}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("↩️ بازگشت به ابزارها", callback_data=f"conn:tools:{connector_id}")
                    ]]),
                )
            return

        if data.startswith("conn:test:"):
            if not is_owner(query.from_user) or query.message.chat.type != "private":
                await answer("دسترسی مجاز نیست.", show_alert=True)
                return
            connector_id = data.split(":", 2)[2]
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            item = config.get("connectors", {}).get(connector_id)
            if not item:
                await answer("اتصال پیدا نشد.", show_alert=True)
                return
            await query.edit_message_text("🧪 در حال بررسی امن endpoint...")
            try:
                if item.get("type") in ("mcp", "auto"):
                    try:
                        tools = await asyncio.to_thread(connect_mcp_and_list_tools, item)
                        ok, detail = True, f"ارتباط MCP برقرار شد و {len(tools)} ابزار شناسایی شد"
                    except Exception:
                        if item.get("type") == "mcp":
                            raise
                        ok, detail = await asyncio.to_thread(test_connector_endpoint, item)
                        detail = "MCP شناسایی نشد؛ endpoint پاسخ HTTP داد. " + detail
                else:
                    ok, detail = await asyncio.to_thread(test_connector_endpoint, item)
                await query.edit_message_text(
                    f"{'✅' if ok else '⚠️'} نتیجه آزمایش «{item.get('name', 'اتصال')}» | {detail} | هیچ ابزاری اجرا نمیشه",
                    reply_markup=connector_item_menu(connector_id),
                )
            except Exception as error:
                await query.edit_message_text(
                    f"⚠️ آزمایش ناموفق بود | {type(error).__name__}: {error}",
                    reply_markup=connector_item_menu(connector_id),
                )
            return

        if data.startswith("conn:key:"):
            if not is_owner(query.from_user) or query.message.chat.type != "private":
                await answer("دسترسی مجاز نیست.", show_alert=True)
                return
            connector_id = data.split(":", 2)[2]
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            if connector_id not in config.get("connectors", {}):
                await answer("اتصال پیدا نشد.", show_alert=True)
                return
            context.user_data["connector_key_target"] = connector_id
            context.user_data["pending"] = "connector_api_key"
            await query.edit_message_text("کلید API رو بفرست. ربات اول تلاش میکنه پیام حاوی کلید رو حذف کنه و فقط بعد از حذف موفق، کلید رو ذخیره میکنه. برای پاک کردن کلید فقط - بفرست")
            return

        if data.startswith("conn:delete:"):
            if not is_owner(query.from_user):
                await answer("دسترسی مجاز نیست.", show_alert=True)
                return
            connector_id = data.split(":", 2)[2]
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            old = json.loads(json.dumps(config))
            config.setdefault("connectors", {}).pop(connector_id, None)
            try:
                await persist_ai_config(config, context)
                await query.edit_message_text(connector_overview(config), reply_markup=connector_menu(config))
            except Exception as error:
                context.application.bot_data["ai_config"] = old
                await query.edit_message_text(f"ذخیره حذف ناموفق بود: {type(error).__name__}", reply_markup=connector_menu(old))
            return

        if data == "main:ai_admin":
            if not is_owner(query.from_user):
                await answer("⛔ فقط مدیر اصلی به این بخش دسترسی داره.", show_alert=True)
                return
            context.user_data.pop("pending", None)
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            await query.edit_message_text(ai_admin_text(config), reply_markup=ai_admin_menu(config))
            return

        if data == "ai:back":
            context.user_data.pop("pending", None)
            await query.edit_message_text(
                home_text(is_owner(query.from_user)),
                reply_markup=main_menu(is_owner(query.from_user)),
            )
            return

        if data == "ai:admin":
            if not is_owner(query.from_user):
                await query.edit_message_text("⛔ فقط مدیر اصلی به این بخش دسترسی داره.")
                return
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            await query.edit_message_text(ai_admin_text(config), reply_markup=ai_admin_menu(config))
            return

        if data == "ai:add":
            if not is_owner(query.from_user):
                await query.edit_message_text("⛔ فقط مدیر اصلی.")
                return
            context.user_data["ai_service_draft"] = {"editing_id": None}
            context.user_data["pending"] = "ai_admin_name"
            await query.edit_message_text(ai_wizard_prompt("name"), reply_markup=cancel_menu())
            return

        if data.startswith("ai:service:"):
            if not is_owner(query.from_user):
                return
            service_id = data.split(":", 2)[2]
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            service = config.get("services", {}).get(service_id)
            if not service:
                await query.edit_message_text("این سرویس پیدا نشد.")
                return
            await api_async("editMessageText", {
                "chat_id": query.message.chat_id,
                "message_id": query.message.message_id,
                "rich_message": ai_service_rich_message(
                    service,
                    config.get("default_service") == service_id,
                ),
                "reply_markup": ai_service_menu(service_id).to_dict(),
            })
            return

        if data.startswith("ai:edit:"):
            if not is_owner(query.from_user):
                return
            service_id = data.split(":", 2)[2]
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            service = config.get("services", {}).get(service_id)
            if not service:
                await query.edit_message_text("این سرویس پیدا نشد.")
                return
            context.user_data["ai_service_draft"] = {
                "editing_id": service_id,
                "name": service.get("name", ""),
                "url": service.get("url", ""),
                "api_key": service.get("api_key", ""),
                "model": service.get("model", ""),
                "headers": service.get("headers", {}),
            }
            context.user_data["pending"] = "ai_admin_name"
            await query.edit_message_text(
                "✏️ ویرایش سرویس\n\nاسم جدید سرویس رو بفرست",
                reply_markup=cancel_menu(),
            )
            return

        if data.startswith("ai:default:"):
            if not is_owner(query.from_user):
                return
            service_id = data.split(":", 2)[2]
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            if service_id not in config.get("services", {}):
                await query.edit_message_text("این سرویس پیدا نشد.")
                return
            old = config.get("default_service")
            config["default_service"] = service_id
            try:
                await persist_ai_config(config, context)
                await query.edit_message_text("⭐ سرویس پیش فرض تغییر کرد\n\n" + ai_admin_text(config), reply_markup=ai_admin_menu(config))
            except Exception as error:
                config["default_service"] = old
                await query.edit_message_text(f"⚠️ ذخیره تغییر ناموفق بود: {error}", reply_markup=ai_admin_menu(config))
            return

        if data.startswith("ai:delete:"):
            if not is_owner(query.from_user):
                return
            service_id = data.split(":", 2)[2]
            config = context.application.bot_data.get("ai_config") or ai_default_config()
            if service_id not in config.get("services", {}):
                await query.edit_message_text("این سرویس پیدا نشد.")
                return
            old_config = json.loads(json.dumps(config))
            config["services"].pop(service_id, None)
            if config.get("default_service") == service_id:
                config["default_service"] = next(iter(config["services"]), None)
            try:
                await persist_ai_config(config, context)
                await query.edit_message_text("🗑 سرویس حذف شد\n\n" + ai_admin_text(config), reply_markup=ai_admin_menu(config))
            except Exception as error:
                context.application.bot_data["ai_config"] = old_config
                await query.edit_message_text(f"⚠️ حذف سرویس ذخیره نشد: {error}", reply_markup=ai_admin_menu(old_config))
            return

        if data == "ai:test_save":
            if not is_owner(query.from_user):
                return
            draft = context.user_data.get("ai_service_draft")
            if not draft:
                await answer("اطلاعات سرویس پیدا نشد.", show_alert=True)
                return
            try:
                await query.edit_message_text("🔌 در حال آزمایش اتصال...")
                await request_ai(draft, [{"role": "user", "content": "فقط بنویس: اتصال برقرار است"}])
                config = context.application.bot_data.get("ai_config") or ai_default_config()
                service_id = draft.get("editing_id") or str(int(time.time() * 1000))
                config.setdefault("services", {})[service_id] = {
                    "name": draft["name"],
                    "url": draft["url"],
                    "api_key": draft.get("api_key", ""),
                    "model": draft["model"],
                    "headers": draft.get("headers", {}),
                }
                if not config.get("default_service"):
                    config["default_service"] = service_id
                await persist_ai_config(config, context)
                context.user_data.pop("ai_service_draft", None)
                await query.edit_message_text(
                    "✅ اتصال موفق بود و سرویس به صورت رمزگذاری‌شده ذخیره شد\n\n" + ai_admin_text(config),
                    reply_markup=ai_admin_menu(config),
                )
            except Exception as error:
                await query.edit_message_text(
                    f"⚠️ اتصال یا ذخیره ناموفق بود\n{type(error).__name__}: {error}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔁 تلاش دوباره", callback_data="ai:test_save")],
                        [InlineKeyboardButton("❌ لغو", callback_data="ai:admin")],
                    ]),
                )
            return

        if data in ("rich:ai", "ai:rich"):
            context.user_data["widget_mode"] = False
            context.user_data["pending"] = "ai_rich"
            await query.edit_message_text(
                "🧱 توضیح بده چه ریچ مسیجی میخای ساخته بشه\n\n"
                "مثلا: یک معرفی محصول با تیتر، توضیحات، جدول مشخصات و دکمه خرید",
                reply_markup=cancel_menu(),
            )
            return

        if data == "rich:manual":
            await open_rich_editor(query, context)
            return

        if data == "rich:back":
            await query.edit_message_text(
                home_text(is_owner(query.from_user)),
                reply_markup=main_menu(is_owner(query.from_user)),
            )
            return

        if data == "widget:ai":
            context.user_data["widget_mode"] = False
            context.user_data["pending"] = "ai_widget"
            await query.edit_message_text(
                "🧩 توضیح بده چه ویجتی میخای ساخته بشه\n\n"
                "مثلا: یک ماشین حساب فارسی با طراحی مدرن و دکمه های تعاملی",
                reply_markup=cancel_menu(),
            )
            return

        if data == "widget:manual":
            await widget_mode_message(query.message, context)
            return

        if data == "widget:back":
            context.user_data.pop("pending", None)
            await query.edit_message_text(
                home_text(is_owner(query.from_user)),
                reply_markup=main_menu(is_owner(query.from_user)),
            )
            return

        if data == "ai:text":
            context.user_data["pending"] = "ai_text"
            await query.edit_message_text("✍️ متن مورد نظرت رو توضیح بده یا متن فعلی رو بفرست و بگو چطور ویرایشش کنم")
            return

        if data == "ai:code":
            context.user_data["pending"] = "ai_code"
            await query.edit_message_text("💻 توضیح بده چه کدی لازم داری\n\nزبان برنامه نویسی و کاری که کد باید انجام بده رو هم بگو")
            return

        if data == "ai:chat":
            context.user_data["ai_chat_mode"] = True
            context.user_data["ai_chat_history"] = []
            context.user_data.pop("ai_chat_pending_edit", None)
            context.user_data.pop("pending", None)
            await query.edit_message_text(
                "💬 دستیار هوشمند فعاله\nمیتونی سوال بپرسی یا مستقیم بگی چه تغییری روی پیش نویس انجام بدم\n\nقبل از اعمال هر تغییر، تاییدش رو ازت میگیرم",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 پایان گفتگو", callback_data="ai:chat_end")]]),
            )
            return

        if data == "ai:chat_apply":
            pending_edit = context.user_data.get("ai_chat_pending_edit")
            if not pending_edit:
                await query.edit_message_text("این تغییر پیشنهادی منقضی شده.")
                return
            current = context.user_data.get("blocks", [])
            if current != pending_edit.get("base"):
                context.user_data.pop("ai_chat_pending_edit", None)
                await query.edit_message_text("پیش نویس بعد از پیشنهاد تغییر کرده. برای جلوگیری از حذف تغییراتت، دوباره درخواستت رو بفرست.")
                return
            context.user_data["blocks"] = pending_edit["blocks"]
            context.user_data.pop("ai_chat_pending_edit", None)
            await query.edit_message_text("✅ تغییرات تایید و روی پیش نویس اعمال شد.")
            await send_editor_reply(query.message, context)
            return

        if data == "ai:chat_cancel":
            context.user_data.pop("ai_chat_pending_edit", None)
            await query.edit_message_text("❌ تغییرات اعمال نشد.")
            return

        if data == "ai:chat_end":
            context.user_data.pop("ai_chat_mode", None)
            context.user_data.pop("ai_chat_history", None)
            context.user_data.pop("ai_chat_pending_edit", None)
            await query.edit_message_text("گفتگو پایان یافت", reply_markup=ai_main_menu(is_owner(query.from_user)))
            return

        if data == "ai:code_rich":
            code = context.user_data.get("ai_code_result")
            if not code:
                await query.edit_message_text("کدی برای ثبت پیدا نشد.")
                return
            context.user_data["blocks"] = [{"type": "pre", "text": code}]
            await query.edit_message_text("✅ کد به ویرایشگر ریچ اضافه شد")
            await send_editor_reply(query.message, context)
            return

        if data == "ai:code_parts":
            code = context.user_data.get("ai_code_result")
            if not code:
                await query.edit_message_text("کدی برای تبدیل پیدا نشد.")
                return
            chunks = [code[i:i + 3500] for i in range(0, len(code), 3500)]
            context.user_data["multipart"] = {
                "chunks": [{"text": chunk, "has_entities": False, "blocks": markdown_text_to_blocks(chunk)} for chunk in chunks],
                "characters": len(code),
            }
            context.user_data["pending"] = "parts"
            await query.edit_message_text(
                f"🧩 کد به {len(chunks)} قطعه تبدیل شد\nحالا میتونی به صورت یک بلوک کد ثبتش کنی",
                reply_markup=parts_menu(),
            )
            return

        if data == "main:home":
            context.user_data.pop("widget_mode", None)
            context.user_data.pop("ai_chat_mode", None)
            context.user_data.pop("pending", None)
            await query.edit_message_text(
                home_text(is_owner(query.from_user)),
                reply_markup=main_menu(is_owner(query.from_user)),
            )
            return

        if data == "main:rich":
            context.user_data.pop("pending", None)
            context.user_data["widget_mode"] = False
            await query.edit_message_text(
                "🧱 روش ساخت ریچ مسیج رو انتخاب کن",
                reply_markup=rich_creation_menu(),
            )
            return

        if data == "main:widget":
            context.user_data.pop("pending", None)
            context.user_data["widget_mode"] = False
            await query.edit_message_text(
                "🧩 روش ساخت ویجت رو انتخاب کن",
                reply_markup=widget_creation_menu(),
            )
            return

        if data == "access:menu":
            if not is_owner(query.from_user):
                await query.edit_message_text("⛔ فقط مدیر اصلی می‌تواند دسترسی‌ها را مدیریت کند.")
                return
            users = context.bot_data.setdefault("authorized_users", set())
            await query.edit_message_text(access_menu_text(users), reply_markup=access_menu_markup(users))
            return

        if data == "access:grant":
            if not is_owner(query.from_user):
                await query.edit_message_text("⛔ فقط مدیر اصلی می‌تواند دسترسی بدهد.")
                return
            context.user_data["pending"] = "access_grant"
            await query.edit_message_text(
                "➕ <b>افزودن کاربر</b>\n\nشناسه عددی تلگرام کاربر را همین‌جا بفرست.\n\n"
                "💡 کاربر می‌تواند در چت با ربات دستور /myid را بزند و شناسه‌اش را ببیند.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ برگشت", callback_data="access:menu")]])
            )
            return

        if data.startswith("access:revoke:"):
            if not is_owner(query.from_user):
                await query.edit_message_text("⛔ فقط مدیر اصلی می‌تواند دسترسی قطع کند.")
                return
            try:
                user_id = int(data.rsplit(":", 1)[1])
            except ValueError:
                await query.edit_message_text("شناسه نامعتبر است.", reply_markup=access_menu_markup(context.bot_data.setdefault("authorized_users", set())))
                return
            users = context.bot_data.setdefault("authorized_users", set())
            if user_id not in users:
                await query.edit_message_text("این کاربر از قبل حذف شده است.\n\n" + access_menu_text(users), reply_markup=access_menu_markup(users))
                return
            users.remove(user_id)
            try:
                await persist_access_users(users, context)
            except Exception:
                users.add(user_id)
                await query.edit_message_text("⚠️ ذخیره تغییر ناموفق بود؛ دسترسی قبلی حفظ شد.\n\n" + access_menu_text(users), reply_markup=access_menu_markup(users))
                return
            await query.edit_message_text("🚫 دسترسی قطع شد.\n\n" + access_menu_text(users), reply_markup=access_menu_markup(users))
            return

        if data == "access:back":
            await show_editor(query, context)
            return

        if data == "access:noop":
            await query.edit_message_text(access_menu_text(context.bot_data.setdefault("authorized_users", set())), reply_markup=access_menu_markup(context.bot_data.setdefault("authorized_users", set())))
            return

        if data == "media:single":
            context.user_data["pending"] = "media"
            await query.edit_message_text(
                prompt_text("media"),
                parse_mode="HTML",
                reply_markup=cancel_menu(),
            )
            return

        if data == "media:collage":
            context.user_data["pending"] = "collage"
            context.user_data["collection"] = {"type": "collage", "blocks": []}
            await query.edit_message_text(
                prompt_text("collage"),
                parse_mode="HTML",
                reply_markup=collection_menu(),
            )
            return

        if data == "media:slideshow":
            context.user_data["pending"] = "slideshow"
            context.user_data["collection"] = {"type": "slideshow", "blocks": []}
            await query.edit_message_text(
                prompt_text("slideshow"),
                parse_mode="HTML",
                reply_markup=collection_menu(),
            )
            return

        if data.startswith("src_show:"):
            source_id = data.split(":", 1)[1]
            source_text = get_source_texts(context).get(source_id)
            if not source_text:
                await answer("این منبع دیگر در دسترس نیست.", show_alert=True)
                return
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"📚 منبع\n\n{source_text}",
            )
            return

        if data.startswith("anchor_target:"):
            try:
                visible_index = int(data.split(":", 1)[1])
            except ValueError:
                await answer("مقصد نامعتبر است.", show_alert=True)
                return
            target_indexes = context.user_data.get("anchor_visible", [])
            if not 0 <= visible_index < len(target_indexes):
                await answer("این متن دیگر وجود ندارد.", show_alert=True)
                return
            target_index = target_indexes[visible_index]
            blocks = context.user_data.setdefault("blocks", [])
            if not 0 <= target_index < len(blocks):
                await answer("این متن دیگر وجود ندارد.", show_alert=True)
                return
            context.user_data["pending"] = f"a_target:{target_index}"
            await query.edit_message_text(
                "🔗 <b>عبارت دقیق مقصد را بفرست</b>\n\nمثال: <code>مشخصات فنی محصول</code>\n\n💡 فقط همان تکه‌ای از متن را بفرست که می‌خواهی لینک دقیقاً روی آن قرار بگیرد.",
                parse_mode="HTML",
                reply_markup=cancel_menu(),
            )
            return

        if data.startswith("heading:"):
            try:
                size = int(data.split(":", 1)[1])
            except ValueError:
                await answer("اندازه تیتر نامعتبر است.", show_alert=True)
                return
            if not 1 <= size <= 6:
                await answer("اندازه تیتر باید بین 1 تا 6 باشد.", show_alert=True)
                return

            context.user_data["pending"] = f"h:{size}"
            keyboard = {
                "inline_keyboard": [[
                    {"text": "1", "callback_data": "heading:1", **({"style": "success"} if size == 1 else {})},
                    {"text": "2", "callback_data": "heading:2", **({"style": "success"} if size == 2 else {})},
                    {"text": "3", "callback_data": "heading:3", **({"style": "success"} if size == 3 else {})},
                    {"text": "4", "callback_data": "heading:4", **({"style": "success"} if size == 4 else {})},
                    {"text": "5", "callback_data": "heading:5", **({"style": "success"} if size == 5 else {})},
                    {"text": "6", "callback_data": "heading:6", **({"style": "success"} if size == 6 else {})},
                ], [
                    {"text": "❌ لغو", "callback_data": "cancel"},
                ]]
            }
            try:
                await api_async("editMessageText", {
                    "chat_id": update.effective_chat.id,
                    "message_id": query.message.message_id,
                    "rich_message": {
                        "blocks": [
                            {
                                "type": "paragraph",
                                "text": f"🔠 اندازه انتخاب‌شده: {size}\nحالا متن تیتر را بفرست."
                            },
                            {
                                "type": "heading",
                                "text": f"این متن نمایشی است که با تیتر شماره {size} نوشته شده است.",
                                "size": size,
                            },
                        ],
                        "is_rtl": True,
                    },
                    "reply_markup": keyboard,
                })
            except Exception as e:
                await query.edit_message_text(
                    f"⚠️ پیش‌نمایش تیتر نشد: {e}",
                    reply_markup=heading_menu(),
                )
            return

        if data == "add:lists":
            context.user_data.pop("pending", None)
            await query.edit_message_text(
                "📋 <b>انتخاب نوع لیست</b>\n\nیکی از سه نوع لیست را انتخاب کن:",
                parse_mode="HTML",
                reply_markup=list_menu(),
            )
            return

        if data.startswith("list:"):
            list_type = data.split(":", 1)[1]
            if list_type not in ("l", "x", "ol"):
                await answer("نوع لیست نامعتبر است.", show_alert=True)
                return
            context.user_data["pending"] = list_type
            await query.edit_message_text(
                prompt_text(list_type),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("↩️ برگشت", callback_data="add:lists")],
                ]),
            )
            return

        if data == "parts:cancel":
            context.user_data.pop("pending", None)
            context.user_data.pop("multipart", None)
            await show_editor(query, context)
            return

        if data in ("parts:finish_text", "parts:finish_code"):
            draft = context.user_data.get("multipart") or {"chunks": [], "characters": 0}
            chunks = draft.get("chunks", [])
            if not chunks:
                await answer("هنوز هیچ قطعه‌ای دریافت نشده.", show_alert=True)
                return
            blocks = context.user_data.setdefault("blocks", [])
            if data == "parts:finish_code":
                combined = "".join(item.get("text", "") for item in chunks)
                if not combined:
                    await answer("کد خالیه.", show_alert=True)
                    return
                if len(blocks) >= MAX_RICH_BLOCKS:
                    await answer("Rich Message حداکثر 500 بلوک دارد.", show_alert=True)
                    return
                blocks.append({"type": "pre", "text": combined})
            else:
                if all(not item.get("has_entities") for item in chunks):
                    combined = "".join(item.get("text", "") for item in chunks)
                    new_blocks = markdown_text_to_blocks(combined)
                else:
                    new_blocks = [block for item in chunks for block in item.get("blocks", [])]
                if len(blocks) + len(new_blocks) > MAX_RICH_BLOCKS:
                    await answer("تعداد بلوک‌ها از سقف 500 بیشتر میشه.", show_alert=True)
                    return
                blocks.extend(new_blocks)
            context.user_data.pop("multipart", None)
            context.user_data.pop("pending", None)
            await show_editor(query, context)
            return

        if data == "cancel":
            cancelled_mode = context.user_data.pop("pending", None)
            context.user_data.pop("multipart", None)
            context.user_data.pop("btn_draft", None)
            context.user_data.pop("btn_current", None)
            context.user_data.pop("btn_wizard", None)
            context.user_data.pop("btn_wizard_type", None)
            context.user_data.pop("btn_wizard_label", None)
            context.user_data.pop("btn_wizard_style", None)
            context.user_data.pop("collection", None)
            if cancelled_mode == "ai_widget":
                await query.edit_message_text(
                    "🧩 روش ساخت ویجت رو انتخاب کن",
                    reply_markup=widget_creation_menu(),
                )
            elif cancelled_mode == "ai_rich":
                await query.edit_message_text(
                    "🧱 روش ساخت ریچ مسیج رو انتخاب کن",
                    reply_markup=rich_creation_menu(),
                )
            elif isinstance(cancelled_mode, str) and cancelled_mode.startswith("ai_admin_"):
                config = context.application.bot_data.get("ai_config") or ai_default_config()
                await query.edit_message_text(ai_admin_text(config), reply_markup=ai_admin_menu(config))
            else:
                await show_editor(query, context)
            return
        if data.startswith("checklist:"):
            draft = context.user_data.get("checklist_draft")
            if not draft:
                await show_editor(query, context)
                return
            if data.startswith("checklist:toggle:"):
                try:
                    index = int(data.rsplit(":", 1)[1])
                except ValueError:
                    return
                items = draft.get("items", [])
                if not 0 <= index < len(items):
                    return
                items[index]["is_checked"] = not items[index].get("is_checked", False)
                await query.edit_message_text(
                    checklist_text(draft),
                    reply_markup=checklist_menu(draft),
                    parse_mode="HTML",
                )
                return
            if data == "checklist:done":
                add_block(context, draft)
                context.user_data.pop("checklist_draft", None)
                context.user_data.pop("pending", None)
                await show_editor(query, context)
                return
            if data == "checklist:cancel":
                context.user_data.pop("checklist_draft", None)
                context.user_data.pop("pending", None)
                await show_editor(query, context)
                return

        if data == "preview":
            blocks = sanitize_rich_blocks(context.user_data.get("blocks", []))
            context.user_data["blocks"] = blocks
            if not blocks:
                await answer("⚠️ هنوز هیچ قابلیتی اضافه نکردی.", show_alert=True)
                return
            try:
                if context.user_data.get("control_widget"):
                    await api_async("sendRichMessage", {
                        "chat_id": update.effective_chat.id,
                        "rich_message": {"blocks": blocks, "is_rtl": True},
                    })
                    await control_widget_set_screen(
                        context,
                        "👀 پیش‌نمایش به‌صورت یک پیام جداگانه ارسال شد",
                        InlineKeyboardMarkup([[
                            InlineKeyboardButton("↩️ برگشت به ویرایشگر", callback_data="back:editor")
                        ]]),
                    )
                else:
                    await api_async("editMessageText", {
                        "chat_id": update.effective_chat.id,
                        "message_id": query.message.message_id,
                        "rich_message": {"blocks": blocks, "is_rtl": True},
                        "reply_markup": {
                            "inline_keyboard": [[
                                {"text": "↩️ برگشت به ویرایشگر", "callback_data": "back:editor"}
                            ]]
                        },
                    })
            except Exception as e:
                await answer(f"پیش‌نمایش ناموفق بود: {e}", show_alert=True)
            return
        if data == "back:editor":
            await show_editor(query, context)
            return
        if data == "clear":
            blocks = context.user_data.setdefault("blocks", [])
            if not blocks:
                await answer("⚠️ چیزی برای حذف وجود ندارد.", show_alert=True)
                return
            blocks.pop()
            context.user_data.pop("pending", None)
            context.user_data.pop("collection", None)
            await show_editor(query, context)
            return
        if data == "collection:more":
            collection = context.user_data.get("collection")
            if not collection:
                await answer("مجموعه‌ای در حال ساخت نیست.", show_alert=True)
                return
            context.user_data["pending"] = collection["type"]
            await query.edit_message_text(
                f"➕ مدیای بعدی را بفرست.\nتعداد فعلی: {len(collection['blocks'])}\n\n"
                f"مثال: یک عکس، ویدیو یا GIF ارسال کن.",
                reply_markup=collection_menu(),
            )
            return
        if data == "collection:finish":
            collection = context.user_data.get("collection")
            if not collection or not collection["blocks"]:
                await answer("حداقل یک مدیا اضافه کن.", show_alert=True)
                return
            blocks = context.user_data.setdefault("blocks", [])
            add_block(context, {"type": collection["type"], "blocks": collection["blocks"]})
            context.user_data.pop("collection", None)
            context.user_data.pop("pending", None)
            await show_editor(query, context)
            return
        if data == "collection:cancel":
            context.user_data.pop("collection", None)
            context.user_data.pop("pending", None)
            await show_editor(query, context)
            return
        if data == "send":
            blocks = context.user_data.get("blocks", [])
            if not blocks:
                await answer("اول حداقل یک قابلیت اضافه کن.", show_alert=True)
                return
            if len(blocks) > MAX_RICH_BLOCKS:
                await answer("Rich Message حداکثر 500 بلوک دارد.", show_alert=True)
                return
            if media_attachment_count(blocks) > MAX_MEDIA_ATTACHMENTS:
                await answer("Rich Message حداکثر 50 مدیا دارد.", show_alert=True)
                return
            try:
                # Draft streaming is for text-only messages. Telegram does not support
                # direct upload of new files in sendRichMessageDraft.
                media_types = {"photo", "video", "animation", "audio", "voice_note", "document", "collage", "slideshow"}
                has_media = any(block.get("type") in media_types for block in blocks)

                if update.effective_chat.type == "private" and not has_media:
                    # Draft streaming is optional. A draft API failure must never block
                    # the final persistent Rich Message send.
                    try:
                        draft_id = (context.user_data.get("_draft_id", 0) + 1) % 2147483647
                        if draft_id == 0:
                            draft_id = 1
                        context.user_data["_draft_id"] = draft_id

                        await api_async("sendRichMessageDraft", {
                            "chat_id": update.effective_chat.id,
                            "draft_id": draft_id,
                            "rich_message": {
                                "blocks": [{
                                    "type": "thinking",
                                    "text": "🧠 در حال آماده‌سازی پیام..."
                                }],
                                "is_rtl": True,
                            },
                        })
                        await asyncio.sleep(0.7)

                        for index in range(1, len(blocks) + 1):
                            partial = blocks[:index]
                            if not any(block.get("type") != "anchor" for block in partial):
                                continue
                            await api_async("sendRichMessageDraft", {
                                "chat_id": update.effective_chat.id,
                                "draft_id": draft_id,
                                "rich_message": {
                                    "blocks": partial,
                                    "is_rtl": True,
                                },
                            })
                            if index < len(blocks):
                                await asyncio.sleep(0.18)
                    except Exception as draft_error:
                        print(
                            f"RICH MESSAGE DRAFT FAILED; continuing with final send: {type(draft_error).__name__}: {draft_error}",
                            flush=True,
                        )

                blocks = sanitize_rich_blocks(blocks)
                await api_async("sendRichMessage", {
                    "chat_id": update.effective_chat.id,
                    "rich_message": {"blocks": blocks, "is_rtl": True},
                })
                context.user_data["blocks"] = []
                context.user_data.pop("pending", None)
                context.user_data.pop("collection", None)
                context.user_data.pop("ai_rich_draft", None)
                try:
                    await show_editor(query, context)
                except Exception as editor_error:
                    # The Rich Message is already sent; an editor refresh error
                    # must not be reported as a failed send.
                    print(
                        f"EDITOR REFRESH FAILED AFTER SUCCESSFUL SEND: {type(editor_error).__name__}: {editor_error}",
                        flush=True,
                    )
            except Exception as e:
                await query.edit_message_text(f"⚠️ ارسال ناموفق بود:\n{e}")
            return
        if data.startswith("add:"):
            await choose_add(update, context)
            return

        # Rich Message buttons can send arbitrary callback_data.
        # Do not leave the callback query unanswered when it is not a bot-editor action.
        await answer("دکمه فشرده شد", show_alert=False)


    finally:
        try:
            await answer()
        except Exception as callback_error:
            print(f"CALLBACK ACK FAILED: {type(callback_error).__name__}: {callback_error}", flush=True)


# A separate widget-based control surface. The legacy /start menu and callbacks
# remain available and unchanged; this surface reuses the same callback router.
CONTROL_WIDGET_HTML = """<!doctype html>
<html lang="fa" dir="rtl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"><style>
:root{color-scheme:light dark;--bg:var(--tg-theme-bg-color,#10131d);--panel:var(--tg-theme-secondary-bg-color,#191e2b);--fg:var(--tg-theme-text-color,#f5f7ff);--muted:var(--tg-theme-hint-color,#9ca6ba);--a:var(--tg-theme-button-color,#7067ed);--line:color-mix(in srgb,var(--muted) 22%,transparent)}
*{box-sizing:border-box}body{margin:0;padding:12px;background:var(--bg);color:var(--fg);font:inherit}.wrap{max-width:720px;margin:auto}.hero{padding:17px;border-radius:20px;background:linear-gradient(135deg,color-mix(in srgb,var(--a) 25%,var(--panel)),var(--panel));border:1px solid var(--line)}.brand{font-size:10px;letter-spacing:1.5px;color:var(--muted)}h1{font-size:18px;margin:5px 0}#sub{font-size:12px;line-height:1.8;color:var(--muted);white-space:pre-wrap}.head{display:flex;justify-content:space-between;align-items:center;margin:18px 2px 10px;font-size:13px}#tools{display:flex;gap:6px;align-items:center}#tools button{border:1px solid var(--line);border-radius:18px;padding:6px 9px;background:var(--panel);color:var(--muted);font:inherit;font-size:10px}.force-light{--bg:#f4f6fb;--panel:#fff;--fg:#171923;--muted:#687083;--a:#6558d3;--line:rgba(35,40,55,.14);color-scheme:light}.force-dark{--bg:#10131d;--panel:#191e2b;--fg:#f5f7ff;--muted:#9ca6ba;--a:#938aff;--line:rgba(180,190,215,.2);color-scheme:dark}.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.tile{display:flex;flex-direction:column;gap:7px;text-align:right;min-height:94px;padding:12px;border:1px solid var(--line);border-radius:16px;background:var(--panel);color:var(--fg);font:inherit}.tile:active{transform:scale(.98)}.tile:disabled{opacity:.5}.top{display:flex;justify-content:space-between;font-size:19px}.name{font-size:13px;font-weight:800}.desc{font-size:10px;line-height:1.5;color:var(--muted)}.primary{border-color:color-mix(in srgb,var(--a) 50%,transparent);background:color-mix(in srgb,var(--a) 14%,var(--panel))}.compact .tile{min-height:65px;padding:9px;gap:4px}.compact .desc{display:none}#status{text-align:center;min-height:22px;padding:8px 2px;color:var(--muted);font-size:11px}#status.error{color:var(--tg-theme-destructive-text-color,#e66)}
</style></head><body><main class="wrap" id="wrap"><header class="hero"><div class="brand">TELEGRAM JOURNAL</div><h1>فضای ساخت و مدیریت محتوا</h1><div id="sub">ابزارهای ساخت محتوای پیشرفته، همه در یک جا</div></header><div class="head"><b id="heading">از کجا شروع کنیم؟</b><div id="tools"><button id="theme" type="button">تم: خودکار</button><button id="haptic" type="button">لرزش: روشن</button><button id="density" type="button">فشرده</button></div></div><div class="grid" id="buttons"></div><div id="status" role="status" aria-live="polite"></div></main>
<script>(function(){'use strict';const W=window.TelegramWidget,$=id=>document.getElementById(id),wrap=$('wrap'),sub=$('sub'),buttons=$('buttons'),status=$('status'),heading=$('heading'),density=$('density'),themeBtn=$('theme'),hapticBtn=$('haptic');let busy=false,last='',themeMode='auto',haptics=true;try{themeMode=localStorage.getItem('tj_widget_theme')||'auto';haptics=localStorage.getItem('tj_widget_haptics')!=='0'}catch(e){}function buzz(method,arg){if(!haptics||!W.HapticFeedback)return;try{W.HapticFeedback[method](arg)}catch(e){}}function setTheme(v){themeMode=v;document.documentElement.classList.toggle('force-light',v==='light');document.documentElement.classList.toggle('force-dark',v==='dark');themeBtn.textContent='تم: '+({auto:'خودکار',light:'روشن',dark:'تاریک'}[v]);try{localStorage.setItem('tj_widget_theme',v)}catch(e){}}function setHaptics(v){haptics=v;hapticBtn.textContent='لرزش: '+(v?'روشن':'خاموش');try{localStorage.setItem('tj_widget_haptics',v?'1':'0')}catch(e){}}const presets={'main:rich':['🧱','ریچ مسیج','ساخت پیام های حرفه ای با متن و رسانه',1],'main:widget':['🧩','ویجت','ساخت ابزارهای تعاملی',1],'main:ai_admin':['🧠','مدیریت هوش مصنوعی','تنظیم سرویس ها و مدل ها',0],'access:menu':['🔐','مدیریت دسترسی','کنترل کاربران مجاز',0],'rich:manual':['✍️','ساخت دستی','کنترل کامل روی بخش های پیام',0],'rich:ai':['✨','ساخت با هوش مصنوعی','تبدیل ایده به پیش نویس',1],'widget:ai':['✨','ساخت ویجت با هوش مصنوعی','تولید ویجت از توضیح متنی',1],'widget:manual':['🛠','ساخت دستی ویجت','طراحی و تنظیم دستی',0],'rich:back':['↩️','بازگشت','رفتن به صفحه قبل',0]};function info(x){let d=presets[x[1]];if(d)return d;let k=x[1]||'';return [k.startsWith('access:')?'🔐':k==='preview'?'👁️':k==='send'?'🚀':k==='cancel'||k.endsWith(':back')?'↩️':'⚡',x[0]||'گزینه',k==='preview'?'مشاهده نتیجه قبل از انتشار':k==='send'?'انتشار محتوای آماده':'برای ادامه انتخاب کن',k==='send'?1:0]}function msg(t,e){status.textContent=t||'';status.classList.toggle('error',!!e)}function render(){let s={};try{s=JSON.parse(W.chatData||'{}')}catch(e){}let items=Array.isArray(s.b)?s.b:[];sub.textContent=s.t||'ابزارهای ساخت محتوای پیشرفته، همه در یک جا';heading.textContent=items.some(x=>x[1]==='main:rich'||x[1]==='main:widget')?'ابزارهای اصلی':'گزینه های این بخش';buttons.textContent='';items.forEach(x=>{let d=info(x),b=document.createElement('button');b.type='button';b.className='tile'+(d[3]?' primary':'');b.disabled=busy;let top=document.createElement('div');top.className='top';top.innerHTML='<span></span><span>←</span>';top.firstChild.textContent=d[0];let name=document.createElement('div');name.className='name';name.textContent=d[1];let desc=document.createElement('div');desc.className='desc';desc.textContent=d[2];b.append(top,name,desc);b.onclick=()=>{if(busy)return;busy=true;render();msg('در حال اجرای درخواست...');buzz('impactOccurred','light');let p;try{p=W.request(x[1])}catch(e){busy=false;render();msg('خطا: '+(e.code||e.message||'FAILED'),true);return}p.then(a=>{busy=false;render();msg(a&&a.result||'درخواست به ربات رسید');buzz('notificationOccurred','success')}).catch(e=>{busy=false;render();let k=e&& (e.code||e.message)||'FAILED',m={BUSY:'یه درخواست دیگه در حال اجراست',RATE_LIMITED:'کمی صبر کن و دوباره بزن',TIMEOUT:'پاسخ ربات دیر رسید، دوباره امتحان کن',NOT_ALLOWED:'این گزینه قابل اجرا نیست',FAILED:'ارتباط با ربات برقرار نشد',INVALID_ANSWER:'پاسخ ربات قابل نمایش نبود'};msg(m[k]||'خطا: '+k,true);buzz('notificationOccurred','error')})};buttons.appendChild(b)});if(!items.length)buttons.textContent='در حال آماده سازی...'}function compact(v){wrap.classList.toggle('compact',v);density.textContent=v?'معمولی':'فشرده';try{localStorage.setItem('tj_widget_compact',v?'1':'0')}catch(e){}}density.onclick=()=>{compact(!wrap.classList.contains('compact'));buzz('selectionChanged')};themeBtn.onclick=()=>{let a=['auto','light','dark'];setTheme(a[(a.indexOf(themeMode)+1)%3]);buzz('selectionChanged')};hapticBtn.onclick=()=>{setHaptics(!haptics);if(haptics)buzz('selectionChanged')};setTheme(themeMode);setHaptics(haptics);try{compact(localStorage.getItem('tj_widget_compact')==='1')}catch(e){}if(W.viewer&&W.viewer.first_name)heading.textContent='خوش اومدی '+W.viewer.first_name;W.onEvent('chatDataChanged',()=>{let v=W.chatData||'';if(v!==last){last=v;msg('صفحه به روز شد')}render()});W.onEvent('themeChanged',render);last=W.chatData||'';render();try{W.setResizeAnchor&&W.setResizeAnchor('top');W.ready&&W.ready()}catch(e){}})();</script></body></html>"""

def control_widget_markup_state(text, reply_markup=None):
    plain = re.sub(r"<[^>]*>", "", str(text or ""))
    plain = html_lib.unescape(plain)
    buttons = []
    keyboard = getattr(reply_markup, "inline_keyboard", None) or []
    for row in keyboard:
        for button in row:
            data = getattr(button, "callback_data", None)
            label = str(getattr(button, "text", "") or "گزینه")[:42]
            if data:
                style = "secondary"
                api_kwargs = getattr(button, "api_kwargs", None) or {}
                if hasattr(api_kwargs, "get"):
                    style = api_kwargs.get("style", style)
                buttons.append([label, str(data), style])
    state = {"t": plain[:420], "b": buttons[:18]}
    packed = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    while len(packed.encode("utf-8")) > 1000 and state["b"]:
        state["b"].pop()
        packed = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    if len(packed.encode("utf-8")) > 1000:
        state["t"] = state["t"][:180]
        packed = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    return packed

async def control_widget_set_screen(context, text, reply_markup=None):
    session = context.user_data.get("control_widget")
    if not session:
        return False
    key = f"{session['chat_id']}:{session['message_id']}"
    entry = context.application.bot_data.setdefault("control_widgets", {}).get(key)
    if not entry or entry.get("user_id") != session.get("user_id"):
        return False
    packed = control_widget_markup_state(text, reply_markup)
    if packed == entry.get("chat_data"):
        return True
    await api_async(
        "editMessageWidgetData",
        {
            "chat_id": session["chat_id"],
            "message_id": session["message_id"],
            "chat_data": packed,
        },
        base_url=WIDGET_API_BASE,
        timeout=30,
    )
    entry["chat_data"] = packed
    return True

class ControlWidgetMessage:
    def __init__(self, context, chat_id, message_id, user):
        self._context = context
        self.chat_id = chat_id
        self.message_id = message_id
        self.from_user = user
        self.reply_markup = None

    async def reply_text(self, text, **kwargs):
        markup = kwargs.pop("reply_markup", None)
        if markup is None:
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 منوی اصلی", callback_data="main:home")
            ]])
        await control_widget_set_screen(self._context, text, markup)
        return None

    async def reply_document(self, document, **kwargs):
        return await self._context.bot.send_document(chat_id=self.chat_id, document=document, **kwargs)

class ControlWidgetQuery:
    def __init__(self, context, data, user, message):
        self._context = context
        self.data = data
        self.from_user = user
        self.message = message
        self.is_control_widget = True

    async def answer(self, *args, **kwargs):
        return True

    async def edit_message_text(self, text, **kwargs):
        markup = kwargs.pop("reply_markup", None)
        if markup is None:
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 منوی اصلی", callback_data="main:home")
            ]])
        await control_widget_set_screen(self._context, text, markup)
        return True

class ControlWidgetMessageProxy:
    def __init__(self, message, context):
        self._message = message
        self._context = context

    def __getattr__(self, name):
        return getattr(self._message, name)

    async def reply_text(self, text, **kwargs):
        markup = kwargs.pop("reply_markup", None)
        if markup is None:
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 منوی اصلی", callback_data="main:home")
            ]])
        await control_widget_set_screen(self._context, text, markup)
        return None

class ControlWidgetUpdateProxy:
    def __init__(self, update, context):
        self._update = update
        self.message = ControlWidgetMessageProxy(update.message, context)

    def __getattr__(self, name):
        return getattr(self._update, name)

def control_widget_update_proxy(update, context):
    return ControlWidgetUpdateProxy(update, context)

async def widget_menu_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or update.effective_chat.type != "private":
        if message:
            await message.reply_text("منوی ویجتی فعلا فقط داخل گفتگوی خصوصی با ربات کار می‌کنه.")
        return
    context.user_data["blocks"] = []
    context.user_data.pop("pending", None)
    context.user_data.pop("collection", None)
    context.user_data.pop("widget_mode", None)
    context.user_data.pop("ai_rich_draft", None)
    context.user_data["menu_expanded"] = False
    owner = is_owner(update.effective_user)
    markup = main_menu(owner, request_access=not is_authorized(context, update.effective_user),
                       user_id=update.effective_user.id)
    initial = control_widget_markup_state(home_text(owner), markup)
    validate_widget_html(CONTROL_WIDGET_HTML)
    try:
        sent = await asyncio.to_thread(
            api,
            "sendWidget",
            {
                "chat_id": message.chat_id,
                "html": CONTROL_WIDGET_HTML,
                "caption": "منوی جدید ربات",
                "chat_data": initial,
                "allow_queries": True,
            },
            base_url=WIDGET_API_BASE,
            timeout=60,
        )
    except Exception as error:
        await message.reply_text(f"⚠️ ساخت منوی ویجتی ناموفق بود: {error}")
        return
    message_id = sent.get("message_id")
    if not message_id:
        raise RuntimeError("تلگرام پیام ویجت منو رو برنگردوند")
    session = {
        "chat_id": message.chat_id,
        "message_id": message_id,
        "user_id": update.effective_user.id,
    }
    context.user_data["control_widget"] = session
    key = f"{message.chat_id}:{message_id}"
    registry = context.application.bot_data.setdefault("control_widgets", {})
    registry[key] = {"user_id": update.effective_user.id, "chat_data": initial}
    if len(registry) > 1500:
        for old_key in list(registry)[:500]:
            registry.pop(old_key, None)

async def widget_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"CONTROL WIDGET UPDATE RECEIVED: {list(update.to_dict().keys())}", flush=True)
    raw = (getattr(update, "api_kwargs", None) or {}).get("widget_query")
    shared_raw = (getattr(update, "api_kwargs", None) or {}).get("shared_widget_query")
    query = raw or shared_raw
    if not isinstance(query, dict):
        return
    query_id = query.get("id")
    if not query_id:
        return
    if raw and query.get("data") == "wallex:refresh":
        user_data = query.get("from") or {}
        chat = query.get("chat") or {}
        user_id = user_data.get("id")
        chat_id = chat.get("id")
        if not user_id or not chat_id or chat.get("type") != "private" or user_id != OWNER_ID:
            await api_async("answerWidgetQuery", {
                "widget_query_id": query_id,
                "data": json.dumps({"error": "دسترسی مجاز نیست"}, ensure_ascii=False),
            }, base_url=WIDGET_API_BASE, timeout=12)
            return
        try:
            balances, markets = await fetch_wallex_wallet_data()
            total, unpriced = wallex_total_value(balances, markets)
            data = {
                "total": (
                    "محاسبه کامل نیست" if unpriced
                    else wallex_format_amount(total, 0) + " تومان"
                ),
                "detail": (
                    "قیمت بعضی دارایی ها در بازار والکس در دسترس نیست"
                    if unpriced else "بر اساس آخرین قیمت های بازار والکس"
                ),
                "updated": datetime.now(timezone.utc).isoformat(),
                "complete": not bool(unpriced),
            }
        except Exception as error:
            data = {"error": f"دریافت اطلاعات ناموفق بود: {type(error).__name__}"}
        await api_async("answerWidgetQuery", {
            "widget_query_id": query_id,
            "data": json.dumps(data, ensure_ascii=False),
        }, base_url=WIDGET_API_BASE, timeout=12)
        return
    if shared_raw and not raw:
        await api_async("answerWidgetQuery", {
            "widget_query_id": query_id,
            "data": "این ویجت منوی تعاملی نیست",
        }, base_url=WIDGET_API_BASE, timeout=20)
        return
    user_data = query.get("from") or {}
    user_id = user_data.get("id")
    chat = query.get("chat") or {}
    chat_id = chat.get("id")
    message_id = query.get("message_id")
    if not user_id or not chat_id or not message_id or chat.get("type") != "private":
        await api_async("answerWidgetQuery", {
            "widget_query_id": query_id,
            "data": "درخواست معتبر نیست",
        }, base_url=WIDGET_API_BASE, timeout=20)
        return
    key = f"{chat_id}:{message_id}"
    entry = context.application.bot_data.setdefault("control_widgets", {}).get(key)
    if not entry or entry.get("user_id") != user_id:
        await api_async("answerWidgetQuery", {
            "widget_query_id": query_id,
            "data": "این منو منقضی شده، دوباره «م» رو بفرست",
        }, base_url=WIDGET_API_BASE, timeout=20)
        return
    user = SimpleNamespace(
        id=user_id,
        first_name=user_data.get("first_name", ""),
        last_name=user_data.get("last_name"),
        username=user_data.get("username"),
        is_bot=bool(user_data.get("is_bot", False)),
    )
    user_context = SimpleNamespace(
        application=context.application,
        bot=context.bot,
        bot_data=context.application.bot_data,
        user_data=(context.application.user_data.get(user_id) if context.application.user_data.get(user_id) is not None else context.application._user_data.setdefault(user_id, {})),
    )
    session = {
        "chat_id": chat_id,
        "message_id": message_id,
        "user_id": user_id,
    }
    user_context.user_data["control_widget"] = session
    message = ControlWidgetMessage(user_context, chat_id, message_id, user)
    fake_query = ControlWidgetQuery(user_context, query.get("data", ""), user, message)
    fake_update = SimpleNamespace(
        callback_query=fake_query,
        effective_chat=SimpleNamespace(id=chat_id, type="private"),
        effective_user=user,
    )
    # Acknowledge immediately so Telegram never times out the widget request.
    await api_async("answerWidgetQuery", {
        "widget_query_id": query_id,
        "data": "در حال انجام...",
    }, base_url=WIDGET_API_BASE, timeout=20)
    try:
        await callbacks(fake_update, user_context)
    except Exception as error:
        print(f"CONTROL WIDGET ACTION FAILED: {type(error).__name__}: {error}", flush=True)
        try:
            await control_widget_set_screen(
                user_context,
                f"⚠️ انجام عملیات ناموفق بود\n{type(error).__name__}: {error}",
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("🏠 منوی اصلی", callback_data="main:home")
                ]]),
            )
        except Exception as screen_error:
            print(f"CONTROL WIDGET ERROR SCREEN FAILED: {type(screen_error).__name__}: {screen_error}", flush=True)


def validate_widget_html(html):
    if not isinstance(html, str) or not html.strip():
        raise ValueError("فایل HTML خالیه.")
    if "\x00" in html:
        raise ValueError("کد HTML نباید کاراکتر NUL داشته باشه.")
    raw = html.encode("utf-8")
    if len(raw) > MAX_WIDGET_HTML_BYTES:
        raise ValueError("حجم HTML بیشتر از 32 کیلوبایته.")
    packed = zlib.compress(raw, 9)
    if len(packed) > MAX_WIDGET_COMPRESSED_BYTES:
        raise ValueError("حجم فشرده ویجت بیشتر از 4 کیلوبایته و تلگرام قبولش نمی‌کنه.")
    return html

async def send_html_widget(message, html, context):
    html = validate_widget_html(html)
    bot_data = context.application.bot_data
    if not bot_data.get("widget_archive_ready"):
        raise RuntimeError("آرشیو کانال آماده نیست؛ ویجت ارسال نشد تا لینک و کد آن گم نشود.")

    index = bot_data.get("widget_index") or empty_widget_index()
    existing_tokens = set(index.get("widgets", {}))
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    token = None
    for _ in range(200):
        candidate = "".join(secrets.choice(alphabet) for _ in range(3))
        if candidate not in existing_tokens:
            token = candidate
            break
    if token is None:
        raise RuntimeError("ساخت کد کوتاه یکتا ممکن نشد، دوباره تلاش کن.")
    archived = await context.bot.send_document(
        chat_id=ACCESS_CHANNEL_ID,
        message_thread_id=(bot_data.get("storage_topics") or {}).get("آرشیو ویجت ها"),
        document=InputFile(
            io.BytesIO(html.encode("utf-8")),
            filename=f"widget-{token}.html",
        ),
        caption=f"Widget source · {token}",
        disable_notification=True,
    )
    sent = await asyncio.to_thread(
        api,
        "sendWidget",
        {
            "chat_id": message.chat_id,
            "html": html,
            "is_shared": True,
        },
        base_url=WIDGET_API_BASE,
        timeout=90,
    )
    widget = sent.get("widget") or {}
    widget_id = widget.get("id")
    if not widget_id:
        raise RuntimeError("تلگرام ویجت را ارسال کرد اما شناسه ویجت اشتراکی برنگرداند.")

    await record_widget_message(
        context,
        token,
        message.chat_id,
        sent["message_id"],
        {
            "file_id": archived.document.file_id,
            "widget_id": str(widget_id),
            "created_at": int(time.time()),
            "owner_id": message.from_user.id if message.from_user else None,
        },
    )
    await message.reply_text("✅ ویجت ذخیره شد\nبرای گرفتن لینک یا کد، روی خود ویجت ریپلای کن و بنویس «لینک» یا «کد»")
    if context.user_data.get("control_widget"):
        owner = is_owner(message.from_user)
        await control_widget_set_screen(context, home_text(owner), main_menu(owner))
    return sent

async def send_shared_widget_token(message, context, token):
    bot_data = context.application.bot_data
    if not bot_data.get("widget_archive_ready"):
        await message.reply_text("⚠️ آرشیو ویجت فعلا در دسترس نیست.")
        return
    index = bot_data.get("widget_index") or empty_widget_index()
    entry = index.get("widgets", {}).get(token)
    if not entry:
        await message.reply_text("⚠️ این لینک معتبر نیست یا ویجت پیدا نشد.")
        return
    sent = await asyncio.to_thread(
        api,
        "sendSharedWidget",
        {
            "chat_id": message.chat_id,
            "widget_id": entry["widget_id"],
        },
        base_url=WIDGET_API_BASE,
        timeout=90,
    )
    await record_widget_message(context, token, message.chat_id, sent["message_id"])

async def ensure_short_widget_token(context, token):
    if len(token) == 3 and token.isascii() and token.isalnum() and token.isupper():
        return token
    bot_data = context.application.bot_data
    lock = bot_data.setdefault("widget_index_lock", asyncio.Lock())
    async with lock:
        current = bot_data.get("widget_index") or empty_widget_index()
        if token not in current.get("widgets", {}):
            return token
        index = json.loads(json.dumps(current))
        existing = set(index["widgets"])
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        short_code = None
        for _ in range(200):
            candidate = "".join(secrets.choice(alphabet) for _ in range(3))
            if candidate not in existing:
                short_code = candidate
                break
        if short_code is None:
            raise RuntimeError("ساخت کد کوتاه یکتا ممکن نشد، دوباره تلاش کن.")
        index["widgets"][short_code] = index["widgets"][token]
        bot_data["widget_index"] = index
        try:
            await save_widget_index(context)
        except Exception:
            bot_data["widget_index"] = current
            raise
        return short_code
async def handle_widget_reply(message, context):
    if not message.reply_to_message:
        return False
    command = (message.text or "").strip()
    if command not in ("لینک", "کد"):
        return False
    index = context.application.bot_data.get("widget_index") or empty_widget_index()
    key = f"{message.chat_id}:{message.reply_to_message.message_id}"
    token = index.get("messages", {}).get(key)
    entry = index.get("widgets", {}).get(token) if token else None
    if not entry:
        await message.reply_text("⚠️ ویجت توی آرشیو پیدا نشد")
        return True

    if command == "لینک":
        token = await ensure_short_widget_token(context, token)
        bot_user = await context.bot.get_me()
        url = f"https://t.me/{bot_user.username}?start={token}"
        await message.reply_text(f"🔗 لینک ویجت:\n{url}")
        return True

    remote = await context.bot.get_file(entry["file_id"])
    content = await remote.download_as_bytearray()
    await message.reply_document(
        document=InputFile(io.BytesIO(bytes(content)), filename=f"widget-{token}.html"),
        caption="کد HTML ویجت",
    )
    return True

async def widget_mode_message(message, context):
    context.user_data["widget_mode"] = True
    context.user_data.pop("pending", None)
    context.user_data.pop("collection", None)
    await message.reply_text(
        "🧩 <b>ساخت دستی ویجت</b>\n\n"
        "کد HTML رو همین‌جا بفرست یا فایل .html رو ارسال کن\n"
        "CSS و JavaScript باید داخل خود فایل باشن\n\n"
        "حداکثر حجم: 32 کیلوبایت خام و 4 کیلوبایت فشرده",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="main:widget")]]),
    )

async def widget_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("control_widget", None)
    await widget_mode_message(update.effective_message, context)

async def widget_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if await handle_widget_reply(message, context):
        raise ApplicationHandlerStop
    if context.user_data.get("widget_mode"):
        try:
            await send_html_widget(message, message.text or "", context)
            context.user_data["widget_mode"] = False
        except Exception as e:
            await message.reply_text(f"⚠️ ارسال ویجت ناموفق بود:\n{e}")
        raise ApplicationHandlerStop

async def widget_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("widget_mode"):
        await update.effective_message.reply_text("اول از منوی اصلی بخش «ویجت» رو انتخاب کن.")
        return
    message = update.effective_message
    document = message.document
    try:
        remote_file = await context.bot.get_file(document.file_id)
        content = await remote_file.download_as_bytearray()
        html = bytes(content).decode("utf-8")
        await send_html_widget(message, html, context)
        context.user_data["widget_mode"] = False
    except UnicodeDecodeError:
        await message.reply_text("⚠️ فایل باید با کدگذاری UTF-8 ذخیره شده باشه.")
    except Exception as e:
        await message.reply_text(f"⚠️ ارسال ویجت ناموفق بود:\n{e}")


async def media_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await receive_media(update, context)

# Telegram storage operations include multi-file uploads. PTB's default HTTPX
# timeouts are too short for these requests on slower Railway/network links.
telegram_request = HTTPXRequest(
    connection_pool_size=32,
    connect_timeout=20.0,
    read_timeout=60.0,
    write_timeout=120.0,
    pool_timeout=30.0,
    media_write_timeout=180.0,
    http_version="1.1",
)
telegram_updates_request = HTTPXRequest(
    connection_pool_size=8,
    connect_timeout=20.0,
    read_timeout=60.0,
    write_timeout=30.0,
    pool_timeout=30.0,
    http_version="1.1",
)
app = (
    Application.builder()
    .token(TOKEN)
    .request(telegram_request)
    .get_updates_request(telegram_updates_request)
    .post_init(initialize_access_storage)
    .build()
)
app.bot_data["source_texts"] = {}
app.bot_data["authorized_users"] = set()
# Activity auditing is intentionally disabled. No user messages or button clicks
# are copied into the support/storage group.
# Group -1 checks every update before the bot's normal handlers.
app.add_handler(TypeHandler(Update, log_incoming_update), group=-2)
app.add_handler(MessageHandler(filters.ALL, access_guard), group=-1)
app.add_handler(TypeHandler(Update, widget_query_handler), group=-3)
app.add_handler(CommandHandler("myid", myid))
app.add_handler(CommandHandler("version", version_command))
app.add_handler(CommandHandler("grant", grant_access))
app.add_handler(CommandHandler("revoke", revoke_access))
app.add_handler(CommandHandler("access", list_access))
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("widget", widget_start))
app.add_handler(CallbackQueryHandler(callbacks))
app.add_handler(MessageHandler(
    filters.Document.FileExtension("html") | filters.Document.FileExtension("htm"),
    widget_document,
))
app.add_handler(MessageHandler(
    filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.AUDIO |
    filters.VOICE | filters.Document.ALL,
    media_router,
))
app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^\s*(?:شرووع(?:\s+[0-9۰-۹٠-٩]+)?|توقف)\s*$"), topic_loop_command), group=0)
app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^\s*م\s*$"), widget_menu_start), group=0)
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, widget_text), group=0)
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_text), group=1)

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    if is_message_not_modified_error(err):
        return
    if isinstance(err, (NetworkError, TimedOut)):
        print(f"TELEGRAM TRANSIENT NETWORK ERROR: {type(err).__name__}: {err}", flush=True)
        return
    print(f"UNHANDLED BOT ERROR: {type(err).__name__}: {err}", flush=True)


app.add_error_handler(global_error_handler)

if __name__ == "__main__":
    allowed_updates = list(Update.ALL_TYPES) + ["widget_query", "shared_widget_query"]
    app.run_polling(allowed_updates=allowed_updates, bootstrap_retries=-1)