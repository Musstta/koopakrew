import re
from collections import OrderedDict

from deep_translator import GoogleTranslator
from flask import flash, g, has_request_context, request, session, url_for

from i18n import SPANISH_TRANSLATIONS, SUPPORTED_LANGUAGES  # noqa: F401 — re-exported

_TRANSLATOR_CACHE_MAX = 512
_translator_cache: OrderedDict[tuple[str, str], str] = OrderedDict()
_google_translator = GoogleTranslator(source="en", target="es")
_PLACEHOLDER_PATTERN = re.compile(r"{([^}]+)}")


def _cache_get(key):
    if key in _translator_cache:
        _translator_cache.move_to_end(key)
        return _translator_cache[key]
    return None


def _cache_set(key, value):
    _translator_cache[key] = value
    _translator_cache.move_to_end(key)
    if len(_translator_cache) > _TRANSLATOR_CACHE_MAX:
        _translator_cache.popitem(last=False)


def _protect_placeholders(text: str):
    replacements = {}

    def repl(match):
        key = match.group(1)
        token = f"__PH_{len(replacements)}__"
        replacements[token] = "{" + key + "}"
        return token

    safe_text = _PLACEHOLDER_PATTERN.sub(repl, text)
    return safe_text, replacements


def _restore_placeholders(text: str, replacements: dict[str, str]):
    restored = text
    for token, original in replacements.items():
        restored = restored.replace(token, original)
    return restored


def get_current_language() -> str:
    if has_request_context():
        return getattr(g, "current_lang", "en")
    return "en"


def translate_text(text: str) -> str:
    lang = get_current_language()
    if lang == "es":
        if not text:
            return text
        if text in SPANISH_TRANSLATIONS:
            return SPANISH_TRANSLATIONS[text]
        cache_key = (lang, text)
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        safe_text, replacements = _protect_placeholders(text)
        try:
            translated = _google_translator.translate(safe_text)
        except Exception:
            translated = text
        else:
            translated = _restore_placeholders(translated, replacements)
        _cache_set(cache_key, translated)
        return translated
    return text


def flash_message(message: str, category: str, **kwargs):
    flash(translate_text(message).format(**kwargs), category)


def build_lang_url(lang_code: str) -> str:
    endpoint = request.endpoint or "index"
    args = request.view_args.copy() if request.view_args else {}
    query = request.args.to_dict(flat=True)
    query["lang"] = lang_code
    args.update(query)
    try:
        return url_for(endpoint, **args)
    except Exception:
        return url_for("index", lang=lang_code)


def set_language():
    lang = request.args.get("lang")
    if lang in SUPPORTED_LANGUAGES:
        session["lang"] = lang
    g.current_lang = session.get("lang", "en")


def inject_i18n() -> dict:
    return {
        "_": translate_text,
        "current_lang": getattr(g, "current_lang", "en"),
        "supported_languages": SUPPORTED_LANGUAGES,
        "lang_url": build_lang_url,
    }
