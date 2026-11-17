"""
shared.language - Reusable fastText-based language detection.

Not an LLM call: ~sub-millisecond, no network round-trip per request.

Model: fastText lid.176 (176 languages). By default we compare its confidence
only across a set of candidate languages (EN vs FR out of the box) and ignore
the other labels.

The ~130MB model file is NOT shipped with the package. It is downloaded once
by ensure_model_ready(), which you should call at application startup
(e.g. the FastAPI lifespan). If the download fails, startup fails loudly
instead of silently defaulting every request to one language.

Model path resolution (first match wins):
  1. ensure_model_ready(model_path=...) / configure(model_path=...)
  2. LANGUAGE_ID_MODEL_PATH environment variable
  3. ~/.cache/shared/lid.176.bin
"""
import logging
import os
import re
import threading
import urllib.request
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "configure",
    "ensure_model_ready",
    "detect_language",
    "detect_language_label",
]

MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
ENV_VAR = "LANGUAGE_ID_MODEL_PATH"
DEFAULT_MODEL_PATH = os.path.join(
    os.path.expanduser("~"), ".cache", "shared", "lid.176.bin"
)

_configured_path: Optional[str] = None
_model = None
_model_lock = threading.Lock()
_ready_lock = threading.Lock()


def configure(model_path: Optional[str] = None) -> None:
    """Override where the model file lives. Call before first use."""
    global _configured_path, _model
    with _model_lock:
        if model_path != _configured_path:
            _configured_path = model_path
            _model = None  # force reload from the new location


def _model_path() -> str:
    return _configured_path or os.environ.get(ENV_VAR) or DEFAULT_MODEL_PATH


def _download_model(path: str) -> None:
    """Download the fastText model atomically (write to .tmp, then rename)."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp_path = f"{path}.tmp"
    logger.info("Language ID model missing at %s; downloading from %s", path, MODEL_URL)
    try:
        urllib.request.urlretrieve(MODEL_URL, tmp_path)
        os.replace(tmp_path, path)  # atomic on the same filesystem
        logger.info(
            "Language ID model downloaded to %s (%d bytes)", path, os.path.getsize(path)
        )
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def _get_model():
    """Lazy-loaded, process-wide singleton.

    Raises if the model file is missing or cannot be loaded -- callers must
    either call ensure_model_ready() at startup or handle the exception.
    """
    global _model
    if _model is None:
        with _model_lock:
            if _model is None:  # re-check inside the lock
                import fasttext

                path = _model_path()
                if not os.path.exists(path):
                    raise FileNotFoundError(
                        f"fastText model not found at {path}. "
                        f"Call ensure_model_ready() at startup to download it."
                    )
                logger.info("Loading fastText language ID model from %s", path)
                _model = fasttext.load_model(path)
    return _model


def ensure_model_ready(model_path: Optional[str] = None) -> None:
    """Startup hook: download the model if absent, then load it eagerly.

    Idempotent and thread-safe. Fails fast (raises) if the model cannot be
    downloaded or loaded.
    """
    if model_path is not None:
        configure(model_path)
    with _ready_lock:
        path = _model_path()
        if not os.path.exists(path):
            _download_model(path)
        _get_model()


def detect_language(
    text: str,
    candidates: Iterable[str] = ("en", "fr"),
    default: str = "en",
) -> str:
    """Return the most likely language among `candidates` (ISO 639-1 codes).

    Empty/whitespace-only input, or text where none of the candidates appear
    in fastText's ranking, returns `default`. Raises only for genuine
    model-loading failures; callers decide how to degrade.
    """
    clean = re.sub(r"\s+", " ", (text or "").strip())
    if not clean:
        return default

    wanted = set(candidates)
    model = _get_model()
    # k=176: see the full ranking so we pick the best *candidate*, not just
    # fastText's top-1 (which may be some other language on short text).
    labels, probs = model.predict(clean, k=176)

    scores = {}
    for label, prob in zip(labels, probs):
        code = label.replace("__label__", "")
        if code in wanted:
            scores[code] = float(prob)

    if not scores:
        return default
    return max(scores, key=scores.get)


def detect_language_label(text: str) -> str:
    """Binary EN/FR detection. Returns "en" or "fr"."""
    return detect_language(text, candidates=("en", "fr"), default="en")
