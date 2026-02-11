from __future__ import annotations

import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import ENCRYPTION_AT_REST_ENABLED, ENCRYPTION_KEY

MAGIC_HEADER = b"CSENC1\n"

_FERNET = None
_FERNET_READY = False


def _get_fernet():
    global _FERNET, _FERNET_READY
    if _FERNET_READY:
        return _FERNET
    _FERNET_READY = True
    if not ENCRYPTION_AT_REST_ENABLED:
        return None
    try:
        from cryptography.fernet import Fernet
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Encryption at rest is enabled but 'cryptography' is not installed."
        ) from exc
    if not ENCRYPTION_KEY:
        raise RuntimeError("Encryption at rest is enabled but CITYSORT_ENCRYPTION_KEY is missing.")
    try:
        _FERNET = Fernet(ENCRYPTION_KEY.encode("utf-8"))
    except Exception as exc:
        raise RuntimeError("CITYSORT_ENCRYPTION_KEY is invalid. Must be a valid Fernet key.") from exc
    return _FERNET


def validate_encryption_configuration() -> None:
    _get_fernet()


def _encrypt(payload: bytes) -> bytes:
    fernet = _get_fernet()
    if not fernet:
        return payload
    return MAGIC_HEADER + fernet.encrypt(payload)


def _decrypt(payload: bytes) -> bytes:
    if not payload.startswith(MAGIC_HEADER):
        return payload
    fernet = _get_fernet()
    if not fernet:
        raise RuntimeError("Encrypted payload found but encryption key is unavailable.")
    return fernet.decrypt(payload[len(MAGIC_HEADER) :])


def write_document_bytes(destination_path: Path, payload: bytes) -> None:
    destination_path.write_bytes(_encrypt(payload))


def read_document_bytes(source_path: Path) -> bytes:
    data = source_path.read_bytes()
    return _decrypt(data)


def copy_source_to_storage(source_path: Path, destination_path: Path) -> None:
    write_document_bytes(destination_path, source_path.read_bytes())


def is_encrypted_file(source_path: Path) -> bool:
    try:
        prefix = source_path.read_bytes()[: len(MAGIC_HEADER)]
    except Exception:
        return False
    return prefix == MAGIC_HEADER


@contextmanager
def open_plaintext_path(source_path: Path, *, suffix: str = "") -> Iterator[Path]:
    """Yield a plaintext path for processing. Handles encrypted-at-rest files."""
    if not source_path.exists():
        raise FileNotFoundError(str(source_path))

    if not is_encrypted_file(source_path):
        yield source_path
        return

    data = read_document_bytes(source_path)
    temp_file = tempfile.NamedTemporaryFile(prefix="citysort_dec_", suffix=suffix, delete=False)
    temp_path = Path(temp_file.name)
    try:
        temp_file.write(data)
        temp_file.flush()
        temp_file.close()
        yield temp_path
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            pass
