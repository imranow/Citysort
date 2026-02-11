from __future__ import annotations

import pytest


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    """Provide an isolated SQLite database in a temp directory."""
    from app import db

    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setattr(db, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(db, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(db, "DATABASE_PATH", tmp_path / "citysort.db")
    db.init_db()
    return db


@pytest.fixture()
def isolated_repo(isolated_db):
    """Provide repository module backed by isolated DB."""
    from app import repository

    return repository


@pytest.fixture()
def sample_document(isolated_repo, isolated_db):
    """Create a sample document in the isolated DB and return it."""
    upload_dir = isolated_db.UPLOAD_DIR
    upload_dir.mkdir(parents=True, exist_ok=True)
    sample_path = upload_dir / "sample.txt"
    sample_path.write_text(
        "Building Permit\nApplicant: John Doe\nAddress: 12 Main Street\n"
        "Parcel Number: P-1234\nDate: 02/07/2026",
        encoding="utf-8",
    )

    doc = isolated_repo.create_document(
        document={
            "id": "doc-test-1",
            "filename": "sample.txt",
            "storage_path": str(sample_path),
            "source_channel": "test",
            "content_type": "text/plain",
            "status": "ingested",
            "requires_review": False,
            "confidence": 0.0,
            "doc_type": None,
            "department": None,
            "urgency": "normal",
        }
    )
    return doc
