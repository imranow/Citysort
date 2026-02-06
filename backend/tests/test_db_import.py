from __future__ import annotations

import sqlite3

import pytest

from app.db_import import (
    ExternalDatabaseError,
    coerce_row_content_to_bytes,
    connect_external_database,
    fetch_import_rows,
    get_row_value,
    validate_readonly_query,
)


def test_validate_readonly_query_rejects_mutation_keywords() -> None:
    with pytest.raises(ExternalDatabaseError):
        validate_readonly_query("DELETE FROM files")


def test_connect_and_fetch_import_rows_from_sqlite(tmp_path) -> None:
    source_db = tmp_path / "source.db"
    connection = sqlite3.connect(source_db)
    connection.execute(
        """
        CREATE TABLE incoming_files (
            filename TEXT NOT NULL,
            content BLOB NOT NULL,
            content_type TEXT
        )
        """
    )
    connection.execute(
        "INSERT INTO incoming_files (filename, content, content_type) VALUES (?, ?, ?)",
        ("alpha.txt", b"hello world", "text/plain"),
    )
    connection.execute(
        "INSERT INTO incoming_files (filename, content, content_type) VALUES (?, ?, ?)",
        ("beta.txt", b"second file", "text/plain"),
    )
    connection.commit()
    connection.close()

    import_connection = connect_external_database(f"sqlite:///{source_db}")
    rows = fetch_import_rows(
        connection=import_connection,
        query="SELECT filename, content, content_type FROM incoming_files ORDER BY filename",
        limit=10,
    )
    import_connection.close()

    assert len(rows) == 2
    assert get_row_value(rows[0], "filename") == "alpha.txt"
    assert get_row_value(rows[1], "content_type") == "text/plain"
    assert coerce_row_content_to_bytes(get_row_value(rows[0], "content")) == b"hello world"


def test_get_row_value_handles_case_insensitive_column_names(tmp_path) -> None:
    source_db = tmp_path / "case_test.db"
    connection = sqlite3.connect(source_db)
    connection.execute("CREATE TABLE docs (FileName TEXT, Content BLOB)")
    connection.execute("INSERT INTO docs (FileName, Content) VALUES (?, ?)", ("Case.TXT", b"Case content"))
    connection.commit()
    connection.close()

    import_connection = connect_external_database(str(source_db))
    rows = fetch_import_rows(connection=import_connection, query="SELECT FileName, Content FROM docs", limit=1)
    import_connection.close()

    assert get_row_value(rows[0], "filename") == "Case.TXT"
    assert get_row_value(rows[0], "content") == b"Case content"


def test_rejects_unsupported_database_scheme() -> None:
    with pytest.raises(ExternalDatabaseError):
        connect_external_database("sqlserver://user:pass@localhost:1433/dbname")


class _FakeCursor:
    def __init__(self) -> None:
        self.description = [("filename",), ("content",)]
        self._rows = [("row1.txt", b"a"), ("row2.txt", b"b")]

    def execute(self, _query: str) -> None:
        return None

    def fetchmany(self, count: int):
        return self._rows[:count]

    def close(self) -> None:
        return None


class _FakeConnection:
    def cursor(self):
        return _FakeCursor()


def test_fetch_import_rows_normalizes_tuple_rows() -> None:
    rows = fetch_import_rows(connection=_FakeConnection(), query="SELECT filename, content FROM sample", limit=1)
    assert rows == [{"filename": "row1.txt", "content": b"a"}]
