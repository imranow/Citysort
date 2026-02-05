from pathlib import Path

from app.pipeline import process_document
from app.rules import get_active_rules, normalize_rules, reset_rules_to_default, save_rules


def test_normalize_rules_adds_other() -> None:
    rules = normalize_rules(
        {
            "task_sheet": {
                "keywords": ["task sheet", "public works"],
                "department": "Public Works",
                "required_fields": ["applicant_name", "date"],
            }
        }
    )

    assert "task_sheet" in rules
    assert "other" in rules


def test_save_and_get_custom_rules(monkeypatch, tmp_path) -> None:
    rules_path = tmp_path / "rules.json"
    monkeypatch.setattr("app.rules.RULES_CONFIG_PATH", rules_path)

    saved = save_rules(
        {
            "task_sheet": {
                "keywords": ["task sheet"],
                "department": "Public Works",
                "required_fields": ["applicant_name", "date"],
            }
        }
    )

    loaded, source = get_active_rules()
    assert source == "custom"
    assert loaded == saved
    assert loaded["task_sheet"]["department"] == "Public Works"


def test_process_document_uses_custom_rules(monkeypatch, tmp_path) -> None:
    rules_path = tmp_path / "rules.json"
    monkeypatch.setattr("app.rules.RULES_CONFIG_PATH", rules_path)

    save_rules(
        {
            "task_sheet": {
                "keywords": ["task sheet", "work order"],
                "department": "Public Works",
                "required_fields": ["applicant_name", "date"],
            }
        }
    )

    monkeypatch.setattr("app.pipeline.try_external_ocr", lambda file_path, content_type=None: None)
    monkeypatch.setattr("app.pipeline.try_external_classification", lambda text, extracted_fields, active_rules=None: None)

    sample = Path(tmp_path / "task_sheet.txt")
    sample.write_text(
        "2025 Task Sheet\\nApplicant: Maria Nguyen\\nDate: 02/05/2026\\nPublic Works work order",
        encoding="utf-8",
    )

    result = process_document(file_path=str(sample), content_type="text/plain")
    assert result["doc_type"] == "task_sheet"
    assert result["department"] == "Public Works"

    reset_rules_to_default()
