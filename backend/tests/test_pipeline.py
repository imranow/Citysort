from __future__ import annotations

from app.config import DOCUMENT_TYPE_RULES
from app.pipeline import (
    classify_document,
    detect_urgency,
    process_document,
    route_document,
    validate_document,
)


def test_classifies_building_permit_with_high_confidence() -> None:
    text = """
    Building Permit Application
    Applicant: Jane Smith
    Address: 100 Main St
    Parcel Number: P-100-22
    Date: 02/03/2026
    Includes site plan and construction details.
    """
    doc_type, confidence, meta = classify_document(
        text, active_rules=DOCUMENT_TYPE_RULES
    )

    assert doc_type == "building_permit"
    assert confidence > 0.55
    assert meta["matched_keywords"]


def test_validate_missing_fields() -> None:
    fields = {"applicant_name": "Jane Smith", "date": "02/03/2026"}
    missing_fields, errors = validate_document(
        "building_permit", fields, active_rules=DOCUMENT_TYPE_RULES
    )

    assert "address" in missing_fields
    assert "parcel_number" in missing_fields
    assert len(errors) >= 2


def test_route_unknown_document_to_general_intake() -> None:
    department = route_document("not_a_real_type", active_rules=DOCUMENT_TYPE_RULES)
    assert department == "General Intake"


def test_detect_urgency_high() -> None:
    urgency = detect_urgency("This is an emergency filing with an immediate deadline.")
    assert urgency == "high"


def test_process_document_uses_external_classification_when_available(
    monkeypatch, tmp_path
) -> None:
    sample = tmp_path / "sample.txt"
    sample.write_text(
        "Permit packet\\nApplicant: Jane Smith\\nAddress: 10 Main St\\nDate: 02/05/2026",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "app.pipeline.try_external_ocr", lambda file_path, content_type=None: None
    )
    monkeypatch.setattr(
        "app.pipeline.try_external_classification",
        lambda text, extracted_fields, active_rules=None: {
            "doc_type": "complaint",
            "department": "Code Enforcement",
            "urgency": "high",
            "confidence": 0.93,
            "matched_keywords": ["complaint"],
            "provider": "openai",
        },
    )

    result = process_document(file_path=str(sample), content_type="text/plain")
    assert result["doc_type"] == "complaint"
    assert result["department"] == "Code Enforcement"
    assert result["urgency"] == "high"
    assert result["pipeline_meta"]["classification_meta"]["provider"] == "openai"
