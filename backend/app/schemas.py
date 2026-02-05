from __future__ import annotations

from typing import Any
from typing import Optional

from pydantic import BaseModel, Field


class DocumentResponse(BaseModel):
    id: str
    filename: str
    source_channel: str
    content_type: Optional[str] = None
    status: str
    doc_type: Optional[str] = None
    department: Optional[str] = None
    urgency: Optional[str] = None
    confidence: float = 0.0
    requires_review: bool = False
    extracted_text: Optional[str] = None
    extracted_fields: dict[str, Any] = Field(default_factory=dict)
    missing_fields: list[str] = Field(default_factory=list)
    validation_errors: list[str] = Field(default_factory=list)
    reviewer_notes: Optional[str] = None
    created_at: str
    updated_at: str


class DocumentListResponse(BaseModel):
    items: list[DocumentResponse]


class QueueItem(BaseModel):
    department: str
    total: int
    needs_review: int
    ready: int


class QueueResponse(BaseModel):
    queues: list[QueueItem]


class MetricBucket(BaseModel):
    label: str
    count: int


class AnalyticsResponse(BaseModel):
    total_documents: int
    needs_review: int
    routed_or_approved: int
    average_confidence: float
    by_type: list[MetricBucket] = Field(default_factory=list)
    by_status: list[MetricBucket] = Field(default_factory=list)


class ReviewRequest(BaseModel):
    approve: bool = True
    corrected_doc_type: Optional[str] = None
    corrected_department: Optional[str] = None
    corrected_fields: dict[str, Any] = Field(default_factory=dict)
    notes: Optional[str] = None
    actor: str = "reviewer"


class AuditEvent(BaseModel):
    id: int
    document_id: str
    action: str
    actor: str
    details: Optional[str] = None
    created_at: str


class AuditTrailResponse(BaseModel):
    items: list[AuditEvent]


class RuleDefinition(BaseModel):
    keywords: list[str] = Field(default_factory=list)
    department: str
    required_fields: list[str] = Field(default_factory=list)


class RulesConfigResponse(BaseModel):
    source: str
    path: str
    rules: dict[str, RuleDefinition]


class RulesConfigUpdate(BaseModel):
    rules: dict[str, RuleDefinition]
    actor: str = "dashboard_admin"
