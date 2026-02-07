from __future__ import annotations

from typing import Any
from typing import Optional

from pydantic import BaseModel, Field, model_validator


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


class DatabaseImportRequest(BaseModel):
    database_url: str = Field(..., description="Database URL (sqlite/postgresql/mysql) or sqlite filesystem path")
    query: str = Field(..., description="SELECT query that returns file rows")
    filename_column: str = "filename"
    content_column: Optional[str] = "content"
    file_path_column: Optional[str] = None
    content_type_column: Optional[str] = "content_type"
    source_channel: str = "database_import"
    actor: str = "database_connector"
    process_async: bool = False
    limit: int = Field(default=500, ge=1, le=5000)

    @model_validator(mode="after")
    def _validate_source_columns(self) -> "DatabaseImportRequest":
        if not self.content_column and not self.file_path_column:
            raise ValueError("Set either content_column or file_path_column.")
        return self


class DatabaseImportDocument(BaseModel):
    id: str
    filename: str
    status: str


class DatabaseImportResponse(BaseModel):
    imported_count: int
    processed_sync_count: int
    scheduled_async_count: int
    failed_count: int
    documents: list[DatabaseImportDocument] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class ServiceHealth(BaseModel):
    name: str
    status: str
    configured: bool
    details: str


class ConnectivityResponse(BaseModel):
    database: ServiceHealth
    ocr_provider: ServiceHealth
    classifier_provider: ServiceHealth
    checked_at: str


class ManualDeploymentRequest(BaseModel):
    environment: str = "production"
    actor: str = "dashboard_admin"
    notes: Optional[str] = None


class DeploymentRecord(BaseModel):
    id: int
    environment: str
    status: str
    actor: str
    notes: Optional[str] = None
    details: Optional[str] = None
    created_at: str
    finished_at: Optional[str] = None


class DeploymentListResponse(BaseModel):
    items: list[DeploymentRecord] = Field(default_factory=list)


class InvitationCreateRequest(BaseModel):
    email: str
    role: str = "member"
    actor: str = "dashboard_admin"
    expires_in_days: int = Field(default=7, ge=1, le=90)


class InvitationRecord(BaseModel):
    id: int
    email: str
    role: str
    status: str
    actor: str
    created_at: str
    expires_at: str
    accepted_at: Optional[str] = None


class InvitationCreateResponse(BaseModel):
    invitation: InvitationRecord
    invite_token: str
    invite_link: str


class InvitationListResponse(BaseModel):
    items: list[InvitationRecord] = Field(default_factory=list)


class ApiKeyCreateRequest(BaseModel):
    name: str = Field(min_length=2, max_length=64)
    actor: str = "dashboard_admin"


class ApiKeyRecord(BaseModel):
    id: int
    name: str
    key_prefix: str
    status: str
    actor: str
    created_at: str
    revoked_at: Optional[str] = None


class ApiKeyCreateResponse(BaseModel):
    api_key: ApiKeyRecord
    raw_key: str


class ApiKeyListResponse(BaseModel):
    items: list[ApiKeyRecord] = Field(default_factory=list)


class PlatformSummaryResponse(BaseModel):
    connectivity: ConnectivityResponse
    active_api_keys: int
    pending_invitations: int
    latest_deployment: Optional[DeploymentRecord] = None
