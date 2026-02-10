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
    due_date: Optional[str] = None
    sla_days: Optional[int] = None
    assigned_to: Optional[str] = None
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
    overdue: int = 0
    automated_documents: int = 0
    manual_documents: int = 0
    automation_rate: float = 0.0
    manual_rate: float = 0.0
    manual_unassigned: int = 0
    missing_contact_email: int = 0
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
    sla_days: Optional[int] = None


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


class ConnectorTestRequest(BaseModel):
    connector_type: str
    config: dict[str, Any] = Field(default_factory=dict)
    database_url: Optional[str] = None


class ConnectorTestResponse(BaseModel):
    success: bool
    message: str
    connector_type: str
    details: Optional[str] = None


class ServiceHealth(BaseModel):
    name: str
    status: str
    configured: bool
    details: str


class ConnectivityResponse(BaseModel):
    database: ServiceHealth
    ocr_provider: ServiceHealth
    classifier_provider: ServiceHealth
    deployment_provider: Optional[ServiceHealth] = None
    checked_at: str


class ManualDeploymentRequest(BaseModel):
    environment: str = "production"
    actor: str = "dashboard_admin"
    notes: Optional[str] = None


class DeploymentRecord(BaseModel):
    id: int
    environment: str
    provider: str
    status: str
    actor: str
    notes: Optional[str] = None
    details: Optional[str] = None
    external_id: Optional[str] = None
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


class AuthBootstrapRequest(BaseModel):
    email: str
    password: str
    full_name: Optional[str] = None


class AuthLoginRequest(BaseModel):
    email: str
    password: str


class UserRecord(BaseModel):
    id: str
    email: str
    full_name: Optional[str] = None
    role: str
    status: str
    last_login_at: Optional[str] = None
    created_at: str
    updated_at: str


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserRecord


class UserCreateRequest(BaseModel):
    email: str
    password: str
    role: str = "viewer"
    full_name: Optional[str] = None


class UserRoleUpdateRequest(BaseModel):
    role: str


class UserListResponse(BaseModel):
    items: list[UserRecord] = Field(default_factory=list)


class JobRecord(BaseModel):
    id: str
    job_type: str
    status: str
    payload: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    actor: str
    attempts: int
    max_attempts: int
    worker_id: Optional[str] = None
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class JobListResponse(BaseModel):
    items: list[JobRecord] = Field(default_factory=list)


# --- Notifications ---

class NotificationRecord(BaseModel):
    id: int
    user_id: Optional[str] = None
    type: str
    title: str
    message: Optional[str] = None
    document_id: Optional[str] = None
    is_read: bool = False
    created_at: str
    read_at: Optional[str] = None


class NotificationListResponse(BaseModel):
    items: list[NotificationRecord] = Field(default_factory=list)
    unread_count: int = 0


# --- Workflow Transitions ---

class TransitionRequest(BaseModel):
    status: str
    notes: Optional[str] = None
    actor: str = "dashboard_reviewer"


# --- Assignment ---

class AssignRequest(BaseModel):
    user_id: str
    actor: str = "dashboard_reviewer"


# --- Response Templates ---

class TemplateRecord(BaseModel):
    id: int
    name: str
    doc_type: Optional[str] = None
    template_body: str
    created_at: str
    updated_at: str


class TemplateCreateRequest(BaseModel):
    name: str
    doc_type: Optional[str] = None
    template_body: str


class TemplateUpdateRequest(BaseModel):
    name: Optional[str] = None
    doc_type: Optional[str] = None
    template_body: Optional[str] = None


class TemplateListResponse(BaseModel):
    items: list[TemplateRecord] = Field(default_factory=list)


class TemplateRenderResponse(BaseModel):
    rendered: str
    template_name: str
    document_id: str


class TemplateComposeResponse(BaseModel):
    template_id: int
    template_name: str
    document_id: str
    to_email: Optional[str] = None
    subject: str
    body: str


class ResponseEmailSendRequest(BaseModel):
    to_email: str
    subject: str
    body: str
    actor: str = "dashboard_reviewer"


class ResponseEmailSendResponse(BaseModel):
    id: int
    document_id: str
    to_email: str
    subject: str
    status: str
    error: Optional[str] = None
    sent_at: Optional[str] = None
    created_at: str


class AutomationAutoAssignRequest(BaseModel):
    user_id: Optional[str] = None
    actor: str = "automation_assistant"
    limit: int = Field(default=200, ge=1, le=1000)


class AutomationAutoAssignResponse(BaseModel):
    assignee: str
    assigned_count: int
    remaining_unassigned: int
    processed_document_ids: list[str] = Field(default_factory=list)


class AutomationAnthropicSweepRequest(BaseModel):
    limit: int = Field(default=50, ge=1, le=500)
    include_failed: bool = True
    actor: str = "anthropic_automation"


class AutomationAnthropicSweepResponse(BaseModel):
    processed_count: int
    auto_cleared_count: int
    still_manual_count: int
    processed_document_ids: list[str] = Field(default_factory=list)


# --- Bulk Operations ---

class BulkActionRequest(BaseModel):
    action: str
    document_ids: list[str]
    params: dict[str, Any] = Field(default_factory=dict)
    actor: str = "dashboard_reviewer"


class BulkActionResponse(BaseModel):
    success_count: int
    error_count: int
    errors: list[str] = Field(default_factory=list)
