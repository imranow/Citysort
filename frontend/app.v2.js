const uploadForm = document.getElementById("upload-form");
const fileInput = document.getElementById("file-input");
const uploadStatus = document.getElementById("upload-status");
const platformStatus = document.getElementById("platform-status");
const queueChips = document.getElementById("queue-chips");
const queueSummaryBody = document.getElementById("queue-summary-body");
const reviewSummaryBody = document.getElementById("review-summary-body");
const reviewListScroll = document.getElementById("review-list-scroll");
const reviewListCount = document.getElementById("review-list-count");
const reviewDetailContent = document.getElementById("review-detail-content");
const reviewEmptyState = document.getElementById("review-empty-state");
const reviewIssues = document.getElementById("review-issues");
const refreshButton = document.getElementById("refresh");
const lastUpdated = document.getElementById("last-updated");
const docsButton = document.getElementById("docs-btn");
const inviteButton = document.getElementById("invite-btn");
const newKeyButton = document.getElementById("new-key-btn");
const logoutButton = document.getElementById("logout-btn");
const connectButton = document.getElementById("connect-btn");
const deployButton = document.getElementById("deploy-btn");

const filterStatus = document.getElementById("filter-status");
const filterSearch = document.getElementById("filter-search");
let activeDepartmentFilter = "";

const reviewForm = document.getElementById("review-form");
const rejectButton = document.getElementById("reject");
const reprocessButton = document.getElementById("reprocess");
const reviewStatus = document.getElementById("review-status");
const reviewId = document.getElementById("review-id");
const reviewDocType = document.getElementById("review-doc-type");
const reviewDepartment = document.getElementById("review-department");
const reviewAssignedTo = document.getElementById("review-assigned-to");
const reviewNotes = document.getElementById("review-notes");
const reviewFieldsJson = document.getElementById("review-fields-json");
const connectorsGrid = document.getElementById("connectors-grid");
const connectorConfig = document.getElementById("connector-config");
const connectorBackBtn = document.getElementById("connector-back");
const connectorConfigHeader = document.getElementById("connector-config-header");
const connectorConfigFields = document.getElementById("connector-config-fields");
const connectorConfigForm = document.getElementById("connector-config-form");
const connectorTestBtn = document.getElementById("connector-test-btn");
const connectorImportBtn = document.getElementById("connector-import-btn");
const connectorStatus = document.getElementById("connector-status");
const connectorQuerySection = document.getElementById("connector-query-section");
let activeConnectorId = null;

const reviewFilename = document.getElementById("review-filename");
const reviewBadgeStatus = document.getElementById("review-badge-status");
const reviewBadgeType = document.getElementById("review-badge-type");
const reviewBadgeDept = document.getElementById("review-badge-dept");
const reviewConfBar = document.getElementById("review-conf-bar");
const reviewConfPct = document.getElementById("review-conf-pct");
const reviewDueDate = document.getElementById("review-due-date");
const automationInlineRate = document.getElementById("automation-inline-rate");
const automationInlineGap = document.getElementById("automation-inline-gap");
const automationInlineNeedsReview = document.getElementById("automation-inline-needs-review");
const automationInlineUnassigned = document.getElementById("automation-inline-unassigned");
const automationInlineMissingContact = document.getElementById("automation-inline-missing-contact");
const automationAutoAssignBtn = document.getElementById("automation-auto-assign");
const automationAnthropicSweepBtn = document.getElementById("automation-anthropic-sweep");
const automationAssistantStatus = document.getElementById("automation-assistant-status");
const reviewMissing = document.getElementById("review-missing");
const reviewErrors = document.getElementById("review-errors");
const reviewMissingSection = document.getElementById("review-missing-section");
const reviewErrorsSection = document.getElementById("review-errors-section");
const reviewAudit = document.getElementById("review-audit");
const transitionActions = document.getElementById("transition-actions");
const respondButton = document.getElementById("respond-btn");

const detailTabs = document.getElementById("detail-tabs");
const detailPanelReview = document.getElementById("detail-panel-review");
const detailPanelDocument = document.getElementById("detail-panel-document");
const docDownloadBtn = document.getElementById("doc-download-btn");
const docReuploadInput = document.getElementById("doc-reupload-input");
const docReuploadStatus = document.getElementById("doc-reupload-status");
const docTextPreview = document.getElementById("doc-text-preview");
const docFieldsEditor = document.getElementById("doc-fields-editor");
const docFieldsSave = document.getElementById("doc-fields-save");
const docFieldsStatus = document.getElementById("doc-fields-status");
let _originalFields = {};
let _currentDocForDocTab = null;

const rulesMeta = document.getElementById("rules-meta");
const rulesLoad = document.getElementById("rules-load");
const rulesAdd = document.getElementById("rules-add");
const rulesSave = document.getElementById("rules-save");
const rulesReset = document.getElementById("rules-reset");
const rulesApplyJson = document.getElementById("rules-apply-json");
const rulesJson = document.getElementById("rules-json");
const rulesStatus = document.getElementById("rules-status");
const rulesBuilder = document.getElementById("rules-builder");
const uploadButton = document.getElementById("upload-submit-btn");
const uploadDropzone = document.getElementById("upload-dropzone");
const uploadFileList = document.getElementById("upload-file-list");
const watcherStatus = document.getElementById("watcher-status");
const notificationBell = document.getElementById("notification-bell");
const notificationBadge = document.getElementById("notification-badge");
const notificationPanel = document.getElementById("notification-panel");
const notificationList = document.getElementById("notification-list");
const notificationsMarkAll = document.getElementById("notifications-mark-all");
const myDocsChip = document.getElementById("my-docs-chip");
const bulkToggle = document.getElementById("bulk-toggle");
const bulkToolbar = document.getElementById("bulk-toolbar");
const bulkSelectAll = document.getElementById("bulk-select-all");
const bulkSelectedCount = document.getElementById("bulk-selected-count");
const bulkApproveBtn = document.getElementById("bulk-approve");
const bulkAssignBtn = document.getElementById("bulk-assign");
const bulkExportBtn = document.getElementById("bulk-export");
const bulkCancelBtn = document.getElementById("bulk-cancel");
const responseModal = document.getElementById("response-modal");
const responseModalClose = document.getElementById("response-modal-close");
const responseCancelBtn = document.getElementById("response-cancel-btn");
const responseTemplateSelect = document.getElementById("response-template-select");
const responseToEmail = document.getElementById("response-to-email");
const responseSubjectInput = document.getElementById("response-subject-input");
const responseBodyInput = document.getElementById("response-body-input");
const responseSendStatus = document.getElementById("response-send-status");
const responseCopyBtn = document.getElementById("response-copy-btn");
const responseSendBtn = document.getElementById("response-send-btn");
const AUTH_TOKEN_KEY = "citysort_access_token";
let authToken = window.localStorage.getItem(AUTH_TOKEN_KEY) || "";
let authRedirectInProgress = false;

let selectedDocumentId = "";
let activeRules = {};
let currentUserId = "";
let currentUserEmail = "";
let userCache = [];
let documentsCache = [];
let bulkMode = false;
const bulkSelected = new Set();
let templateCache = [];
let notificationsPollHandle = null;
let classifierInfo = { classifier_provider: "rules" };
let platformSummaryRetryHandle = null;

const TRANSITIONS = {
  ingested: ["needs_review", "routed"],
  needs_review: ["acknowledged", "approved", "corrected"],
  routed: ["acknowledged", "approved"],
  acknowledged: ["assigned", "approved", "in_progress"],
  assigned: ["in_progress", "approved"],
  in_progress: ["completed", "approved"],
  completed: ["archived"],
  approved: ["archived"],
  corrected: ["archived"],
  failed: ["needs_review", "ingested"],
};

const OVERDUE_DONE_STATUSES = new Set(["approved", "corrected", "completed", "archived"]);
const PAGE_CONFIG = {
  overview: {
    title: "Overview",
    subtitle: "Operational snapshot across ingestion, queue health, review workload, and routing outcomes.",
  },
  upload: {
    title: "Ingestion",
    subtitle: "Upload and process document batches while monitoring throughput and confidence in real time.",
  },
  queues: {
    title: "Document Review",
    subtitle: "Review, approve, and route documents across department queues.",
  },
  connectors: {
    title: "Connectors",
    subtitle: "Connect databases and SaaS platforms to ingest documents into the CitySort pipeline.",
  },
  rules: {
    title: "Routing Rules",
    subtitle: "Manage classification policy, required fields, and department routing behavior for new intake.",
  },
};
const PAGE_ALIASES = {
  review: "queues",
  "db-import": "connectors",
};
let activeDashboardPage = "overview";

const toastContainer = document.getElementById("toast-container");
const toastIcons = {
  success: '<circle cx="8" cy="8" r="6.5"/><path d="M5.5 8.5l2 2L10.5 6"/>',
  error: '<circle cx="8" cy="8" r="6.5"/><path d="M6 6l4 4M10 6l-4 4"/>',
  warning: '<path d="M8 2L1.5 13.5h13L8 2z"/><path d="M8 7v3M8 11.5v.5"/>',
  info: '<circle cx="8" cy="8" r="6.5"/><path d="M8 7v4M8 5v.5"/>',
};

function resolvePageFromLocation() {
  const searchParams = new URLSearchParams(window.location.search);
  const raw = String(searchParams.get("page") || "overview").trim().toLowerCase();
  const aliased = PAGE_ALIASES[raw] || raw;
  const normalized = Object.prototype.hasOwnProperty.call(PAGE_CONFIG, aliased)
    ? aliased
    : "overview";
  return { raw, normalized };
}

function writePageToLocation(page, mode) {
  const searchParams = new URLSearchParams(window.location.search);
  searchParams.set("page", page);
  if (page !== "queues") {
    searchParams.delete("doc");
  }
  const nextQuery = searchParams.toString();
  const nextUrl = nextQuery ? `/app?${nextQuery}` : "/app";
  if (mode === "replace") {
    window.history.replaceState(null, "", nextUrl);
  } else if (mode === "push") {
    window.history.pushState(null, "", nextUrl);
  }
}

function applyDashboardPage(page, options = {}) {
  const { updateUrlMode = null } = options;
  const normalized = Object.prototype.hasOwnProperty.call(PAGE_CONFIG, page)
    ? page
    : "overview";
  activeDashboardPage = normalized;
  const meta = PAGE_CONFIG[normalized];

  const titleEl = document.querySelector(".shell-topbar h1");
  if (titleEl) {
    titleEl.textContent = meta.title;
  }
  const subtitleEl = document.querySelector(".shell-topbar .subtitle");
  if (subtitleEl) {
    subtitleEl.textContent = meta.subtitle;
  }
  const breadcrumbEl = document.getElementById("breadcrumb-current");
  if (breadcrumbEl) {
    breadcrumbEl.textContent = meta.title;
  }

  document.querySelectorAll(".sidebar-link[data-page], .tab[data-page]").forEach((link) => {
    const isActive = String(link.dataset.page || "") === normalized;
    link.classList.toggle("active", isActive);
    if (isActive) {
      link.setAttribute("aria-current", "page");
    } else {
      link.removeAttribute("aria-current");
    }
  });

  document.querySelectorAll(".panel[data-pages]").forEach((panel) => {
    const pages = String(panel.dataset.pages || "")
      .split(/\s+/)
      .filter(Boolean);
    panel.classList.toggle("is-page-hidden", !pages.includes(normalized));
  });

  if (updateUrlMode) {
    writePageToLocation(normalized, updateUrlMode);
  }
}

function bindPageRouting() {
  const { raw, normalized } = resolvePageFromLocation();
  const needsUrlReplace = raw !== normalized;
  applyDashboardPage(normalized, { updateUrlMode: needsUrlReplace ? "replace" : null });

  document.querySelectorAll(".sidebar-link[data-page], .tab[data-page]").forEach((link) => {
    link.addEventListener("click", (event) => {
      if (
        event.defaultPrevented
        || event.button !== 0
        || event.metaKey
        || event.ctrlKey
        || event.shiftKey
        || event.altKey
      ) {
        return;
      }
      const page = String(link.dataset.page || "").trim().toLowerCase();
      if (!Object.prototype.hasOwnProperty.call(PAGE_CONFIG, page)) {
        return;
      }
      event.preventDefault();
      if (page === activeDashboardPage) {
        return;
      }
      applyDashboardPage(page, { updateUrlMode: "push" });
      window.scrollTo({ top: 0, behavior: "auto" });
    });
  });

  window.addEventListener("popstate", () => {
    const resolved = resolvePageFromLocation();
    applyDashboardPage(resolved.normalized);
  });
}

function showToast(message, type, duration) {
  if (type === undefined) type = "info";
  if (duration === undefined) duration = 4500;
  var toast = document.createElement("div");
  toast.className = "toast " + type;
  toast.innerHTML =
    '<svg class="toast-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round">' +
    (toastIcons[type] || toastIcons.info) +
    "</svg>" +
    '<span class="toast-body">' + escapeHtml(message) + "</span>" +
    '<button class="toast-dismiss" aria-label="Dismiss">&times;</button>';
  toast.querySelector(".toast-dismiss").addEventListener("click", function () {
    dismissToast(toast);
  });
  toastContainer.appendChild(toast);
  if (duration > 0) {
    setTimeout(function () { dismissToast(toast); }, duration);
  }
}

function dismissToast(toast) {
  if (toast.classList.contains("is-leaving")) return;
  toast.classList.add("is-leaving");
  toast.addEventListener("transitionend", function () { toast.remove(); }, { once: true });
  setTimeout(function () { toast.remove(); }, 500);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeDocTypeKey(value) {
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function parseListInput(value) {
  return String(value || "")
    .split(/[,\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function debounce(fn, delay = 220) {
  let timeoutId;
  return (...args) => {
    window.clearTimeout(timeoutId);
    timeoutId = window.setTimeout(() => fn(...args), delay);
  };
}

function formatListInput(items) {
  if (!items || !items.length) return "";
  return items.join(", ");
}

function sortedRuleKeys(rules) {
  const keys = Object.keys(rules || {});
  return keys.sort((a, b) => {
    if (a === "other") return 1;
    if (b === "other") return -1;
    return a.localeCompare(b);
  });
}

function ensureOtherRule(rules) {
  if (!rules.other) {
    rules.other = {
      keywords: [],
      department: "General Intake",
      required_fields: ["applicant_name", "date"],
      sla_days: null,
    };
  }
  return rules;
}

function buildRuleRowHtml(docType, rule) {
  const locked = docType === "other";
  const safeType = escapeHtml(docType);
  const safeDepartment = escapeHtml(rule.department || "");
  const safeKeywords = escapeHtml(formatListInput(rule.keywords || []));
  const safeRequired = escapeHtml(formatListInput(rule.required_fields || []));
  const safeSlaDays = rule.sla_days === null || rule.sla_days === undefined ? "" : escapeHtml(String(rule.sla_days));

  return `
    <tr class="rule-row" data-rule-row="${safeType}">
      <td data-label="Document Type">
        <input class="rule-doc-type rule-input" value="${safeType}" ${locked ? "readonly" : ""} />
        <input type="hidden" class="rule-required" value="${safeRequired}" />
      </td>
      <td data-label="Department">
        <input class="rule-department rule-input" value="${safeDepartment}" placeholder="e.g. City Clerk" />
      </td>
      <td data-label="Keywords">
        <input class="rule-keywords rule-input" value="${safeKeywords}" placeholder="permit, construction, site plan" />
      </td>
      <td data-label="SLA Days">
        <input class="rule-sla-days rule-input" type="number" min="0" value="${safeSlaDays}" placeholder="e.g. 10" />
      </td>
      <td class="rule-actions-cell">
        <button type="button" class="rule-expand-btn" title="Edit required fields" aria-label="Edit required fields">
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M4 6l4 4 4-4"/></svg>
        </button>
        <button type="button" class="rule-remove" ${locked ? "disabled" : ""} title="Remove rule" aria-label="Remove rule">&times;</button>
      </td>
    </tr>
  `;
}

function renderRulesBuilder(rules) {
  const keys = sortedRuleKeys(rules);
  if (!keys.length) {
    rulesBuilder.innerHTML = '<p class="status">No rules loaded.</p>';
    return;
  }

  const rows = keys.map((docType) => buildRuleRowHtml(docType, rules[docType] || {})).join("");
  rulesBuilder.innerHTML = `
    <div class="table-scroll">
      <table class="rules-table">
        <thead>
          <tr>
            <th>Document Type</th>
            <th>Route To Department</th>
            <th>Trigger Keywords</th>
            <th>SLA Days</th>
            <th></th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function collectRulesFromBuilder() {
  const rows = Array.from(rulesBuilder.querySelectorAll(".rule-row"));
  if (!rows.length) {
    throw new Error("Add at least one document type before saving.");
  }

  const parsed = {};

  rows.forEach((row) => {
    const docTypeInput = row.querySelector(".rule-doc-type");
    const departmentInput = row.querySelector(".rule-department");
    const keywordsInput = row.querySelector(".rule-keywords");
    const requiredInput = row.querySelector(".rule-required");
    const slaDaysInput = row.querySelector(".rule-sla-days");

    const docType = normalizeDocTypeKey(docTypeInput.value);
    if (!docType) {
      throw new Error("Every rule needs a document type key.");
    }

    if (parsed[docType]) {
      throw new Error(`Duplicate document type key: ${docType}`);
    }

    const department = String(departmentInput.value || "").trim() || "General Intake";
    const keywords = parseListInput(keywordsInput ? keywordsInput.value : "").map((item) => item.toLowerCase());
    const requiredFields = parseListInput(requiredInput ? requiredInput.value : "");
    const parsedSlaDays = Number.parseInt(String(slaDaysInput ? slaDaysInput.value : "").trim(), 10);
    const slaDays = Number.isFinite(parsedSlaDays) && parsedSlaDays >= 0 ? parsedSlaDays : null;

    parsed[docType] = {
      keywords,
      department,
      required_fields: requiredFields,
      sla_days: slaDays,
    };
  });

  return ensureOtherRule(parsed);
}

function syncJsonFromBuilder(showErrors = false) {
  try {
    const parsed = collectRulesFromBuilder();
    rulesJson.value = JSON.stringify(parsed, null, 2);
    return parsed;
  } catch (error) {
    if (showErrors) {
      rulesStatus.textContent = `Rules form has an issue: ${error.message}`;
    }
    return null;
  }
}

function coerceRuleSet(candidate) {
  if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
    throw new Error("Rules JSON must be an object keyed by document type.");
  }

  const output = {};
  for (const [key, rawRule] of Object.entries(candidate)) {
    const docType = normalizeDocTypeKey(key);
    if (!docType) continue;

    if (!rawRule || typeof rawRule !== "object" || Array.isArray(rawRule)) {
      throw new Error(`Rule for '${key}' must be an object.`);
    }

    const keywords = Array.isArray(rawRule.keywords)
      ? rawRule.keywords.map((item) => String(item).trim().toLowerCase()).filter(Boolean)
      : parseListInput(rawRule.keywords || "").map((item) => item.toLowerCase());

    const requiredFields = Array.isArray(rawRule.required_fields)
      ? rawRule.required_fields.map((item) => String(item).trim()).filter(Boolean)
      : parseListInput(rawRule.required_fields || "");

    const department = String(rawRule.department || "").trim() || "General Intake";
    const parsedSlaDays = Number.parseInt(String(rawRule.sla_days ?? "").trim(), 10);
    const slaDays = Number.isFinite(parsedSlaDays) && parsedSlaDays >= 0 ? parsedSlaDays : null;

    output[docType] = {
      keywords,
      department,
      required_fields: requiredFields,
      sla_days: slaDays,
    };
  }

  return ensureOtherRule(output);
}

function generateNewTypeKey(rules) {
  let index = 1;
  while (rules[`new_type_${index}`]) {
    index += 1;
  }
  return `new_type_${index}`;
}

async function parseJSON(response) {
  const data = await response.json();
  if (!response.ok) {
    const message = data.detail || "Request failed";
    throw new Error(message);
  }
  return data;
}

function setAuthToken(token) {
  authToken = String(token || "").trim();
  if (authToken) {
    window.localStorage.setItem(AUTH_TOKEN_KEY, authToken);
  } else {
    window.localStorage.removeItem(AUTH_TOKEN_KEY);
  }
}

async function promptLogin() {
  // Clear stale/invalid tokens before redirecting to avoid redirect loops.
  setAuthToken("");
  if (authRedirectInProgress) {
    return false;
  }
  authRedirectInProgress = true;
  // Redirect to the landing page for login instead of using window.prompt.
  window.location.href = "/?login=1";
  return false;
}

function logout() {
  setAuthToken("");
  window.location.href = "/?logout=1";
}

async function apiFetch(input, init = {}, options = {}) {
  const requestInit = { ...init };
  const headers = new Headers(requestInit.headers || {});
  if (authToken) {
    headers.set("Authorization", `Bearer ${authToken}`);
  }
  requestInit.headers = headers;

  let response = await window.fetch(input, requestInit);
  if (response.status !== 401 || options.skipAuthRetry) {
    return response;
  }

  const loggedIn = await promptLogin();
  if (!loggedIn) {
    return response;
  }

  const retryInit = { ...init };
  const retryHeaders = new Headers(retryInit.headers || {});
  if (authToken) {
    retryHeaders.set("Authorization", `Bearer ${authToken}`);
  }
  retryInit.headers = retryHeaders;
  return window.fetch(input, retryInit);
}

function setText(id, value) {
  document.getElementById(id).textContent = value;
}

function percent(value) {
  const safe = Number(value || 0);
  return `${Math.round(safe * 100)}%`;
}

function statusLabel(status) {
  return String(status || "")
    .replaceAll("_", " ")
    .trim();
}

function isDocumentOverdue(doc) {
  if (!doc || !doc.due_date || OVERDUE_DONE_STATUSES.has(String(doc.status || ""))) {
    return false;
  }
  const dueDate = new Date(doc.due_date);
  return !Number.isNaN(dueDate.valueOf()) && dueDate.valueOf() < Date.now();
}

function statusBadgeClass(doc) {
  if (isDocumentOverdue(doc)) return "status-badge overdue";
  if (doc.status === "failed") return "status-badge danger";
  if (doc.requires_review) return "status-badge review";
  if (doc.status === "approved" || doc.status === "corrected") return "status-badge approved";
  if (doc.status === "acknowledged") return "status-badge acknowledged";
  if (doc.status === "assigned") return "status-badge assigned";
  if (doc.status === "in_progress") return "status-badge in-progress";
  if (doc.status === "completed") return "status-badge completed";
  if (doc.status === "archived") return "status-badge archived";
  if (doc.status === "routed") return "status-badge routed";
  return "status-badge";
}

function lineList(items) {
  if (!items || !items.length) {
    return "-";
  }
  return items.map((item) => `- ${item}`).join("\n");
}

function safeDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) {
    return value;
  }
  return date.toLocaleString();
}

function userLabel(user) {
  if (!user) return "";
  const name = String(user.full_name || "").trim();
  const email = String(user.email || "").trim();
  return name ? `${name} (${email})` : email;
}

function userNameById(userId) {
  const id = String(userId || "").trim();
  if (!id) return "Unassigned";
  const user = userCache.find((candidate) => String(candidate.id || "") === id);
  if (!user) return id;
  return String(user.full_name || user.email || id);
}

function setBulkSelectedCountLabel() {
  if (!bulkSelectedCount) return;
  bulkSelectedCount.textContent = `${bulkSelected.size} selected`;
}

function populateAssignOptions() {
  if (!reviewAssignedTo) return;
  const selected = reviewAssignedTo.value;
  const options = ['<option value="">(Unassigned)</option>'];
  userCache.forEach((user) => {
    const id = escapeHtml(String(user.id || ""));
    const label = escapeHtml(userLabel(user));
    options.push(`<option value="${id}">${label}</option>`);
  });
  reviewAssignedTo.innerHTML = options.join("");
  if (selected && Array.from(reviewAssignedTo.options).some((opt) => opt.value === selected)) {
    reviewAssignedTo.value = selected;
  }
}

async function loadCurrentUser() {
  try {
    const me = await parseJSON(await apiFetch("/api/auth/me"));
    currentUserId = String(me.id || "");
    currentUserEmail = String(me.email || "");
    return me;
  } catch {
    currentUserId = "";
    currentUserEmail = "";
    return null;
  }
}

async function loadUsers() {
  try {
    const data = await parseJSON(await apiFetch("/api/auth/users?limit=300"));
    userCache = Array.isArray(data.items) ? data.items : [];
  } catch {
    userCache = [];
  }
  if (currentUserId && !userCache.some((item) => String(item.id || "") === currentUserId)) {
    userCache.push({
      id: currentUserId,
      email: currentUserEmail || currentUserId,
      full_name: "",
    });
  }
  populateAssignOptions();
}

async function loadWatcherStatus() {
  if (!watcherStatus) return;
  try {
    const data = await parseJSON(await apiFetch("/api/watcher/status"));
    const enabled = Boolean(data.enabled);
    const dir = data.watch_dir || "(not set)";
    const count = Number(data.files_ingested || 0);
    watcherStatus.textContent = enabled
      ? `Watcher enabled · ${dir} · ${count} file${count === 1 ? "" : "s"} ingested`
      : `Watcher disabled · ${dir} · ${count} file${count === 1 ? "" : "s"} ingested`;
    watcherStatus.classList.toggle("enabled", enabled);
    watcherStatus.classList.toggle("disabled", !enabled);
  } catch (error) {
    watcherStatus.textContent = `Watcher status unavailable: ${error.message}`;
    watcherStatus.classList.remove("enabled", "disabled");
  }
}

function setBulkMode(enabled) {
  bulkMode = Boolean(enabled);
  if (!bulkMode) {
    bulkSelected.clear();
    if (bulkSelectAll) bulkSelectAll.checked = false;
  }
  if (bulkToolbar) bulkToolbar.hidden = !bulkMode;
  if (bulkToggle) bulkToggle.textContent = bulkMode ? "Exit Multi-select" : "Multi-select";
  setBulkSelectedCountLabel();
}

function selectedBulkIds() {
  return Array.from(bulkSelected).filter((id) => documentsCache.some((doc) => doc.id === id));
}

function setNotificationBadge(count) {
  if (!notificationBadge) return;
  const unread = Math.max(0, Number(count || 0));
  notificationBadge.textContent = unread > 99 ? "99+" : String(unread);
  notificationBadge.hidden = unread <= 0;
}

function renderNotificationItems(items) {
  if (!notificationList) return;
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    notificationList.innerHTML = '<div class="notification-empty">No notifications.</div>';
    return;
  }
  notificationList.innerHTML = list.map((item) => {
    const safeId = escapeHtml(String(item.id));
    const title = escapeHtml(item.title || "Notification");
    const message = escapeHtml(item.message || "");
    const createdAt = escapeHtml(safeDate(item.created_at || ""));
    return `<button type="button" class="notification-item${item.is_read ? "" : " unread"}" data-notification-id="${safeId}" data-document-id="${escapeHtml(String(item.document_id || ""))}">
      <span class="notification-item-title">${title}</span>
      <span class="notification-item-meta">${message || createdAt}</span>
    </button>`;
  }).join("");
}

async function loadNotifications() {
  try {
    const data = await parseJSON(await apiFetch("/api/notifications?limit=30"));
    setNotificationBadge(data.unread_count || 0);
    renderNotificationItems(data.items || []);
  } catch {
    renderNotificationItems([]);
  }
}

async function markAllNotificationsAsRead() {
  await parseJSON(await apiFetch("/api/notifications/read-all", { method: "POST" }));
  await loadNotifications();
}

async function markNotificationAsRead(notificationId) {
  await parseJSON(await apiFetch(`/api/notifications/${notificationId}/read`, { method: "POST" }));
  await loadNotifications();
}

function startNotificationPolling() {
  if (notificationsPollHandle) {
    window.clearInterval(notificationsPollHandle);
  }
  notificationsPollHandle = window.setInterval(() => {
    loadNotifications().catch(() => {});
  }, 30000);
}

function openResponseModal() {
  if (!responseModal) return;
  responseModal.hidden = false;
  setResponseSendStatus("", false);
}

function closeResponseModal() {
  if (!responseModal) return;
  responseModal.hidden = true;
  if (responseSendBtn) responseSendBtn.disabled = false;
  setResponseSendStatus("", false);
}

function setResponseSendStatus(message, isError) {
  if (!responseSendStatus) return;
  responseSendStatus.textContent = message || "";
  responseSendStatus.classList.toggle("flag-error", Boolean(isError) && Boolean(message));
}

async function composeTemplateDraft(templateId, documentId) {
  if (!responseBodyInput || !responseToEmail || !responseSubjectInput) return;
  if (!templateId || !documentId) {
    responseToEmail.value = "";
    responseSubjectInput.value = "";
    responseBodyInput.value = "";
    return;
  }
  try {
    const data = await parseJSON(await apiFetch(`/api/templates/${templateId}/compose/${documentId}`, { method: "POST" }));
    responseToEmail.value = data.to_email || "";
    responseSubjectInput.value = data.subject || "";
    responseBodyInput.value = data.body || "";
    setResponseSendStatus("", false);
  } catch (error) {
    responseBodyInput.value = "";
    setResponseSendStatus(`Template compose failed: ${error.message}`, true);
  }
}

async function sendResponseEmail(documentId) {
  if (!documentId || !responseToEmail || !responseSubjectInput || !responseBodyInput) return;
  const toEmail = String(responseToEmail.value || "").trim();
  const subject = String(responseSubjectInput.value || "").trim();
  const body = String(responseBodyInput.value || "").trim();

  if (!toEmail) {
    setResponseSendStatus("Recipient email is required.", true);
    return;
  }
  if (!subject) {
    setResponseSendStatus("Email subject is required.", true);
    return;
  }
  if (!body) {
    setResponseSendStatus("Message body is required.", true);
    return;
  }

  if (responseSendBtn) responseSendBtn.disabled = true;
  setResponseSendStatus("Sending email...", false);
  try {
    const data = await parseJSON(
      await apiFetch(`/api/documents/${documentId}/send-response`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          to_email: toEmail,
          subject,
          body,
          actor: "dashboard_reviewer",
        }),
      }),
    );
    if (data.status !== "sent") {
      setResponseSendStatus(`Email was not sent: ${data.error || data.status || "unknown error"}.`, true);
      showToast(`Email was not sent: ${data.error || "unknown error"}`, "error");
      return;
    }
    setResponseSendStatus(`Sent to ${toEmail}.`, false);
    showToast(`Response email sent to ${toEmail}`, "success");
    closeResponseModal();
    if (reviewId.value === documentId) {
      const [doc, audit] = await Promise.all([
        parseJSON(await apiFetch(`/api/documents/${documentId}`)),
        parseJSON(await apiFetch(`/api/documents/${documentId}/audit?limit=30`)),
      ]);
      renderReviewDocument(doc, audit.items || []);
    }
    loadNotifications().catch(() => {});
  } catch (error) {
    setResponseSendStatus(`Send failed: ${error.message}`, true);
    showToast(`Send failed: ${error.message}`, "error");
  } finally {
    if (responseSendBtn) responseSendBtn.disabled = false;
  }
}

async function openResponseComposer(documentId) {
  if (!responseTemplateSelect || !responseBodyInput || !responseToEmail || !responseSubjectInput) return;
  openResponseModal();
  setResponseSendStatus("", false);
  responseToEmail.value = "";
  responseSubjectInput.value = "";
  responseBodyInput.value = "Loading templates...";
  try {
    const data = await parseJSON(await apiFetch("/api/templates?limit=200"));
    templateCache = Array.isArray(data.items) ? data.items : [];
    if (!templateCache.length) {
      responseTemplateSelect.innerHTML = "";
      responseBodyInput.value = "";
      setResponseSendStatus("No templates available.", true);
      return;
    }
    responseTemplateSelect.innerHTML = templateCache
      .map((template) => `<option value="${escapeHtml(String(template.id))}">${escapeHtml(template.name)}</option>`)
      .join("");
    responseTemplateSelect.dataset.documentId = documentId;
    await composeTemplateDraft(String(templateCache[0].id), documentId);
  } catch (error) {
    responseBodyInput.value = "";
    setResponseSendStatus(`Failed to load templates: ${error.message}`, true);
  }
}

async function assignDocumentToUser(documentId, userId) {
  const payload = {
    user_id: userId,
    actor: "dashboard_reviewer",
  };
  const updated = await parseJSON(
    await apiFetch(`/api/documents/${documentId}/assign`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
  );
  const audit = await parseJSON(await apiFetch(`/api/documents/${documentId}/audit?limit=30`));
  renderReviewDocument(updated, audit.items || []);
  await loadAll();
}

async function transitionDocumentTo(documentId, targetStatus) {
  const payload = {
    status: targetStatus,
    actor: "dashboard_reviewer",
  };
  const updated = await parseJSON(
    await apiFetch(`/api/documents/${documentId}/transition`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
  );
  const audit = await parseJSON(await apiFetch(`/api/documents/${documentId}/audit?limit=30`));
  renderReviewDocument(updated, audit.items || []);
  await loadAll();
}

async function runBulkAction(action, params = {}) {
  const documentIds = selectedBulkIds();
  if (!documentIds.length) {
    throw new Error("Select at least one document.");
  }
  return parseJSON(
    await apiFetch("/api/documents/bulk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action,
        document_ids: documentIds,
        params,
        actor: "dashboard_reviewer",
      }),
    }),
  );
}

async function runAutomationAutoAssign() {
  if (automationAutoAssignBtn) automationAutoAssignBtn.disabled = true;
  if (automationAssistantStatus) {
    automationAssistantStatus.classList.remove("flag-error");
    automationAssistantStatus.textContent = "Auto-assigning unassigned review documents...";
  }
  try {
    const payload = {
      actor: "dashboard_reviewer",
      limit: 300,
    };
    if (currentUserId) {
      payload.user_id = currentUserId;
    }
    const data = await parseJSON(
      await apiFetch("/api/automation/auto-assign", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
    const label = userNameById(data.assignee || currentUserId || "triage_queue");
    if (automationAssistantStatus) {
      automationAssistantStatus.classList.remove("flag-error");
      automationAssistantStatus.textContent = `Assigned ${data.assigned_count} doc${data.assigned_count === 1 ? "" : "s"} to ${label}. Remaining unassigned: ${data.remaining_unassigned}.`;
    }
    showToast(`Automation Assistant assigned ${data.assigned_count} documents.`, "success");
    await loadAll();
  } catch (error) {
    if (automationAssistantStatus) {
      automationAssistantStatus.classList.add("flag-error");
      automationAssistantStatus.textContent = `Automation failed: ${error.message}`;
    }
    showToast(`Automation failed: ${error.message}`, "error");
  } finally {
    if (automationAutoAssignBtn) automationAutoAssignBtn.disabled = false;
  }
}

async function runAnthropicAutomationSweep() {
  if (!window.confirm("Run Anthropic auto-fill on manual queue items now? This may use API credits.")) {
    return;
  }
  if (automationAnthropicSweepBtn) automationAnthropicSweepBtn.disabled = true;
  if (automationAssistantStatus) {
    automationAssistantStatus.classList.remove("flag-error");
    automationAssistantStatus.textContent = "Running Anthropic auto-fill sweep...";
  }
  try {
    const data = await parseJSON(
      await apiFetch("/api/automation/anthropic-sweep", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          limit: 200,
          include_failed: true,
          actor: "anthropic_automation",
        }),
      }),
    );
    if (automationAssistantStatus) {
      automationAssistantStatus.classList.remove("flag-error");
      automationAssistantStatus.textContent =
        `Anthropic sweep processed ${data.processed_count} docs, auto-cleared ${data.auto_cleared_count}, still manual ${data.still_manual_count}.`;
    }
    showToast(
      `Anthropic sweep: ${data.auto_cleared_count}/${data.processed_count} auto-cleared`,
      data.auto_cleared_count > 0 ? "success" : "info",
    );
    await loadAll();
  } catch (error) {
    const rawMessage = String(error.message || "Request failed");
    const message =
      rawMessage.toLowerCase() === "not found"
        ? "Anthropic sweep endpoint is not available. Ensure the server is running the latest version."
        : `Anthropic sweep failed: ${rawMessage}`;
    if (automationAssistantStatus) {
      automationAssistantStatus.classList.add("flag-error");
      automationAssistantStatus.textContent = message;
    }
    showToast(message, "error");
  } finally {
    if (automationAnthropicSweepBtn) automationAnthropicSweepBtn.disabled = false;
  }
}

function exportSelectedDocumentsCsv() {
  const selectedIds = selectedBulkIds();
  const selectedDocs = documentsCache.filter((doc) => selectedIds.includes(doc.id));
  if (!selectedDocs.length) {
    throw new Error("Select at least one document.");
  }
  const header = ["id", "filename", "status", "doc_type", "department", "assigned_to", "due_date"];
  const rows = selectedDocs.map((doc) => [
    doc.id,
    doc.filename,
    doc.status,
    doc.doc_type || "",
    doc.department || "",
    doc.assigned_to || "",
    doc.due_date || "",
  ]);
  const csv = [header, ...rows]
    .map((row) => row.map((value) => `"${String(value || "").replaceAll('"', '""')}"`).join(","))
    .join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `citysort-bulk-${new Date().toISOString().slice(0, 19).replaceAll(":", "-")}.csv`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function formatUpdatedAt(dateValue = new Date()) {
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(dateValue);
}

function setButtonBusy(button, busyLabel, busy) {
  if (!button) return;
  if (!button.dataset.defaultLabel) {
    button.dataset.defaultLabel = button.textContent || "";
  }
  button.disabled = busy;
  button.textContent = busy ? busyLabel : button.dataset.defaultLabel;
}

function setPlatformStatus(message, isError = false) {
  if (!platformStatus) return;
  platformStatus.textContent = message;
  platformStatus.classList.toggle("error", Boolean(isError));
}

function optionalInputValue(element) {
  if (!element) return null;
  const value = String(element.value || "").trim();
  return value || null;
}

function auditToText(items) {
  if (!items || !items.length) {
    return "-";
  }

  return items
    .map((event) => {
      const parts = [safeDate(event.created_at), event.actor, event.action];
      if (event.details) {
        parts.push(event.details);
      }
      return parts.join(" | ");
    })
    .join("\n");
}

function markSelectedDocumentCard() {
  if (!reviewListScroll) return;
  const cards = reviewListScroll.querySelectorAll(".doc-card[data-doc-id]");
  cards.forEach((card) => {
    card.classList.toggle("is-selected", card.dataset.docId === selectedDocumentId);
  });
}

function showReviewDetail() {
  if (reviewEmptyState) reviewEmptyState.style.display = "none";
  if (reviewDetailContent) reviewDetailContent.style.display = "block";
}

function hideReviewDetail() {
  if (reviewEmptyState) reviewEmptyState.style.display = "flex";
  if (reviewDetailContent) reviewDetailContent.style.display = "none";
}

/* ── Detail-pane tab switching ────────────────────── */

function switchDetailTab(tabName) {
  if (!detailTabs) return;
  detailTabs.querySelectorAll(".detail-tab").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.detailTab === tabName);
  });
  document.querySelectorAll(".detail-tab-panel").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.detailPanel === tabName);
  });
}

/* ── Extracted fields editor ─────────────────────── */

function renderFieldsEditor(extractedFields, missingFields) {
  if (!docFieldsEditor) return;
  const fields = extractedFields && typeof extractedFields === "object" ? extractedFields : {};
  const missing = Array.isArray(missingFields) ? missingFields : [];
  _originalFields = { ...fields };

  const allKeys = new Set([...Object.keys(fields), ...missing]);
  if (!allKeys.size) {
    docFieldsEditor.innerHTML = '<p class="status">No extracted fields available.</p>';
    if (docFieldsSave) docFieldsSave.disabled = true;
    return;
  }

  let html = "";
  for (const key of allKeys) {
    const value = fields[key] !== undefined && fields[key] !== null ? String(fields[key]) : "";
    const isMissing = missing.includes(key) && !value;
    const safeKey = escapeHtml(key);
    const safeValue = escapeHtml(value);
    html += `<div class="doc-field-row" data-field-key="${safeKey}">
      <span class="doc-field-key${isMissing ? " is-missing" : ""}" title="${safeKey}">${safeKey}</span>
      <input class="doc-field-value" data-field="${safeKey}" value="${safeValue}" placeholder="${isMissing ? "Missing \u2014 enter value" : ""}" />
    </div>`;
  }
  docFieldsEditor.innerHTML = html;
  if (docFieldsSave) docFieldsSave.disabled = true;
}

function collectFieldsFromEditor() {
  if (!docFieldsEditor) return {};
  const fields = {};
  docFieldsEditor.querySelectorAll(".doc-field-value").forEach((input) => {
    const key = input.dataset.field;
    if (key) fields[key] = input.value.trim();
  });
  return fields;
}

function hasFieldChanges() {
  const current = collectFieldsFromEditor();
  for (const key of Object.keys(current)) {
    const original = _originalFields[key] !== undefined && _originalFields[key] !== null
      ? String(_originalFields[key]) : "";
    if (current[key] !== original) return true;
  }
  return false;
}

function populateDocTypeOptions(rules) {
  const previousValue = reviewDocType.value;
  const options = ['<option value="">(No change)</option>'];

  sortedRuleKeys(rules).forEach((docType) => {
    options.push(`<option value="${docType}">${docType}</option>`);
  });

  reviewDocType.innerHTML = options.join("");
  if (previousValue && rules[previousValue]) {
    reviewDocType.value = previousValue;
  } else {
    reviewDocType.value = "";
  }
}

function renderTransitionButtons(doc) {
  if (!transitionActions) return;
  const allowedTransitions = TRANSITIONS[String(doc.status || "")] || [];
  if (!allowedTransitions.length) {
    transitionActions.innerHTML = "";
    return;
  }
  transitionActions.innerHTML = allowedTransitions.map((status) => (
    `<button type="button" class="secondary transition-btn" data-transition-status="${escapeHtml(status)}">${escapeHtml(statusLabel(status))}</button>`
  )).join("");
}

function renderReviewDocument(doc, auditItems) {
  selectedDocumentId = doc.id;
  reviewId.value = doc.id;
  reviewDocType.value = "";
  reviewDepartment.value = doc.department || "";
  if (reviewAssignedTo) {
    const assignedTo = String(doc.assigned_to || "");
    if (assignedTo && !Array.from(reviewAssignedTo.options).some((option) => option.value === assignedTo)) {
      const option = document.createElement("option");
      option.value = assignedTo;
      option.textContent = userNameById(assignedTo);
      reviewAssignedTo.appendChild(option);
    }
    reviewAssignedTo.value = assignedTo;
    reviewAssignedTo.dataset.documentId = doc.id;
  }
  reviewNotes.value = doc.reviewer_notes || "";

  const defaultFields = doc.extracted_fields && typeof doc.extracted_fields === "object" ? doc.extracted_fields : {};
  reviewFieldsJson.value = JSON.stringify(defaultFields, null, 2);

  // Detail header
  const overdue = isDocumentOverdue(doc);
  const statusText = overdue
    ? `${statusLabel(doc.status)} (overdue)`
    : `${statusLabel(doc.status)}${doc.requires_review ? " (review)" : ""}`;
  const confidenceVal = Math.max(0, Math.min(Number(doc.confidence || 0), 1));
  const confidencePct = Math.round(confidenceVal * 100);

  reviewFilename.textContent = doc.filename;
  reviewBadgeStatus.textContent = statusText;
  reviewBadgeStatus.className = statusBadgeClass(doc);
  reviewBadgeType.textContent = doc.doc_type || "unclassified";
  reviewBadgeDept.textContent = doc.department || "-";
  reviewConfBar.style.width = confidencePct + "%";
  reviewConfPct.textContent = confidencePct + "%";
  if (reviewDueDate) {
    reviewDueDate.textContent = doc.due_date ? `Due: ${safeDate(doc.due_date)}` : "Due: -";
    reviewDueDate.classList.toggle("overdue", overdue);
  }

  // Urgency badge
  var urgencyBadgeEl = document.getElementById("review-urgency-badge");
  if (urgencyBadgeEl) {
    if (doc.urgency === "high") {
      urgencyBadgeEl.textContent = "HIGH PRIORITY";
      urgencyBadgeEl.className = "urgency-badge urgency-high";
      urgencyBadgeEl.hidden = false;
    } else {
      urgencyBadgeEl.hidden = true;
    }
  }

  // AI classifier rationale badge
  var classifierBadgeEl = document.getElementById("review-classifier-badge");
  if (classifierBadgeEl) {
    var provider = classifierInfo.classifier_provider || "rules";
    if (provider === "anthropic" || provider === "openai") {
      classifierBadgeEl.textContent = "Classified by AI (" + provider + ")";
      classifierBadgeEl.className = "classifier-badge classifier-ai";
    } else {
      classifierBadgeEl.textContent = "Classified by rules";
      classifierBadgeEl.className = "classifier-badge";
    }
    classifierBadgeEl.hidden = false;
  }

  renderTransitionButtons(doc);

  // Progressive disclosure: issues
  const hasMissing = doc.missing_fields && doc.missing_fields.length > 0;
  const hasErrors = doc.validation_errors && doc.validation_errors.length > 0;
  if (reviewIssues) reviewIssues.style.display = (hasMissing || hasErrors) ? "block" : "none";
  if (reviewMissingSection) reviewMissingSection.style.display = hasMissing ? "block" : "none";
  if (reviewErrorsSection) reviewErrorsSection.style.display = hasErrors ? "block" : "none";
  reviewMissing.textContent = lineList(doc.missing_fields);
  reviewErrors.textContent = lineList(doc.validation_errors);

  reviewAudit.textContent = auditToText(auditItems);
  reviewStatus.textContent = "";

  // Populate Document tab
  _currentDocForDocTab = doc;
  if (docTextPreview) {
    docTextPreview.textContent = doc.extracted_text
      ? doc.extracted_text.slice(0, 8000)
      : "No extracted text available.";
  }
  renderFieldsEditor(doc.extracted_fields, doc.missing_fields);
  if (docReuploadStatus) docReuploadStatus.textContent = "";
  if (docFieldsStatus) docFieldsStatus.textContent = "";

  // Reset to Review tab when selecting a new document
  switchDetailTab("review");

  showReviewDetail();
  markSelectedDocumentCard();
}

function clearReviewSelection(message) {
  selectedDocumentId = "";
  reviewId.value = "";
  reviewDocType.value = "";
  reviewDepartment.value = "";
  reviewNotes.value = "";
  reviewFieldsJson.value = "{}";
  reviewMissing.textContent = "-";
  reviewErrors.textContent = "-";
  reviewAudit.textContent = "-";
  reviewStatus.textContent = "";

  // Clear Document tab state
  _currentDocForDocTab = null;
  _originalFields = {};
  if (docTextPreview) docTextPreview.textContent = "-";
  if (docFieldsEditor) docFieldsEditor.innerHTML = "";
  if (docReuploadStatus) docReuploadStatus.textContent = "";
  if (docFieldsStatus) docFieldsStatus.textContent = "";
  if (docFieldsSave) docFieldsSave.disabled = true;
  if (reviewAssignedTo) {
    reviewAssignedTo.value = "";
    reviewAssignedTo.dataset.documentId = "";
  }
  if (reviewDueDate) {
    reviewDueDate.textContent = "Due: -";
    reviewDueDate.classList.remove("overdue");
  }
  if (transitionActions) {
    transitionActions.innerHTML = "";
  }
  switchDetailTab("review");

  hideReviewDetail();
  markSelectedDocumentCard();
}

function connectivitySummary(data) {
  const db = data.database?.status || "unknown";
  const ocr = data.ocr_provider?.status || "unknown";
  const classifier = data.classifier_provider?.status || "unknown";
  const deploy = data.deployment_provider?.status || "unknown";
  return `DB: ${db}, OCR: ${ocr}, Classifier: ${classifier}, Deploy: ${deploy}`;
}

function deriveAutomationStats(data) {
  const total = Number(data?.total_documents || 0);
  const routedOrApproved = Number(data?.routed_or_approved || 0);

  let automationRate = Number(data?.automation_rate);
  if (!Number.isFinite(automationRate) || automationRate < 0 || automationRate > 1) {
    automationRate = total > 0 ? Math.min(1, Math.max(0, routedOrApproved / total)) : 0;
  }

  let manualDocs = Number(data?.manual_documents);
  if (!Number.isFinite(manualDocs) || manualDocs < 0) {
    manualDocs = Math.max(total - routedOrApproved, 0);
  }

  return { automationRate, manualDocs };
}

function renderAutomationAssistant(data) {
  const { automationRate, manualDocs } = deriveAutomationStats(data);
  const reviewDocs = Number(data.needs_review || 0);
  const unassignedDocs = Number(data.manual_unassigned || 0);
  const missingContact = Number(data.missing_contact_email || 0);
  if (automationInlineRate) {
    automationInlineRate.textContent = `Automation: ${percent(automationRate)}`;
  }
  if (automationInlineGap) {
    automationInlineGap.textContent = `Manual workload: ${manualDocs} doc${manualDocs === 1 ? "" : "s"}`;
  }
  if (automationInlineNeedsReview) {
    automationInlineNeedsReview.textContent = `Needs Review: ${reviewDocs}`;
  }
  if (automationInlineUnassigned) {
    automationInlineUnassigned.textContent = `Unassigned: ${unassignedDocs}`;
  }
  if (automationInlineMissingContact) {
    automationInlineMissingContact.textContent = `Missing Contact: ${missingContact}`;
  }
}

var prevMetrics = { total: null, review: null, overdue: null, routed: null, automation: null, confidence: null };

function updateMetric(id, newVal, prevVal, barId, barPct, trendId) {
  var el = document.getElementById(id);
  if (!el) return;
  if (prevVal !== null && newVal !== prevVal) {
    el.classList.add("is-updating");
    setTimeout(function () { el.classList.remove("is-updating"); }, 350);
  }
  var trendEl = document.getElementById(trendId);
  if (trendEl && prevVal !== null) {
    trendEl.classList.remove("trend-up", "trend-down", "is-visible");
    if (newVal > prevVal) { trendEl.classList.add("trend-up", "is-visible"); }
    else if (newVal < prevVal) { trendEl.classList.add("trend-down", "is-visible"); }
  }
  var barEl = document.getElementById(barId);
  if (barEl) { barEl.style.width = Math.min(barPct, 100) + "%"; }
}

async function loadAnalytics() {
  const data = await parseJSON(await apiFetch("/api/analytics"));
  var total = data.total_documents || 0;
  var review = data.needs_review || 0;
  var overdue = data.overdue || 0;
  var routed = data.routed_or_approved || 0;
  var automation = deriveAutomationStats(data).automationRate;
  var conf = data.average_confidence || 0;

  setText("metric-total", String(total));
  setText("metric-review", String(review));
  setText("metric-overdue", String(overdue));
  setText("metric-routed", String(routed));
  setText("metric-automation", percent(automation));
  setText("metric-confidence", percent(conf));
  renderAutomationAssistant(data);

  var maxDoc = Math.max(total, 1);
  updateMetric("metric-total", total, prevMetrics.total, "metric-total-bar", 100, "metric-total-trend");
  updateMetric("metric-review", review, prevMetrics.review, "metric-review-bar", (review / maxDoc) * 100, "metric-review-trend");
  updateMetric("metric-overdue", overdue, prevMetrics.overdue, "metric-overdue-bar", (overdue / maxDoc) * 100, "metric-overdue-trend");
  updateMetric("metric-routed", routed, prevMetrics.routed, "metric-routed-bar", (routed / maxDoc) * 100, "metric-routed-trend");
  updateMetric("metric-automation", Math.round(automation * 100), prevMetrics.automation, "metric-automation-bar", automation * 100, "metric-automation-trend");
  updateMetric("metric-confidence", Math.round(conf * 100), prevMetrics.confidence, "metric-confidence-bar", conf * 100, "metric-confidence-trend");

  prevMetrics.total = total;
  prevMetrics.review = review;
  prevMetrics.overdue = overdue;
  prevMetrics.routed = routed;
  prevMetrics.automation = Math.round(automation * 100);
  prevMetrics.confidence = Math.round(conf * 100);
}

async function loadClassifierInfo() {
  try {
    classifierInfo = await parseJSON(await apiFetch("/api/classifier/info"));
  } catch (e) {
    classifierInfo = { classifier_provider: "rules" };
  }
}

async function loadActivityFeed() {
  var feedEl = document.getElementById("activity-feed");
  if (!feedEl) return;
  try {
    var data = await parseJSON(await apiFetch("/api/activity/recent?limit=12"));
    var items = data.items || [];
    if (!items.length) {
      feedEl.innerHTML = '<span class="review-list-count">No recent activity</span>';
      return;
    }
    feedEl.innerHTML = items.map(function (item) {
      var action = String(item.action || "").replaceAll("_", " ");
      var filename = item.filename || "";
      var ago = timeAgo(item.created_at);
      var actor = item.actor || "system";
      return '<div class="activity-item">' +
        '<span class="activity-action">' + escapeHtml(action) + '</span>' +
        '<span class="activity-time">' + escapeHtml(ago) + '</span>' +
        '<span class="activity-file">' + escapeHtml(filename) + '</span>' +
        '<span class="activity-actor">' + escapeHtml(actor) + '</span>' +
        '</div>';
    }).join("");
  } catch (e) {
    feedEl.innerHTML = '<span class="review-list-count">Activity unavailable</span>';
  }
}

function timeAgo(isoStr) {
  if (!isoStr) return "";
  var d = new Date(isoStr);
  var sec = Math.floor((Date.now() - d.getTime()) / 1000);
  if (sec < 60) return "just now";
  if (sec < 3600) return Math.floor(sec / 60) + "m ago";
  if (sec < 86400) return Math.floor(sec / 3600) + "h ago";
  return Math.floor(sec / 86400) + "d ago";
}

function sortDocumentsByPriority(items) {
  return items.slice().sort(function (a, b) {
    var aUrgent = (a.urgency === "high") ? 1 : 0;
    var bUrgent = (b.urgency === "high") ? 1 : 0;
    if (aUrgent !== bUrgent) return bUrgent - aUrgent;
    var aOverdue = isDocumentOverdue(a) ? 1 : 0;
    var bOverdue = isDocumentOverdue(b) ? 1 : 0;
    if (aOverdue !== bOverdue) return bOverdue - aOverdue;
    var aReview = a.requires_review ? 1 : 0;
    var bReview = b.requires_review ? 1 : 0;
    if (aReview !== bReview) return bReview - aReview;
    return 0;
  });
}

async function quickApproveDoc(docId, ev) {
  if (ev) { ev.stopPropagation(); ev.preventDefault(); }
  try {
    await apiFetch("/api/documents/" + encodeURIComponent(docId) + "/review", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ approve: true, actor: "dashboard_reviewer" }),
    });
    showToast("Document approved", "success");
    await loadAll();
  } catch (err) {
    showToast("Approve failed: " + err.message, "error");
  }
}

function navigateDocList(direction) {
  if (!documentsCache.length) return;
  var idx = documentsCache.findIndex(function (d) { return d.id === selectedDocumentId; });
  var nextIdx = idx + direction;
  if (nextIdx < 0) nextIdx = 0;
  if (nextIdx >= documentsCache.length) nextIdx = documentsCache.length - 1;
  var nextDoc = documentsCache[nextIdx];
  if (nextDoc && nextDoc.id !== selectedDocumentId) {
    selectedDocumentId = nextDoc.id;
    loadDocumentDetail(nextDoc.id);
    markSelectedDocumentCard();
    var card = document.querySelector('.doc-card[data-doc-id="' + nextDoc.id + '"]');
    if (card) card.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }
}

async function loadDocumentDetail(docId) {
  try {
    var doc = await parseJSON(await apiFetch("/api/documents/" + encodeURIComponent(docId)));
    var auditData = await parseJSON(await apiFetch("/api/documents/" + encodeURIComponent(docId) + "/audit?limit=30"));
    renderReviewDocument(doc, auditData.items || []);
  } catch (err) {
    showToast("Failed to load document: " + err.message, "error");
  }
}

let _lastQueueData = [];

async function loadQueues() {
  const data = await parseJSON(await apiFetch("/api/queues"));
  _lastQueueData = data.queues || [];

  const totalAll = _lastQueueData.reduce((s, q) => s + q.total, 0);
  const reviewAll = _lastQueueData.reduce((s, q) => s + q.needs_review, 0);

  if (!_lastQueueData.length) {
    if (queueChips) queueChips.innerHTML = '<span class="review-list-count">No department queues yet</span>';
    if (queueSummaryBody) queueSummaryBody.innerHTML = '<span class="review-list-count">No queues</span>';
    return;
  }

  // Build chip HTML
  const buildChip = (dept, label, total, needsReview, interactive) => {
    const active = dept === activeDepartmentFilter ? " active" : "";
    const tag = interactive ? "button" : "span";
    const reviewBadge = needsReview > 0
      ? `<span class="queue-chip-review">${needsReview}</span>`
      : "";
    return `<${tag} class="queue-chip${active}" data-department="${escapeHtml(dept)}">${escapeHtml(label)} <span class="queue-chip-count">${total}</span>${reviewBadge}</${tag}>`;
  };

  // Main chips (interactive)
  if (queueChips) {
    let html = buildChip("", "All", totalAll, reviewAll, true);
    _lastQueueData.forEach((q) => {
      html += buildChip(q.department || "-", q.department || "-", q.total, q.needs_review, true);
    });
    queueChips.innerHTML = html;
  }

  // Overview summary chips (non-interactive)
  if (queueSummaryBody) {
    let html = "";
    _lastQueueData.forEach((q) => {
      html += buildChip(q.department || "-", q.department || "-", q.total, q.needs_review, false);
    });
    queueSummaryBody.innerHTML = html;
  }
}

async function loadDocuments() {
  const params = new URLSearchParams({ limit: "200" });

  const PSEUDO_FILTERS = ["urgent", "unassigned"];
  if (filterStatus && filterStatus.value && !PSEUDO_FILTERS.includes(filterStatus.value)) {
    params.set("status", filterStatus.value);
  }

  if (activeDepartmentFilter) {
    params.set("department", activeDepartmentFilter);
  }

  if (myDocsChip && myDocsChip.classList.contains("active") && currentUserId) {
    params.set("assigned_to", currentUserId);
  }

  const hasSearchFilter = Boolean(filterSearch && filterSearch.value.trim());
  const hasStatusFilter = Boolean(filterStatus && filterStatus.value);
  const hasDepartmentFilter = Boolean(activeDepartmentFilter);
  const hasAssigneeFilter = Boolean(myDocsChip && myDocsChip.classList.contains("active") && currentUserId);
  const needsIndependentSummary = Boolean(
    reviewSummaryBody
      && (hasSearchFilter || hasStatusFilter || hasDepartmentFilter || hasAssigneeFilter),
  );

  const listData = await parseJSON(await apiFetch(`/api/documents?${params.toString()}`));
  let items = listData.items || [];
  let summaryItems = [];

  if (reviewSummaryBody) {
    if (needsIndependentSummary) {
      const summaryData = await parseJSON(await apiFetch("/api/documents?limit=200"));
      summaryItems = summaryData.items || [];
    } else {
      summaryItems = listData.items || [];
    }
  }

  // Client-side pseudo-filters: urgent, unassigned
  const filterVal = filterStatus ? filterStatus.value : "";
  if (filterVal === "urgent") {
    items = items.filter((doc) => doc.urgency === "high");
  } else if (filterVal === "unassigned") {
    items = items.filter((doc) => !doc.assigned_to || String(doc.assigned_to).trim() === "");
  }

  const search = filterSearch ? filterSearch.value.trim().toLowerCase() : "";
  if (search) {
    items = items.filter((doc) => doc.filename.toLowerCase().includes(search));
  }
  items = sortDocumentsByPriority(items);
  documentsCache = items.slice();

  // Update count
  if (reviewListCount) {
    reviewListCount.textContent = `${items.length} document${items.length !== 1 ? "s" : ""}`;
  }

  const itemIdSet = new Set(items.map((doc) => doc.id));
  Array.from(bulkSelected).forEach((docId) => {
    if (!itemIdSet.has(docId)) {
      bulkSelected.delete(docId);
    }
  });
  setBulkSelectedCountLabel();
  if (bulkSelectAll) {
    bulkSelectAll.checked = items.length > 0 && items.every((doc) => bulkSelected.has(doc.id));
  }

  if (selectedDocumentId && !items.some((doc) => doc.id === selectedDocumentId)) {
    clearReviewSelection();
  }

  if (!items.length) {
    if (reviewListScroll) {
      reviewListScroll.innerHTML = '<div class="review-list-empty">' +
        '<svg viewBox="0 0 48 48" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M14 6h14l10 10v24a4 4 0 01-4 4H14a4 4 0 01-4-4V10a4 4 0 014-4z"/><path d="M28 6v10h10"/><path d="M18 28h12M18 34h8"/></svg>' +
        '<p>No matching documents</p></div>';
    }
  } else {
    // Render document cards in left pane
    if (reviewListScroll) {
      reviewListScroll.innerHTML = items.map((doc) => {
        const overdue = isDocumentOverdue(doc);
        const statusText = overdue
          ? `${statusLabel(doc.status)} (overdue)`
          : `${statusLabel(doc.status)}${doc.requires_review ? " (review)" : ""}`;
        const confidenceValue = Math.max(0, Math.min(Number(doc.confidence || 0), 1));
        const confidencePct = Math.round(confidenceValue * 100);
        const safeDocId = escapeHtml(doc.id);
        const safeFilename = escapeHtml(doc.filename);
        const safeDocType = escapeHtml(doc.doc_type || "unclassified");
        const safeDepartment = escapeHtml(doc.department || "-");
        const safeStatusText = escapeHtml(statusText);
        const assigneeLabel = escapeHtml(userNameById(doc.assigned_to));
        const dueText = doc.due_date ? safeDate(doc.due_date) : "";
        const checkbox = bulkMode
          ? `<input type="checkbox" class="bulk-item-check" data-bulk-doc-id="${safeDocId}" ${bulkSelected.has(doc.id) ? "checked" : ""} />`
          : "";
        const overdueText = overdue ? '<span class="doc-card-overdue">OVERDUE</span>' : "";
        const dueLabel = dueText
          ? `<span class="${overdue ? "doc-card-overdue" : "doc-card-due"}">Due ${escapeHtml(dueText)}</span>`
          : "";
        const isHighUrgency = doc.urgency === "high";
        const urgencyLabel = isHighUrgency ? '<span class="doc-card-urgency">HIGH</span>' : "";
        const cardClasses = [
          "doc-card",
          doc.id === selectedDocumentId ? "is-selected" : "",
          isHighUrgency ? "urgency-high" : "",
          overdue ? "is-overdue-card" : "",
        ].filter(Boolean).join(" ");
        const quickActions = !bulkMode
          ? `<span class="doc-card-actions"><button type="button" class="quick-approve-btn" data-quick-id="${safeDocId}" title="Quick Approve">✓</button></span>`
          : "";
        return `<div class="${cardClasses}" data-doc-id="${safeDocId}">
          <div class="doc-card-top">
            ${checkbox}
            <span class="doc-card-name">${safeFilename}</span>
            ${urgencyLabel}
            <span class="${statusBadgeClass(doc)}">${safeStatusText}</span>
            ${quickActions}
          </div>
          <div class="doc-card-bottom">
            <span class="pill">${safeDocType}</span>
            <span class="doc-card-dept">${safeDepartment}</span>
            <span class="doc-card-assignee">${assigneeLabel}</span>
            ${overdueText}
            ${dueLabel}
            <span class="doc-card-conf">${confidencePct}% <span class="confidence-track"><span style="width:${confidencePct}%"></span></span></span>
          </div>
        </div>`;
      }).join("");
    }
  }

  // Overview: top 5 needing review
  if (reviewSummaryBody) {
    const source = summaryItems.length ? summaryItems : listData.items || [];
    const needsReview = source.filter((d) => d.requires_review || d.status === "needs_review").slice(0, 5);
    if (!needsReview.length) {
      reviewSummaryBody.innerHTML = '<span class="review-list-count">All documents reviewed ✓</span>';
    } else {
      reviewSummaryBody.innerHTML = needsReview.map((doc) => {
        const safeId = escapeHtml(doc.id);
        const summaryStatus = isDocumentOverdue(doc) ? "overdue" : String(doc.status || "");
        return `<div class="review-summary-row" data-doc-id="${safeId}">
          <span class="review-summary-name">${escapeHtml(doc.filename)}</span>
          <span class="pill">${escapeHtml(doc.doc_type || "-")}</span>
          <span class="${statusBadgeClass(doc)}">${escapeHtml(statusLabel(summaryStatus))}</span>
        </div>`;
      }).join("");
    }
  }
}

async function loadRulesConfig() {
  const data = await parseJSON(await apiFetch("/api/config/rules"));
  activeRules = ensureOtherRule(data.rules || {});
  populateDocTypeOptions(activeRules);
  renderRulesBuilder(activeRules);
  rulesMeta.textContent = `Source: ${data.source} | Path: ${data.path}`;
  rulesJson.value = JSON.stringify(activeRules, null, 2);
  rulesStatus.textContent = `Loaded ${Object.keys(activeRules).length} document type rules.`;
}

async function saveRulesConfig() {
  const parsed = syncJsonFromBuilder(true);
  if (!parsed) {
    return;
  }

  try {
    const updated = await parseJSON(
      await apiFetch("/api/config/rules", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rules: parsed, actor: "dashboard_admin" }),
      }),
    );

    activeRules = ensureOtherRule(updated.rules || {});
    populateDocTypeOptions(activeRules);
    renderRulesBuilder(activeRules);
    rulesMeta.textContent = `Source: ${updated.source} | Path: ${updated.path}`;
    rulesJson.value = JSON.stringify(activeRules, null, 2);
    rulesStatus.textContent = "Rules saved. New uploads now use this rule set.";
    showToast("Rules saved successfully", "success");
    await loadAll();
  } catch (error) {
    rulesStatus.textContent = `Failed to save rules: ${error.message}`;
    showToast("Failed to save rules: " + error.message, "error");
  }
}

async function resetRulesConfig() {
  try {
    const updated = await parseJSON(
      await apiFetch("/api/config/rules/reset", {
        method: "POST",
      }),
    );

    activeRules = ensureOtherRule(updated.rules || {});
    populateDocTypeOptions(activeRules);
    renderRulesBuilder(activeRules);
    rulesMeta.textContent = `Source: ${updated.source} | Path: ${updated.path}`;
    rulesJson.value = JSON.stringify(activeRules, null, 2);
    rulesStatus.textContent = "Rules reset to defaults.";
    showToast("Rules reset to defaults", "info");
    await loadAll();
  } catch (error) {
    rulesStatus.textContent = `Failed to reset rules: ${error.message}`;
    showToast("Failed to reset rules: " + error.message, "error");
  }
}

function applyJsonToBuilder() {
  try {
    const parsed = JSON.parse(rulesJson.value || "{}");
    const coerced = coerceRuleSet(parsed);
    activeRules = coerced;
    renderRulesBuilder(activeRules);
    populateDocTypeOptions(activeRules);
    rulesJson.value = JSON.stringify(activeRules, null, 2);
    rulesStatus.textContent = "JSON applied to form editor.";
  } catch (error) {
    rulesStatus.textContent = `Could not apply JSON: ${error.message}`;
  }
}

function addNewRuleType() {
  const current = syncJsonFromBuilder(false) || { ...activeRules };
  const nextKey = generateNewTypeKey(current);
  current[nextKey] = {
    keywords: [],
    department: "General Intake",
    required_fields: ["applicant_name", "date"],
    sla_days: null,
  };

  activeRules = ensureOtherRule(current);
  renderRulesBuilder(activeRules);
  populateDocTypeOptions(activeRules);
  rulesJson.value = JSON.stringify(activeRules, null, 2);
  rulesStatus.textContent = `Added ${nextKey}. Fill it out and click Save Rules.`;

  const newRow = rulesBuilder.querySelector(`[data-rule-row="${escapeHtml(nextKey)}"]`);
  if (newRow) {
    newRow.scrollIntoView({ behavior: "smooth", block: "center" });
    const typeInput = newRow.querySelector(".rule-doc-type");
    if (typeInput) typeInput.focus();
  }
}

function showMetricsSkeleton() {
  ["metric-total", "metric-review", "metric-overdue", "metric-routed", "metric-automation", "metric-confidence"].forEach(function (id) {
    var el = document.getElementById(id);
    if (el) el.innerHTML = '<span class="skeleton skeleton-number"></span>';
  });
}

function showTableSkeleton(tbody, cols, rows) {
  if (!rows) rows = 4;
  var cls = cols === 6 ? "skeleton-row skeleton-row-6" : "skeleton-row";
  var html = "";
  for (var i = 0; i < rows; i++) {
    html += '<tr><td colspan="' + cols + '"><div class="' + cls + '">';
    for (var c = 0; c < cols; c++) {
      html += '<span class="skeleton skeleton-cell"></span>';
    }
    html += '</div></td></tr>';
  }
  tbody.innerHTML = html;
}

async function loadAll() {
  showMetricsSkeleton();
  // Show skeleton in left pane
  if (reviewListScroll) {
    let skeletonHtml = "";
    for (let i = 0; i < 6; i++) {
      skeletonHtml += '<div class="doc-card" style="pointer-events:none"><div class="doc-card-top"><span class="skeleton" style="width:60%;height:0.75rem"></span><span class="skeleton" style="width:4rem;height:0.75rem"></span></div><div class="doc-card-bottom"><span class="skeleton" style="width:4rem;height:0.625rem"></span><span class="skeleton" style="width:5rem;height:0.625rem"></span></div></div>';
    }
    reviewListScroll.innerHTML = skeletonHtml;
  }
  await Promise.all([loadAnalytics(), loadQueues(), loadDocuments(), loadWatcherStatus(), loadClassifierInfo(), loadActivityFeed()]);
  if (uploadStatus) {
    const currentStatus = String(uploadStatus.textContent || "");
    if (
      currentStatus.startsWith("Failed to load dashboard:")
      || currentStatus.startsWith("Refresh failed:")
    ) {
      uploadStatus.textContent = "";
    }
  }
  if (lastUpdated) {
    lastUpdated.textContent = `Updated: ${formatUpdatedAt()}`;
  }
}

async function loadPlatformSummary() {
  try {
    const data = await parseJSON(await apiFetch("/api/platform/summary"));
    if (platformSummaryRetryHandle) {
      window.clearTimeout(platformSummaryRetryHandle);
      platformSummaryRetryHandle = null;
    }
    const latestDeploy = data.latest_deployment ? `Last deploy: ${data.latest_deployment.status}` : "No deploys yet";
    setPlatformStatus(
      `${connectivitySummary(data.connectivity)} | Active keys: ${data.active_api_keys} | Pending invites: ${data.pending_invitations} | ${latestDeploy}`,
    );
  } catch (error) {
    setPlatformStatus(`Platform summary failed: ${error.message}`, true);
    if (!platformSummaryRetryHandle && /rate limit/i.test(String(error.message || ""))) {
      platformSummaryRetryHandle = window.setTimeout(() => {
        platformSummaryRetryHandle = null;
        loadPlatformSummary().catch(() => {});
      }, 5000);
    }
  }
}

function bindDocumentClicks() {
  // Document cards in left pane
  if (reviewListScroll) {
    reviewListScroll.addEventListener("click", async (event) => {
      const checkbox = event.target.closest(".bulk-item-check");
      if (checkbox) {
        const docId = checkbox.dataset.bulkDocId;
        if (docId) {
          if (checkbox.checked) {
            bulkSelected.add(docId);
          } else {
            bulkSelected.delete(docId);
          }
          setBulkSelectedCountLabel();
          if (bulkSelectAll) {
            bulkSelectAll.checked = documentsCache.length > 0 && documentsCache.every((doc) => bulkSelected.has(doc.id));
          }
        }
        return;
      }
      const card = event.target.closest(".doc-card");
      if (!card) return;
      const docId = card.dataset.docId;
      if (!docId) return;

      try {
        const [doc, audit] = await Promise.all([
          parseJSON(await apiFetch(`/api/documents/${docId}`)),
          parseJSON(await apiFetch(`/api/documents/${docId}/audit?limit=30`)),
        ]);
        renderReviewDocument(doc, audit.items || []);
      } catch (error) {
        showToast("Failed to load document: " + error.message, "error");
      }
    });
  }

  // Queue chips click
  if (queueChips) {
    queueChips.addEventListener("click", (event) => {
      const chip = event.target.closest(".queue-chip");
      if (!chip) return;
      queueChips.querySelectorAll(".queue-chip").forEach((c) => c.classList.remove("active"));
      chip.classList.add("active");
      activeDepartmentFilter = chip.dataset.department || "";
      loadDocuments().catch(() => {});
    });
  }

  // Overview summary row clicks → navigate to review
  if (reviewSummaryBody) {
    reviewSummaryBody.addEventListener("click", (event) => {
      const row = event.target.closest(".review-summary-row");
      if (!row) return;
      const docId = row.dataset.docId;
      window.location.href = `/app?page=queues&doc=${encodeURIComponent(docId)}`;
    });
  }
}

function bindFilters() {
  const trigger = () => {
    loadDocuments().catch((error) => {
      console.error("Failed to load documents:", error.message);
    });
  };
  const debouncedTrigger = debounce(trigger, 220);

  if (filterStatus) filterStatus.addEventListener("change", trigger);
  if (filterSearch) filterSearch.addEventListener("input", debouncedTrigger);
  if (myDocsChip) {
    myDocsChip.addEventListener("click", () => {
      if (!currentUserId) {
        showToast("Sign in required to use My Documents.", "warning");
        return;
      }
      myDocsChip.classList.toggle("active");
      trigger();
    });
  }
}

/* ═══════════════════════════════════════════════════
   Connectors — registry, grid, config, test, import
   ═══════════════════════════════════════════════════ */

const _cIcon = {
  postgresql: '<svg viewBox="0 0 40 40" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="20" cy="10" rx="14" ry="5"/><path d="M6 10v20c0 2.76 6.27 5 14 5s14-2.24 14-5V10"/><path d="M6 18c0 2.76 6.27 5 14 5s14-2.24 14-5"/><path d="M6 26c0 2.76 6.27 5 14 5s14-2.24 14-5"/></svg>',
  mysql: '<svg viewBox="0 0 40 40" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="20" cy="10" rx="14" ry="5"/><path d="M6 10v20c0 2.76 6.27 5 14 5s14-2.24 14-5V10"/><path d="M6 20c0 2.76 6.27 5 14 5s14-2.24 14-5"/></svg>',
  sqlite: '<svg viewBox="0 0 40 40" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M10 5h14l8 8v22H10z"/><path d="M24 5v8h8"/><ellipse cx="20" cy="24" rx="6" ry="2.5"/><path d="M14 24v6c0 1.38 2.69 2.5 6 2.5s6-1.12 6-2.5v-6"/></svg>',
  servicenow: '<svg viewBox="0 0 40 40" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="20" cy="20" r="14"/><path d="M20 12v5"/><path d="M20 23v5"/><path d="M12 20h5"/><path d="M23 20h5"/><circle cx="20" cy="20" r="3"/></svg>',
  confluence: '<svg viewBox="0 0 40 40" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M8 12h24v18H8z"/><path d="M12 12V8h16v4"/><path d="M14 18h12"/><path d="M14 22h8"/><path d="M14 26h10"/></svg>',
  salesforce: '<svg viewBox="0 0 40 40" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M10 24c-3 0-5-2.5-5-5.5S7 13 10 13c1-3 4-5 7.5-5 3 0 5.5 1.5 6.5 4 1-.5 2-.8 3-.8 3.5 0 6.5 3 6.5 6.3 0 3.3-3 6.5-6.5 6.5H10z"/></svg>',
  google_cloud_storage: '<svg viewBox="0 0 40 40" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6l14 8v12l-14 8-14-8V14z"/><path d="M20 6v28"/><path d="M6 14l14 8 14-8"/></svg>',
  amazon_s3: '<svg viewBox="0 0 40 40" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M12 8h16v24H12z"/><ellipse cx="20" cy="8" rx="8" ry="3"/><ellipse cx="20" cy="32" rx="8" ry="3"/><path d="M12 20c0 1.66 3.58 3 8 3s8-1.34 8-3"/></svg>',
  jira: '<svg viewBox="0 0 40 40" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6l14 14-14 14L6 20z"/><circle cx="20" cy="20" r="4"/></svg>',
  sharepoint: '<svg viewBox="0 0 40 40" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="20" cy="15" r="8"/><circle cx="14" cy="25" r="6"/><circle cx="26" cy="25" r="6"/></svg>',
};

const CONNECTOR_REGISTRY = {
  postgresql: {
    name: "PostgreSQL",
    category: "database",
    status: "available",
    icon: _cIcon.postgresql,
    fields: [
      { id: "host", label: "Host", type: "text", placeholder: "localhost", required: true },
      { id: "port", label: "Port", type: "number", placeholder: "5432", defaultVal: "5432" },
      { id: "database", label: "Database", type: "text", placeholder: "mydb", required: true },
      { id: "username", label: "Username", type: "text", placeholder: "postgres", required: true },
      { id: "password", label: "Password", type: "password", placeholder: "", required: true, sensitive: true },
    ],
    buildDatabaseUrl: (v) => `postgresql://${encodeURIComponent(v.username || "")}:${encodeURIComponent(v.password || "")}@${v.host || "localhost"}:${v.port || 5432}/${v.database || ""}`,
    hasQuery: true,
  },
  mysql: {
    name: "MySQL",
    category: "database",
    status: "available",
    icon: _cIcon.mysql,
    fields: [
      { id: "host", label: "Host", type: "text", placeholder: "localhost", required: true },
      { id: "port", label: "Port", type: "number", placeholder: "3306", defaultVal: "3306" },
      { id: "database", label: "Database", type: "text", placeholder: "mydb", required: true },
      { id: "username", label: "Username", type: "text", placeholder: "root", required: true },
      { id: "password", label: "Password", type: "password", placeholder: "", required: true, sensitive: true },
    ],
    buildDatabaseUrl: (v) => `mysql+pymysql://${encodeURIComponent(v.username || "")}:${encodeURIComponent(v.password || "")}@${v.host || "localhost"}:${v.port || 3306}/${v.database || ""}`,
    hasQuery: true,
  },
  sqlite: {
    name: "SQLite",
    category: "database",
    status: "available",
    icon: _cIcon.sqlite,
    fields: [
      { id: "filepath", label: "File Path", type: "text", placeholder: "/path/to/database.db", required: true },
    ],
    buildDatabaseUrl: (v) => v.filepath || "",
    hasQuery: true,
  },
  servicenow: {
    name: "ServiceNow",
    category: "saas",
    status: "available",
    icon: _cIcon.servicenow,
    fields: [
      { id: "instance_url", label: "Instance URL", type: "url", placeholder: "https://yourinstance.service-now.com", required: true },
      { id: "username", label: "Username", type: "text", required: true },
      { id: "password", label: "Password", type: "password", required: true, sensitive: true },
      { id: "table_name", label: "Table Name", type: "text", placeholder: "incident", required: true },
    ],
    hasQuery: false,
  },
  confluence: {
    name: "Confluence",
    category: "saas",
    status: "available",
    icon: _cIcon.confluence,
    fields: [
      { id: "base_url", label: "Base URL", type: "url", placeholder: "https://yoursite.atlassian.net/wiki", required: true },
      { id: "email", label: "Email", type: "email", required: true },
      { id: "api_token", label: "API Token", type: "password", required: true, sensitive: true },
      { id: "space_key", label: "Space Key", type: "text", placeholder: "MYSPACE", required: true },
    ],
    hasQuery: false,
  },
  salesforce: {
    name: "Salesforce",
    category: "saas",
    status: "available",
    icon: _cIcon.salesforce,
    fields: [
      { id: "instance_url", label: "Instance URL", type: "url", placeholder: "https://yourorg.salesforce.com", required: true },
      { id: "client_id", label: "Client ID", type: "text", required: true },
      { id: "client_secret", label: "Client Secret", type: "password", required: true, sensitive: true },
      { id: "username", label: "Username", type: "text", required: true },
      { id: "password", label: "Password", type: "password", required: true, sensitive: true },
    ],
    hasQuery: false,
  },
  google_cloud_storage: {
    name: "Google Cloud Storage",
    category: "saas",
    status: "available",
    icon: _cIcon.google_cloud_storage,
    fields: [
      { id: "project_id", label: "Project ID", type: "text", required: true },
      { id: "bucket_name", label: "Bucket Name", type: "text", required: true },
      { id: "service_account_key", label: "Service Account JSON Key", type: "textarea", required: true, sensitive: true },
    ],
    hasQuery: false,
  },
  amazon_s3: {
    name: "Amazon S3",
    category: "saas",
    status: "available",
    icon: _cIcon.amazon_s3,
    fields: [
      { id: "bucket_name", label: "Bucket Name", type: "text", required: true },
      { id: "region", label: "Region", type: "text", placeholder: "us-east-1", required: true },
      { id: "access_key_id", label: "Access Key ID", type: "text", required: true },
      { id: "secret_access_key", label: "Secret Access Key", type: "password", required: true, sensitive: true },
      { id: "prefix", label: "Prefix (optional)", type: "text", placeholder: "documents/" },
    ],
    hasQuery: false,
  },
  jira: {
    name: "Jira",
    category: "saas",
    status: "available",
    icon: _cIcon.jira,
    fields: [
      { id: "base_url", label: "Base URL", type: "url", placeholder: "https://yoursite.atlassian.net", required: true },
      { id: "email", label: "Email", type: "email", required: true },
      { id: "api_token", label: "API Token", type: "password", required: true, sensitive: true },
      { id: "project_key", label: "Project Key", type: "text", placeholder: "PROJ" },
      { id: "jql_query", label: "JQL Query (optional)", type: "text", placeholder: "project = PROJ AND status = Done" },
    ],
    hasQuery: false,
  },
  sharepoint: {
    name: "SharePoint",
    category: "saas",
    status: "available",
    icon: _cIcon.sharepoint,
    fields: [
      { id: "site_url", label: "Site URL", type: "url", placeholder: "https://yourorg.sharepoint.com/sites/...", required: true },
      { id: "client_id", label: "Client ID", type: "text", required: true },
      { id: "client_secret", label: "Client Secret", type: "password", required: true, sensitive: true },
      { id: "tenant_id", label: "Tenant ID", type: "text", required: true },
      { id: "library_name", label: "Library Name", type: "text", placeholder: "Documents" },
    ],
    hasQuery: false,
  },
};

const CONNECTOR_STORAGE_KEY = "citysort_connector_configs";

function _loadConnectorConfig(connectorId) {
  try {
    const all = JSON.parse(localStorage.getItem(CONNECTOR_STORAGE_KEY) || "{}");
    return all[connectorId] || {};
  } catch { return {}; }
}

function _saveConnectorConfig(connectorId, values) {
  const connector = CONNECTOR_REGISTRY[connectorId];
  if (!connector) return;
  const safe = {};
  for (const f of connector.fields) {
    if (!f.sensitive) safe[f.id] = values[f.id] || "";
  }
  try {
    const all = JSON.parse(localStorage.getItem(CONNECTOR_STORAGE_KEY) || "{}");
    all[connectorId] = safe;
    localStorage.setItem(CONNECTOR_STORAGE_KEY, JSON.stringify(all));
  } catch { /* ignore */ }
}

function _getConnectorFieldValues(connectorId) {
  const connector = CONNECTOR_REGISTRY[connectorId];
  if (!connector) return {};
  const values = {};
  for (const f of connector.fields) {
    const el = document.getElementById(`cf-${f.id}`);
    values[f.id] = el ? el.value.trim() : "";
  }
  return values;
}

function _buildConnectorCardHtml(id, connector) {
  const badge = connector.status === "coming_soon"
    ? '<span class="connector-badge coming-soon">Coming Soon</span>'
    : "";
  return `<button type="button" class="connector-card" data-connector-id="${escapeHtml(id)}">${badge}<div class="connector-card-icon">${connector.icon}</div><span class="connector-card-name">${escapeHtml(connector.name)}</span></button>`;
}

function renderConnectorsGrid() {
  if (!connectorsGrid) return;
  const db = [];
  const saas = [];
  for (const [id, c] of Object.entries(CONNECTOR_REGISTRY)) {
    (c.category === "database" ? db : saas).push({ id, ...c });
  }
  let html = '<div class="connectors-category"><h3 class="connectors-category-label">Databases</h3><div class="connectors-card-grid">';
  for (const c of db) html += _buildConnectorCardHtml(c.id, c);
  html += '</div></div>';
  html += '<div class="connectors-category"><h3 class="connectors-category-label">SaaS &amp; Cloud</h3><div class="connectors-card-grid">';
  for (const c of saas) html += _buildConnectorCardHtml(c.id, c);
  html += '</div></div>';
  connectorsGrid.innerHTML = html;
}

function openConnectorConfig(connectorId) {
  const connector = CONNECTOR_REGISTRY[connectorId];
  if (!connector) return;
  activeConnectorId = connectorId;
  connectorsGrid.style.display = "none";
  connectorConfig.style.display = "block";

  connectorConfigHeader.innerHTML = `<div class="connector-config-icon">${connector.icon}</div><div><h3>${escapeHtml(connector.name)}</h3><span class="connector-config-category">${connector.category === "database" ? "Database" : "SaaS Integration"}</span></div>`;

  const saved = _loadConnectorConfig(connectorId);
  let fieldsHtml = '<div class="connector-fields-grid">';
  for (const f of connector.fields) {
    const val = f.sensitive ? "" : (saved[f.id] || f.defaultVal || "");
    if (f.type === "textarea") {
      fieldsHtml += `<div class="connector-field"><label for="cf-${f.id}">${escapeHtml(f.label)}</label><textarea id="cf-${f.id}" ${f.required ? "required" : ""} placeholder="${escapeHtml(f.placeholder || "")}" rows="4">${escapeHtml(val)}</textarea></div>`;
    } else {
      fieldsHtml += `<div class="connector-field"><label for="cf-${f.id}">${escapeHtml(f.label)}</label><input id="cf-${f.id}" type="${f.type || "text"}" ${f.required ? "required" : ""} placeholder="${escapeHtml(f.placeholder || "")}" value="${escapeHtml(val)}" /></div>`;
    }
  }
  fieldsHtml += '</div>';
  connectorConfigFields.innerHTML = fieldsHtml;

  // Show query section for database connectors, limit section for SaaS
  connectorQuerySection.style.display = connector.hasQuery ? "block" : "none";

  // For SaaS connectors without query section, add a limit input
  if (!connector.hasQuery && connector.category === "saas") {
    const limitHtml = `<div class="connector-field" style="margin-top:12px;"><label for="connector-limit">Import Limit</label><input id="connector-limit" type="number" min="1" max="500" value="50" style="width:100px;" /></div>`;
    connectorConfigFields.insertAdjacentHTML("beforeend", limitHtml);
  }

  connectorImportBtn.disabled = false;
  connectorImportBtn.textContent = "Import Documents";
  connectorStatus.textContent = "";
}

function closeConnectorConfig() {
  activeConnectorId = null;
  if (connectorConfig) connectorConfig.style.display = "none";
  if (connectorsGrid) connectorsGrid.style.display = "block";
}

async function _handleDatabaseImport(connector, values) {
  const databaseUrl = connector.buildDatabaseUrl(values);
  const query = optionalInputValue(document.getElementById("connector-query"));
  if (!query) { connectorStatus.textContent = "SQL query is required."; return; }
  if (!/^\s*select\b/i.test(query)) { connectorStatus.textContent = "Use a read-only SELECT query."; return; }

  const sourcePathRadio = document.getElementById("connector-source-path");
  const sourceMode = sourcePathRadio?.checked ? "path" : "content";
  const contentColumn = sourceMode === "content" ? (optionalInputValue(document.getElementById("connector-content-col")) || "content") : null;
  const filePathColumn = sourceMode === "path" ? (optionalInputValue(document.getElementById("connector-path-col")) || "file_path") : null;
  const parsedLimit = Number(document.getElementById("connector-limit")?.value || 500);
  const limit = Math.max(1, Math.min(Number.isFinite(parsedLimit) ? parsedLimit : 500, 5000));

  const payload = {
    database_url: databaseUrl,
    query,
    filename_column: optionalInputValue(document.getElementById("connector-filename-col")) || "filename",
    content_column: contentColumn,
    file_path_column: filePathColumn,
    content_type_column: optionalInputValue(document.getElementById("connector-content-type-col")),
    source_channel: "connector_" + (activeConnectorId || "database"),
    actor: "dashboard_admin",
    process_async: Boolean(document.getElementById("connector-process-async")?.checked),
    limit,
  };

  connectorStatus.textContent = "Importing from database...";
  setButtonBusy(connectorImportBtn, "Importing...", true);

  try {
    const result = await parseJSON(
      await apiFetch("/api/documents/import/database", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
    const msg = `Imported ${result.imported_count} row(s). Failed ${result.failed_count}. Processed ${result.processed_sync_count}, async ${result.scheduled_async_count}.`;
    if (result.errors && result.errors.length) {
      connectorStatus.textContent = `${msg} First error: ${result.errors[0]}`;
      showToast("Import completed with errors", "warning");
    } else {
      connectorStatus.textContent = msg;
      showToast("Import completed: " + result.imported_count + " rows", "success");
    }
    await loadAll();
  } catch (error) {
    connectorStatus.textContent = `Import failed: ${error.message}`;
    showToast("Import failed: " + error.message, "error");
  } finally {
    setButtonBusy(connectorImportBtn, "Importing...", false);
  }
}

async function _handleSaaSImport(connectorId, values) {
  const limit = Number(document.getElementById("connector-limit")?.value || 50);
  const safeLimit = Math.max(1, Math.min(Number.isFinite(limit) ? limit : 50, 500));

  const payload = {
    config: values,
    limit: safeLimit,
    process_async: true,
    actor: "dashboard_admin",
  };

  connectorStatus.textContent = "Importing from " + (CONNECTOR_REGISTRY[connectorId]?.name || connectorId) + "...";
  setButtonBusy(connectorImportBtn, "Importing...", true);

  try {
    const result = await parseJSON(
      await apiFetch(`/api/connectors/${connectorId}/import`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
    const msg = `Imported ${result.imported_count} document(s). Skipped ${result.skipped_count} (already imported). Failed ${result.failed_count}.`;
    if (result.errors && result.errors.length) {
      connectorStatus.textContent = `${msg} First error: ${result.errors[0]}`;
      showToast("Import completed with errors", "warning");
    } else {
      connectorStatus.textContent = msg;
      showToast("Import completed: " + result.imported_count + " documents", "success");
    }
    await loadAll();
  } catch (error) {
    connectorStatus.textContent = `Import failed: ${error.message}`;
    showToast("Import failed: " + error.message, "error");
  } finally {
    setButtonBusy(connectorImportBtn, "Importing...", false);
  }
}

function bindConnectors() {
  if (!connectorsGrid) return;
  renderConnectorsGrid();

  connectorsGrid.addEventListener("click", (event) => {
    const card = event.target.closest(".connector-card");
    if (!card) return;
    openConnectorConfig(card.dataset.connectorId);
  });

  connectorBackBtn?.addEventListener("click", closeConnectorConfig);

  connectorTestBtn?.addEventListener("click", async () => {
    if (!activeConnectorId) return;
    const connector = CONNECTOR_REGISTRY[activeConnectorId];
    const values = _getConnectorFieldValues(activeConnectorId);
    _saveConnectorConfig(activeConnectorId, values);

    setButtonBusy(connectorTestBtn, "Testing...", true);
    connectorStatus.textContent = "Testing connection...";

    try {
      const body = { connector_type: activeConnectorId, config: values };
      if (connector.buildDatabaseUrl) body.database_url = connector.buildDatabaseUrl(values);

      const result = await parseJSON(
        await apiFetch(`/api/connectors/${activeConnectorId}/test`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        }),
      );
      connectorStatus.textContent = result.message || "Test complete.";
      showToast(result.message || "Test complete", result.success ? "success" : "warning");
    } catch (error) {
      connectorStatus.textContent = `Test failed: ${error.message}`;
      showToast("Connection test failed", "error");
    } finally {
      setButtonBusy(connectorTestBtn, "Testing...", false);
    }
  });

  connectorConfigForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!activeConnectorId) return;
    const connector = CONNECTOR_REGISTRY[activeConnectorId];
    if (connector.status === "coming_soon") {
      connectorStatus.textContent = "This connector is coming soon. Your settings have been saved.";
      return;
    }
    const values = _getConnectorFieldValues(activeConnectorId);
    _saveConnectorConfig(activeConnectorId, values);
    if (connector.category === "database") {
      await _handleDatabaseImport(connector, values);
    } else {
      await _handleSaaSImport(activeConnectorId, values);
    }
  });
}

function bindRulesActions() {
  rulesLoad.addEventListener("click", () => {
    loadRulesConfig().catch((error) => {
      rulesStatus.textContent = `Failed to load rules: ${error.message}`;
    });
  });

  rulesAdd.addEventListener("click", () => {
    addNewRuleType();
  });

  rulesSave.addEventListener("click", () => {
    saveRulesConfig();
  });

  rulesReset.addEventListener("click", () => {
    resetRulesConfig();
  });

  rulesApplyJson.addEventListener("click", () => {
    applyJsonToBuilder();
  });

  rulesBuilder.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;

    /* ── Remove button ── */
    const removeBtn = target.closest(".rule-remove");
    if (removeBtn) {
      const row = removeBtn.closest(".rule-row");
      if (!row) return;
      const editor = row.nextElementSibling;
      if (editor && editor.classList.contains("rule-required-editor")) editor.remove();
      row.remove();
      syncJsonFromBuilder(false);
      return;
    }

    /* ── Expand / collapse required-fields editor ── */
    const expandBtn = target.closest(".rule-expand-btn");
    if (expandBtn) {
      const row = expandBtn.closest(".rule-row");
      if (!row) return;

      const existing = row.nextElementSibling;
      if (existing && existing.classList.contains("rule-required-editor")) {
        existing.remove();
        return;
      }

      rulesBuilder.querySelectorAll(".rule-required-editor").forEach((el) => el.remove());

      const hiddenInput = row.querySelector(".rule-required");
      const currentValue = hiddenInput ? hiddenInput.value : "";

      const editorRow = document.createElement("tr");
      editorRow.className = "rule-required-editor";
      editorRow.innerHTML = `
        <td colspan="5">
          <label>Required Fields (comma-separated)</label>
          <input class="rule-required-inline rule-input" value="${escapeHtml(currentValue)}" placeholder="applicant_name, date" />
        </td>
      `;
      row.after(editorRow);

      const inlineInput = editorRow.querySelector(".rule-required-inline");
      inlineInput.addEventListener("input", () => {
        hiddenInput.value = inlineInput.value;
        syncJsonFromBuilder(false);
      });
      inlineInput.focus();
    }
  });

  rulesBuilder.addEventListener("input", () => {
    syncJsonFromBuilder(false);
  });
}

/* ── Detail-pane tab & Document tab bindings ─────── */

function bindDetailTabs() {
  if (!detailTabs) return;
  detailTabs.addEventListener("click", (event) => {
    const tab = event.target.closest(".detail-tab");
    if (!tab) return;
    switchDetailTab(tab.dataset.detailTab);
  });
}

function bindDocumentTab() {
  // Download via fetch + blob (preserves auth headers)
  if (docDownloadBtn) {
    docDownloadBtn.addEventListener("click", async () => {
      if (!_currentDocForDocTab) return;
      try {
        const response = await apiFetch(`/api/documents/${_currentDocForDocTab.id}/download`);
        const blob = await response.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = _currentDocForDocTab.filename || "download";
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
      } catch (err) {
        showToast("Download failed: " + err.message, "error");
      }
    });
  }

  // Re-upload
  if (docReuploadInput) {
    docReuploadInput.addEventListener("change", async () => {
      const file = docReuploadInput.files[0];
      if (!file || !_currentDocForDocTab) return;
      if (docReuploadStatus) docReuploadStatus.textContent = "Uploading replacement\u2026";
      const formData = new FormData();
      formData.append("file", file);
      formData.append("reprocess", "true");
      try {
        const updated = await parseJSON(
          await apiFetch(`/api/documents/${_currentDocForDocTab.id}/reupload`, {
            method: "POST",
            body: formData,
          })
        );
        if (docReuploadStatus) docReuploadStatus.textContent = `Replaced with ${file.name}. Document reprocessed.`;
        showToast("Document replaced and reprocessed", "success");
        await loadAll();
        const audit = await parseJSON(await apiFetch(`/api/documents/${updated.id}/audit?limit=30`));
        renderReviewDocument(updated, audit.items || []);
        switchDetailTab("document");
      } catch (error) {
        if (docReuploadStatus) docReuploadStatus.textContent = `Re-upload failed: ${error.message}`;
        showToast("Re-upload failed: " + error.message, "error");
      } finally {
        docReuploadInput.value = "";
      }
    });
  }

  // Fields editor: detect changes
  if (docFieldsEditor) {
    docFieldsEditor.addEventListener("input", (event) => {
      if (!event.target.classList.contains("doc-field-value")) return;
      const key = event.target.dataset.field;
      const original = _originalFields[key] !== undefined && _originalFields[key] !== null
        ? String(_originalFields[key]) : "";
      event.target.classList.toggle("is-modified", event.target.value.trim() !== original);
      if (docFieldsSave) docFieldsSave.disabled = !hasFieldChanges();
    });
  }

  // Save field changes
  if (docFieldsSave) {
    docFieldsSave.addEventListener("click", async () => {
      if (!_currentDocForDocTab) return;
      const correctedFields = collectFieldsFromEditor();
      const payload = {
        approve: true,
        corrected_fields: correctedFields,
        notes: "Fields corrected via Document tab editor",
        actor: "dashboard_reviewer",
      };
      setButtonBusy(docFieldsSave, "Saving\u2026", true);
      if (docFieldsStatus) docFieldsStatus.textContent = "Saving field corrections\u2026";
      try {
        const updated = await parseJSON(
          await apiFetch(`/api/documents/${_currentDocForDocTab.id}/review`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          })
        );
        if (docFieldsStatus) docFieldsStatus.textContent = "Fields saved and document approved.";
        showToast("Field corrections saved", "success");
        await loadAll();
        const audit = await parseJSON(await apiFetch(`/api/documents/${updated.id}/audit?limit=30`));
        renderReviewDocument(updated, audit.items || []);
        switchDetailTab("document");
      } catch (error) {
        if (docFieldsStatus) docFieldsStatus.textContent = `Save failed: ${error.message}`;
        showToast("Save failed: " + error.message, "error");
      } finally {
        setButtonBusy(docFieldsSave, "Saving\u2026", false);
      }
    });
  }
}

function bindAdvancedActions() {
  if (transitionActions) {
    transitionActions.addEventListener("click", async (event) => {
      const button = event.target.closest(".transition-btn");
      if (!button || !reviewId.value) return;
      const targetStatus = String(button.dataset.transitionStatus || "").trim();
      if (!targetStatus) return;
      setButtonBusy(button, "Transitioning...", true);
      try {
        await transitionDocumentTo(reviewId.value, targetStatus);
        reviewStatus.textContent = `Transitioned to ${statusLabel(targetStatus)}.`;
        showToast(`Transitioned to ${statusLabel(targetStatus)}`, "success");
      } catch (error) {
        reviewStatus.textContent = `Transition failed: ${error.message}`;
        showToast(`Transition failed: ${error.message}`, "error");
      } finally {
        setButtonBusy(button, "Transitioning...", false);
      }
    });
  }

  if (reviewAssignedTo) {
    reviewAssignedTo.addEventListener("change", async () => {
      if (!reviewId.value) return;
      const userId = String(reviewAssignedTo.value || "").trim();
      if (!userId) {
        showToast("Select a user to assign this document.", "warning");
        return;
      }
      try {
        await assignDocumentToUser(reviewId.value, userId);
        reviewStatus.textContent = `Assigned to ${userNameById(userId)}.`;
        showToast(`Assigned to ${userNameById(userId)}`, "success");
      } catch (error) {
        reviewStatus.textContent = `Assignment failed: ${error.message}`;
        showToast(`Assignment failed: ${error.message}`, "error");
      }
    });
  }

  if (automationAutoAssignBtn) {
    automationAutoAssignBtn.addEventListener("click", async () => {
      await runAutomationAutoAssign();
    });
  }
  if (automationAnthropicSweepBtn) {
    automationAnthropicSweepBtn.addEventListener("click", async () => {
      await runAnthropicAutomationSweep();
    });
  }

  if (respondButton) {
    respondButton.addEventListener("click", async () => {
      if (!reviewId.value) {
        showToast("Select a document first.", "warning");
        return;
      }
      await openResponseComposer(reviewId.value);
    });
  }

  if (responseTemplateSelect) {
    responseTemplateSelect.addEventListener("change", async () => {
      const templateId = responseTemplateSelect.value;
      const documentId = responseTemplateSelect.dataset.documentId || reviewId.value;
      await composeTemplateDraft(templateId, documentId);
    });
  }

  if (responseCopyBtn) {
    responseCopyBtn.addEventListener("click", async () => {
      if (!responseBodyInput) return;
      try {
        await navigator.clipboard.writeText(responseBodyInput.value || "");
        showToast("Email message copied.", "success");
      } catch {
        showToast("Clipboard copy failed.", "error");
      }
    });
  }

  if (responseSendBtn) {
    responseSendBtn.addEventListener("click", async () => {
      const documentId = (responseTemplateSelect && responseTemplateSelect.dataset.documentId) || reviewId.value;
      if (!documentId) {
        setResponseSendStatus("Select a document first.", true);
        return;
      }
      await sendResponseEmail(documentId);
    });
  }

  if (responseModalClose) {
    responseModalClose.addEventListener("click", closeResponseModal);
  }
  if (responseCancelBtn) {
    responseCancelBtn.addEventListener("click", closeResponseModal);
  }
  if (responseModal) {
    responseModal.addEventListener("click", (event) => {
      if (event.target === responseModal) {
        closeResponseModal();
      }
    });
  }
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && responseModal && !responseModal.hidden) {
      closeResponseModal();
    }
  });

  if (notificationBell) {
    notificationBell.addEventListener("click", async () => {
      const open = notificationPanel && !notificationPanel.hidden;
      if (notificationPanel) notificationPanel.hidden = open;
      if (!open) {
        await loadNotifications();
      }
    });
  }

  if (notificationsMarkAll) {
    notificationsMarkAll.addEventListener("click", async () => {
      try {
        await markAllNotificationsAsRead();
      } catch (error) {
        showToast(`Unable to mark notifications: ${error.message}`, "error");
      }
    });
  }

  if (notificationList) {
    notificationList.addEventListener("click", async (event) => {
      const item = event.target.closest(".notification-item");
      if (!item) return;
      const notificationId = item.dataset.notificationId;
      const documentId = item.dataset.documentId;
      try {
        await markNotificationAsRead(notificationId);
      } catch {
        // Continue navigation even if mark-read fails.
      }
      if (notificationPanel) notificationPanel.hidden = true;
      if (documentId) {
        window.location.href = `/app?page=queues&doc=${encodeURIComponent(documentId)}`;
      }
    });
  }

  document.addEventListener("click", (event) => {
    if (!notificationPanel || notificationPanel.hidden) return;
    if (notificationPanel.contains(event.target) || (notificationBell && notificationBell.contains(event.target))) {
      return;
    }
    notificationPanel.hidden = true;
  });

  if (bulkToggle) {
    bulkToggle.addEventListener("click", async () => {
      setBulkMode(!bulkMode);
      await loadDocuments();
    });
  }

  if (bulkCancelBtn) {
    bulkCancelBtn.addEventListener("click", async () => {
      setBulkMode(false);
      await loadDocuments();
    });
  }

  if (bulkSelectAll) {
    bulkSelectAll.addEventListener("change", async () => {
      if (!bulkMode) return;
      if (bulkSelectAll.checked) {
        documentsCache.forEach((doc) => bulkSelected.add(doc.id));
      } else {
        documentsCache.forEach((doc) => bulkSelected.delete(doc.id));
      }
      setBulkSelectedCountLabel();
      await loadDocuments();
    });
  }

  if (bulkApproveBtn) {
    bulkApproveBtn.addEventListener("click", async () => {
      if (!window.confirm("Approve all selected documents?")) return;
      try {
        const result = await runBulkAction("approve");
        showToast(`Bulk approve complete (${result.success_count} success, ${result.error_count} errors).`, "success");
        await loadAll();
      } catch (error) {
        showToast(`Bulk approve failed: ${error.message}`, "error");
      }
    });
  }

  if (bulkAssignBtn) {
    bulkAssignBtn.addEventListener("click", async () => {
      const defaultTarget = currentUserId || "";
      const userId = window.prompt("Assign selected docs to user ID:", defaultTarget);
      if (!userId) return;
      try {
        const result = await runBulkAction("assign", { user_id: userId.trim() });
        showToast(`Bulk assign complete (${result.success_count} success, ${result.error_count} errors).`, "success");
        await loadAll();
      } catch (error) {
        showToast(`Bulk assign failed: ${error.message}`, "error");
      }
    });
  }

  if (bulkExportBtn) {
    bulkExportBtn.addEventListener("click", () => {
      try {
        exportSelectedDocumentsCsv();
        showToast("CSV export created.", "success");
      } catch (error) {
        showToast(`CSV export failed: ${error.message}`, "error");
      }
    });
  }
}

function bindPlatformActions() {
  if (docsButton) {
    docsButton.addEventListener("click", () => {
      window.open("/docs", "_blank", "noopener,noreferrer");
    });
  }

  if (connectButton) {
    connectButton.addEventListener("click", async () => {
      setButtonBusy(connectButton, "Checking...", true);
      try {
        const data = await parseJSON(
          await apiFetch("/api/platform/connectivity/check", {
            method: "POST",
          }),
        );
        setPlatformStatus(`Connectivity check complete. ${connectivitySummary(data)}`);
      } catch (error) {
        setPlatformStatus(`Connectivity check failed: ${error.message}`, true);
      } finally {
        setButtonBusy(connectButton, "Checking...", false);
      }
    });
  }

  if (deployButton) {
    deployButton.addEventListener("click", async () => {
      setButtonBusy(deployButton, "Deploying...", true);
      try {
        const data = await parseJSON(
          await apiFetch("/api/platform/deployments/manual", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              environment: "production",
              actor: "dashboard_admin",
              notes: "Triggered from dashboard",
            }),
          }),
        );
        setPlatformStatus(`Manual deployment #${data.id} ${data.status}. ${data.details || ""}`);
        await loadPlatformSummary();
      } catch (error) {
        setPlatformStatus(`Manual deploy failed: ${error.message}`, true);
      } finally {
        setButtonBusy(deployButton, "Deploying...", false);
      }
    });
  }

  if (inviteButton) {
    inviteButton.addEventListener("click", async () => {
      const email = window.prompt("Invite user email:");
      if (!email) return;

      setButtonBusy(inviteButton, "Inviting...", true);
      try {
        const data = await parseJSON(
          await apiFetch("/api/platform/invitations", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              email,
              role: "member",
              actor: "dashboard_admin",
              expires_in_days: 14,
            }),
          }),
        );

        setPlatformStatus(`Invite created for ${data.invitation.email}. Expires ${safeDate(data.invitation.expires_at)}.`);
        try {
          await navigator.clipboard.writeText(data.invite_link);
          window.alert(`Invite link copied to clipboard:\n${data.invite_link}`);
        } catch {
          window.prompt("Copy invite link:", data.invite_link);
        }
        await loadPlatformSummary();
      } catch (error) {
        setPlatformStatus(`Invite failed: ${error.message}`, true);
      } finally {
        setButtonBusy(inviteButton, "Inviting...", false);
      }
    });
  }

  if (newKeyButton) {
    newKeyButton.addEventListener("click", async () => {
      const keyName = window.prompt("API key name:", "dashboard-key");
      if (!keyName) return;

      setButtonBusy(newKeyButton, "Creating...", true);
      try {
        const data = await parseJSON(
          await apiFetch("/api/platform/api-keys", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              name: keyName,
              actor: "dashboard_admin",
            }),
          }),
        );

        setPlatformStatus(`API key '${data.api_key.name}' created. Prefix ${data.api_key.key_prefix}.`);
        window.prompt("Copy API key now (it is shown only once):", data.raw_key);
        await loadPlatformSummary();
      } catch (error) {
        setPlatformStatus(`API key creation failed: ${error.message}`, true);
      } finally {
        setButtonBusy(newKeyButton, "Creating...", false);
      }
    });
  }

  if (logoutButton) {
    logoutButton.addEventListener("click", () => {
      logout();
    });
  }
}

uploadForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const files = Array.from(fileInput.files || []);
  if (!files.length) {
    uploadStatus.textContent = "Choose at least one file.";
    return;
  }

  uploadStatus.textContent = "Uploading...";
  setButtonBusy(uploadButton, "Uploading...", true);

  try {
    for (const file of files) {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("source_channel", "upload_portal");
      formData.append("process_async", "false");

      try {
        await parseJSON(
          await apiFetch("/api/documents/upload", {
            method: "POST",
            body: formData,
          }),
        );
        uploadStatus.textContent = `Uploaded ${file.name}`;
        showToast("Uploaded " + file.name, "success");
      } catch (error) {
        uploadStatus.textContent = `Upload failed for ${file.name}: ${error.message}`;
        showToast("Upload failed for " + file.name + ": " + error.message, "error");
        break;
      }
    }

    fileInput.value = "";
    uploadFileList.innerHTML = "";
    uploadButton.disabled = true;
    await loadAll();
  } catch (error) {
    uploadStatus.textContent = `Upload flow failed: ${error.message}`;
    showToast("Upload flow failed: " + error.message, "error");
  } finally {
    setButtonBusy(uploadButton, "Uploading...", false);
  }
});

reviewForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!reviewId.value) {
    reviewStatus.textContent = "Select a document first.";
    return;
  }

  let correctedFields = {};
  const rawFields = reviewFieldsJson.value.trim();
  if (rawFields) {
    try {
      correctedFields = JSON.parse(rawFields);
      if (!correctedFields || typeof correctedFields !== "object" || Array.isArray(correctedFields)) {
        throw new Error("Corrected fields must be a JSON object");
      }
    } catch (error) {
      reviewStatus.textContent = `Corrected fields JSON is invalid: ${error.message}`;
      return;
    }
  }

  const payload = {
    approve: true,
    corrected_doc_type: reviewDocType.value || null,
    corrected_department: reviewDepartment.value || null,
    corrected_fields: correctedFields,
    notes: reviewNotes.value || null,
    actor: "dashboard_reviewer",
  };

  try {
    const updated = await parseJSON(
      await apiFetch(`/api/documents/${reviewId.value}/review`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
    reviewStatus.textContent = "Review saved as approved.";
    showToast("Review saved as approved", "success");
    await loadAll();

    const audit = await parseJSON(await apiFetch(`/api/documents/${updated.id}/audit?limit=30`));
    renderReviewDocument(updated, audit.items || []);
  } catch (error) {
    reviewStatus.textContent = `Review failed: ${error.message}`;
    showToast("Review failed: " + error.message, "error");
  }
});

rejectButton.addEventListener("click", async () => {
  if (!reviewId.value) {
    reviewStatus.textContent = "Select a document first.";
    return;
  }

  const payload = {
    approve: false,
    notes: reviewNotes.value || "Needs additional verification",
    actor: "dashboard_reviewer",
  };

  try {
    const updated = await parseJSON(
      await apiFetch(`/api/documents/${reviewId.value}/review`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
    reviewStatus.textContent = "Document kept in review queue.";
    showToast("Document kept in review queue", "info");
    await loadAll();

    const audit = await parseJSON(await apiFetch(`/api/documents/${updated.id}/audit?limit=30`));
    renderReviewDocument(updated, audit.items || []);
  } catch (error) {
    reviewStatus.textContent = `Action failed: ${error.message}`;
  }
});

reprocessButton.addEventListener("click", async () => {
  if (!reviewId.value) {
    reviewStatus.textContent = "Select a document first.";
    return;
  }

  try {
    const updated = await parseJSON(
      await apiFetch(`/api/documents/${reviewId.value}/reprocess`, {
        method: "POST",
      }),
    );
    reviewStatus.textContent = "Document reprocessed with latest rules/providers.";
    showToast("Document reprocessed successfully", "success");
    await loadAll();
    const audit = await parseJSON(await apiFetch(`/api/documents/${updated.id}/audit?limit=30`));
    renderReviewDocument(updated, audit.items || []);
  } catch (error) {
    reviewStatus.textContent = `Reprocess failed: ${error.message}`;
    showToast("Reprocess failed: " + error.message, "error");
  }
});

refreshButton.addEventListener("click", async () => {
  setButtonBusy(refreshButton, "Refreshing...", true);
  try {
    await Promise.all([loadAll(), loadRulesConfig(), loadPlatformSummary(), loadUsers(), loadNotifications()]);

    if (selectedDocumentId) {
      try {
        const [doc, audit] = await Promise.all([
          parseJSON(await apiFetch(`/api/documents/${selectedDocumentId}`)),
          parseJSON(await apiFetch(`/api/documents/${selectedDocumentId}/audit?limit=30`)),
        ]);
        renderReviewDocument(doc, audit.items || []);
      } catch (error) {
        clearReviewSelection("Previously selected document no longer available.");
        reviewStatus.textContent = `Failed to refresh selected document: ${error.message}`;
      }
    }
  } catch (error) {
    uploadStatus.textContent = `Refresh failed: ${error.message}`;
  } finally {
    setButtonBusy(refreshButton, "Refreshing...", false);
  }
});

// ── Dropzone setup ──────────────────────────────────
function formatFileSize(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / (1024 * 1024)).toFixed(1) + " MB";
}

function renderUploadFileList() {
  var files = Array.from(fileInput.files || []);
  uploadButton.disabled = !files.length;
  if (!files.length) { uploadFileList.innerHTML = ""; return; }
  uploadFileList.innerHTML = files.map(function (f, i) {
    return '<div class="upload-file-item" data-index="' + i + '">' +
      '<span class="file-name">' + escapeHtml(f.name) + '</span>' +
      '<span class="file-size">' + formatFileSize(f.size) + '</span>' +
      '<button type="button" class="file-remove" aria-label="Remove">&times;</button>' +
      '</div>';
  }).join("");
}

function setFilesOnInput(fileArray) {
  var dt = new DataTransfer();
  fileArray.forEach(function (f) { dt.items.add(f); });
  fileInput.files = dt.files;
  renderUploadFileList();
}

if (uploadDropzone) {
  uploadDropzone.addEventListener("click", function (e) {
    if (e.target.classList.contains("file-remove")) return;
    fileInput.click();
  });
  fileInput.addEventListener("change", function () { renderUploadFileList(); });

  uploadDropzone.addEventListener("dragenter", function (e) { e.preventDefault(); uploadDropzone.classList.add("is-dragover"); });
  uploadDropzone.addEventListener("dragover", function (e) { e.preventDefault(); uploadDropzone.classList.add("is-dragover"); });
  uploadDropzone.addEventListener("dragleave", function (e) { e.preventDefault(); uploadDropzone.classList.remove("is-dragover"); });
  uploadDropzone.addEventListener("drop", function (e) {
    e.preventDefault();
    uploadDropzone.classList.remove("is-dragover");
    if (e.dataTransfer && e.dataTransfer.files.length) {
      setFilesOnInput(Array.from(e.dataTransfer.files));
    }
  });

  uploadFileList.addEventListener("click", function (e) {
    var removeBtn = e.target.closest(".file-remove");
    if (!removeBtn) return;
    var item = removeBtn.closest(".upload-file-item");
    var idx = parseInt(item.dataset.index, 10);
    var files = Array.from(fileInput.files || []);
    files.splice(idx, 1);
    setFilesOnInput(files);
  });
}

// ── Theme toggle ────────────────────────────────────
var themeToggle = document.getElementById("theme-toggle");
if (themeToggle) {
  themeToggle.addEventListener("click", function () {
    if (typeof window._toggleTheme === "function") window._toggleTheme();
  });
}

// ── Sidebar collapse toggle ─────────────────────────
var sidebarToggle = document.getElementById("sidebar-toggle");
if (sidebarToggle) {
  var SIDEBAR_KEY = "citysort_sidebar";
  var appShell = document.querySelector(".app-shell");
  // Restore saved state
  if (localStorage.getItem(SIDEBAR_KEY) === "collapsed") {
    appShell.classList.add("sidebar-collapsed");
  }
  sidebarToggle.addEventListener("click", function () {
    appShell.classList.toggle("sidebar-collapsed");
    localStorage.setItem(
      SIDEBAR_KEY,
      appShell.classList.contains("sidebar-collapsed") ? "collapsed" : "expanded"
    );
  });
}

bindFilters();
bindPageRouting();
bindDocumentClicks();
bindConnectors();
bindRulesActions();
bindPlatformActions();
bindDetailTabs();
bindDocumentTab();
bindAdvancedActions();
clearReviewSelection();
setBulkMode(false);

// Quick-approve click delegation on doc list
if (reviewListScroll) {
  reviewListScroll.addEventListener("click", function (ev) {
    var btn = ev.target.closest(".quick-approve-btn");
    if (btn) {
      var docId = btn.dataset.quickId;
      if (docId) quickApproveDoc(docId, ev);
    }
  });
}

// Keyboard shortcuts
document.addEventListener("keydown", function (ev) {
  // Don't fire shortcuts when typing in inputs
  var tag = (ev.target.tagName || "").toLowerCase();
  if (tag === "input" || tag === "textarea" || tag === "select") return;

  var shortcutsModal = document.getElementById("shortcuts-modal");

  if (ev.key === "?") {
    if (shortcutsModal) shortcutsModal.hidden = !shortcutsModal.hidden;
    return;
  }
  if (ev.key === "Escape") {
    if (shortcutsModal && !shortcutsModal.hidden) { shortcutsModal.hidden = true; return; }
    if (responseModal && !responseModal.hidden) { closeResponseModal(); return; }
    if (selectedDocumentId) { clearReviewSelection(); return; }
    return;
  }
  if (ev.key === "j") { navigateDocList(1); return; }
  if (ev.key === "k") { navigateDocList(-1); return; }
  if (ev.key === "a" && selectedDocumentId) {
    quickApproveDoc(selectedDocumentId);
    return;
  }
  if (ev.key === "e" && selectedDocumentId && respondButton) {
    respondButton.click();
    return;
  }
  if (ev.key === "r" && selectedDocumentId && reprocessButton) {
    reprocessButton.click();
    return;
  }
  if (ev.key === "/") {
    ev.preventDefault();
    if (filterSearch) filterSearch.focus();
    return;
  }
});

// Shortcuts modal close button
var shortcutsCloseBtn = document.getElementById("shortcuts-modal-close");
if (shortcutsCloseBtn) {
  shortcutsCloseBtn.addEventListener("click", function () {
    var modal = document.getElementById("shortcuts-modal");
    if (modal) modal.hidden = true;
  });
}

(async () => {
  await loadCurrentUser();
  await loadUsers();
  await Promise.all([loadAll(), loadRulesConfig(), loadPlatformSummary(), loadNotifications()]);
  startNotificationPolling();

  // Deep-link: auto-select a document if ?doc=ID is present
  const docParam = new URLSearchParams(window.location.search).get("doc");
  if (docParam) {
    try {
      const [docResponse, auditResponse] = await Promise.all([
        apiFetch(`/api/documents/${docParam}`),
        apiFetch(`/api/documents/${docParam}/audit?limit=30`),
      ]);
      const [doc, audit] = await Promise.all([
        parseJSON(docResponse),
        parseJSON(auditResponse),
      ]);
      renderReviewDocument(doc, audit.items || []);
    } catch {
      // Ignore deep-link preload errors; dashboard remains usable.
    }
  }
})().catch((error) => {
  uploadStatus.textContent = `Failed to load dashboard: ${error.message}`;
});
