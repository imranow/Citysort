const uploadForm = document.getElementById("upload-form");
const fileInput = document.getElementById("file-input");
const uploadStatus = document.getElementById("upload-status");
const platformStatus = document.getElementById("platform-status");
const queueBody = document.getElementById("queue-body");
const docsBody = document.getElementById("docs-body");
const refreshButton = document.getElementById("refresh");
const lastUpdated = document.getElementById("last-updated");
const docsButton = document.getElementById("docs-btn");
const inviteButton = document.getElementById("invite-btn");
const newKeyButton = document.getElementById("new-key-btn");
const connectButton = document.getElementById("connect-btn");
const deployButton = document.getElementById("deploy-btn");

const filterStatus = document.getElementById("filter-status");
const filterDepartment = document.getElementById("filter-department");
const filterSearch = document.getElementById("filter-search");

const reviewForm = document.getElementById("review-form");
const rejectButton = document.getElementById("reject");
const reprocessButton = document.getElementById("reprocess");
const reviewStatus = document.getElementById("review-status");
const reviewId = document.getElementById("review-id");
const reviewDocType = document.getElementById("review-doc-type");
const reviewDepartment = document.getElementById("review-department");
const reviewNotes = document.getElementById("review-notes");
const reviewFieldsJson = document.getElementById("review-fields-json");
const dbImportForm = document.getElementById("db-import-form");
const dbUrlInput = document.getElementById("db-url");
const dbQueryInput = document.getElementById("db-query");
const dbFilenameColumnInput = document.getElementById("db-filename-column");
const dbContentColumnInput = document.getElementById("db-content-column");
const dbPathColumnInput = document.getElementById("db-path-column");
const dbContentTypeColumnInput = document.getElementById("db-content-type-column");
const dbLimitInput = document.getElementById("db-limit");
const dbProcessAsync = document.getElementById("db-process-async");
const dbImportStatus = document.getElementById("db-import-status");
const dbImportSubmit = document.getElementById("db-import-submit");

const reviewSelected = document.getElementById("review-selected");
const reviewCurrentStatus = document.getElementById("review-current-status");
const reviewCurrentType = document.getElementById("review-current-type");
const reviewCurrentDepartment = document.getElementById("review-current-department");
const reviewCurrentConfidence = document.getElementById("review-current-confidence");
const reviewMissing = document.getElementById("review-missing");
const reviewErrors = document.getElementById("review-errors");
const reviewTextPreview = document.getElementById("review-text-preview");
const reviewAudit = document.getElementById("review-audit");

const rulesMeta = document.getElementById("rules-meta");
const rulesLoad = document.getElementById("rules-load");
const rulesAdd = document.getElementById("rules-add");
const rulesSave = document.getElementById("rules-save");
const rulesReset = document.getElementById("rules-reset");
const rulesApplyJson = document.getElementById("rules-apply-json");
const rulesJson = document.getElementById("rules-json");
const rulesStatus = document.getElementById("rules-status");
const rulesBuilder = document.getElementById("rules-builder");
const uploadButton = uploadForm.querySelector('button[type="submit"]');

let selectedDocumentId = "";
let activeRules = {};

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

  return `
    <div class="rule-row" data-rule-row="${safeType}">
      <div class="rule-row-grid">
        <div>
          <label>Document Type Key</label>
          <input class="rule-doc-type" value="${safeType}" ${locked ? "readonly" : ""} />
          <p class="hint">Use lowercase words and underscores.</p>
        </div>
        <div>
          <label>Department</label>
          <input class="rule-department" value="${safeDepartment}" placeholder="e.g. City Clerk" />
        </div>
        <div>
          <label>Keywords</label>
          <input class="rule-keywords" value="${safeKeywords}" placeholder="comma-separated" />
        </div>
        <div>
          <label>Required Fields</label>
          <input class="rule-required" value="${safeRequired}" placeholder="comma-separated" />
        </div>
      </div>
      <div class="actions">
        <button type="button" class="secondary rule-remove" ${locked ? "disabled" : ""}>Remove</button>
      </div>
    </div>
  `;
}

function renderRulesBuilder(rules) {
  const keys = sortedRuleKeys(rules);
  if (!keys.length) {
    rulesBuilder.innerHTML = '<p class="status">No rules loaded.</p>';
    return;
  }

  rulesBuilder.innerHTML = keys.map((docType) => buildRuleRowHtml(docType, rules[docType] || {})).join("");
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

    const docType = normalizeDocTypeKey(docTypeInput.value);
    if (!docType) {
      throw new Error("Every rule needs a document type key.");
    }

    if (parsed[docType]) {
      throw new Error(`Duplicate document type key: ${docType}`);
    }

    const department = String(departmentInput.value || "").trim() || "General Intake";
    const keywords = parseListInput(keywordsInput.value).map((item) => item.toLowerCase());
    const requiredFields = parseListInput(requiredInput.value);

    parsed[docType] = {
      keywords,
      department,
      required_fields: requiredFields,
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

    output[docType] = {
      keywords,
      department,
      required_fields: requiredFields,
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

function setText(id, value) {
  document.getElementById(id).textContent = value;
}

function percent(value) {
  const safe = Number(value || 0);
  return `${Math.round(safe * 100)}%`;
}

function statusBadgeClass(doc) {
  if (doc.status === "failed") return "status-badge danger";
  if (doc.requires_review) return "status-badge review";
  if (doc.status === "approved" || doc.status === "corrected") return "status-badge approved";
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

function markSelectedDocumentRow() {
  const rows = docsBody.querySelectorAll("tr[data-doc-id]");
  rows.forEach((row) => {
    const isSelected = row.dataset.docId === selectedDocumentId;
    row.classList.toggle("is-selected", isSelected);

    const button = row.querySelector(".review-btn");
    if (button instanceof HTMLButtonElement) {
      button.classList.toggle("active", isSelected);
      button.textContent = isSelected ? "Opened" : "Open";
    }
  });
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

function renderReviewDocument(doc, auditItems) {
  selectedDocumentId = doc.id;
  reviewId.value = doc.id;
  reviewDocType.value = "";
  reviewDepartment.value = doc.department || "";
  reviewNotes.value = doc.reviewer_notes || "";

  const defaultFields = doc.extracted_fields && typeof doc.extracted_fields === "object" ? doc.extracted_fields : {};
  reviewFieldsJson.value = JSON.stringify(defaultFields, null, 2);

  reviewSelected.textContent = `Selected: ${doc.filename}`;
  reviewCurrentStatus.textContent = `Status: ${doc.status}${doc.requires_review ? " (review)" : ""}`;
  reviewCurrentType.textContent = `Type: ${doc.doc_type || "unclassified"}`;
  reviewCurrentDepartment.textContent = `Department: ${doc.department || "-"}`;
  reviewCurrentConfidence.textContent = `Confidence: ${percent(doc.confidence)}`;

  reviewMissing.textContent = lineList(doc.missing_fields);
  reviewErrors.textContent = lineList(doc.validation_errors);

  if (doc.extracted_text) {
    reviewTextPreview.textContent = doc.extracted_text.slice(0, 4000);
  } else {
    reviewTextPreview.textContent = "-";
  }

  reviewAudit.textContent = auditToText(auditItems);
  reviewStatus.textContent = `Reviewing ${doc.filename}`;
  markSelectedDocumentRow();
}

function clearReviewSelection(message) {
  selectedDocumentId = "";
  reviewId.value = "";
  reviewDocType.value = "";
  reviewDepartment.value = "";
  reviewNotes.value = "";
  reviewFieldsJson.value = "{}";
  reviewSelected.textContent = message || "Select a document from the worklist.";
  reviewCurrentStatus.textContent = "Status: -";
  reviewCurrentType.textContent = "Type: -";
  reviewCurrentDepartment.textContent = "Department: -";
  reviewCurrentConfidence.textContent = "Confidence: -";
  reviewMissing.textContent = "-";
  reviewErrors.textContent = "-";
  reviewTextPreview.textContent = "-";
  reviewAudit.textContent = "-";
  markSelectedDocumentRow();
}

function connectivitySummary(data) {
  const db = data.database?.status || "unknown";
  const ocr = data.ocr_provider?.status || "unknown";
  const classifier = data.classifier_provider?.status || "unknown";
  return `DB: ${db}, OCR: ${ocr}, Classifier: ${classifier}`;
}

async function loadAnalytics() {
  const data = await parseJSON(await fetch("/api/analytics"));
  setText("metric-total", String(data.total_documents));
  setText("metric-review", String(data.needs_review));
  setText("metric-routed", String(data.routed_or_approved));
  setText("metric-confidence", percent(data.average_confidence));
}

async function loadQueues() {
  const data = await parseJSON(await fetch("/api/queues"));
  if (!data.queues.length) {
    queueBody.innerHTML = "<tr><td colspan='4'>No queues yet.</td></tr>";
    return;
  }

  queueBody.innerHTML = data.queues
    .map(
      (queue) => `
      <tr>
        <td>${escapeHtml(queue.department || "-")}</td>
        <td>${queue.total}</td>
        <td>${queue.needs_review}</td>
        <td>${queue.ready}</td>
      </tr>
    `,
    )
    .join("");
}

async function loadDocuments() {
  const params = new URLSearchParams({ limit: "200" });

  if (filterStatus.value) {
    params.set("status", filterStatus.value);
  }

  const departmentFilter = filterDepartment.value.trim();
  if (departmentFilter) {
    params.set("department", departmentFilter);
  }

  const data = await parseJSON(await fetch(`/api/documents?${params.toString()}`));
  let items = data.items;

  const search = filterSearch.value.trim().toLowerCase();
  if (search) {
    items = items.filter((doc) => doc.filename.toLowerCase().includes(search));
  }

  if (!items.length) {
    docsBody.innerHTML = "<tr><td colspan='6'>No matching documents.</td></tr>";
    return;
  }

  docsBody.innerHTML = items
    .map((doc) => {
      const statusText = doc.requires_review ? `${doc.status} (review)` : doc.status;
      const confidenceValue = Math.max(0, Math.min(Number(doc.confidence || 0), 1));
      const confidencePct = Math.round(confidenceValue * 100);
      const safeDocId = escapeHtml(doc.id);
      const safeFilename = escapeHtml(doc.filename);
      const safeDocType = escapeHtml(doc.doc_type || "-");
      const safeDepartment = escapeHtml(doc.department || "-");
      const safeStatusText = escapeHtml(statusText);
      return `
        <tr data-doc-id="${safeDocId}">
          <td class="doc-file">${safeFilename}</td>
          <td><span class="pill">${safeDocType}</span></td>
          <td>${safeDepartment}</td>
          <td><span class="${statusBadgeClass(doc)}">${safeStatusText}</span></td>
          <td>
            <div class="confidence-cell">
              <span class="confidence-value">${confidencePct}%</span>
              <span class="confidence-track"><span style="width:${confidencePct}%"></span></span>
            </div>
          </td>
          <td><button class="secondary review-btn" data-id="${safeDocId}">Open</button></td>
        </tr>
      `;
    })
    .join("");

  markSelectedDocumentRow();
}

async function loadRulesConfig() {
  const data = await parseJSON(await fetch("/api/config/rules"));
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
      await fetch("/api/config/rules", {
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
    await loadAll();
  } catch (error) {
    rulesStatus.textContent = `Failed to save rules: ${error.message}`;
  }
}

async function resetRulesConfig() {
  try {
    const updated = await parseJSON(
      await fetch("/api/config/rules/reset", {
        method: "POST",
      }),
    );

    activeRules = ensureOtherRule(updated.rules || {});
    populateDocTypeOptions(activeRules);
    renderRulesBuilder(activeRules);
    rulesMeta.textContent = `Source: ${updated.source} | Path: ${updated.path}`;
    rulesJson.value = JSON.stringify(activeRules, null, 2);
    rulesStatus.textContent = "Rules reset to defaults.";
    await loadAll();
  } catch (error) {
    rulesStatus.textContent = `Failed to reset rules: ${error.message}`;
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
  };

  activeRules = ensureOtherRule(current);
  renderRulesBuilder(activeRules);
  populateDocTypeOptions(activeRules);
  rulesJson.value = JSON.stringify(activeRules, null, 2);
  rulesStatus.textContent = `Added ${nextKey}. Fill it out and click Save Rules.`;
}

async function loadAll() {
  await Promise.all([loadAnalytics(), loadQueues(), loadDocuments()]);
  if (lastUpdated) {
    lastUpdated.textContent = `Updated: ${formatUpdatedAt()}`;
  }
}

async function loadPlatformSummary() {
  try {
    const data = await parseJSON(await fetch("/api/platform/summary"));
    const latestDeploy = data.latest_deployment ? `Last deploy: ${data.latest_deployment.status}` : "No deploys yet";
    setPlatformStatus(
      `${connectivitySummary(data.connectivity)} | Active keys: ${data.active_api_keys} | Pending invites: ${data.pending_invitations} | ${latestDeploy}`,
    );
  } catch (error) {
    setPlatformStatus(`Platform summary failed: ${error.message}`, true);
  }
}

function bindDocumentClicks() {
  docsBody.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (!target.classList.contains("review-btn")) return;

    const docId = target.dataset.id;
    if (!docId) return;

    try {
      const [doc, audit] = await Promise.all([
        parseJSON(await fetch(`/api/documents/${docId}`)),
        parseJSON(await fetch(`/api/documents/${docId}/audit?limit=30`)),
      ]);
      renderReviewDocument(doc, audit.items || []);
    } catch (error) {
      reviewStatus.textContent = `Failed to load document details: ${error.message}`;
    }
  });
}

function bindFilters() {
  const trigger = () => {
    loadDocuments().catch((error) => {
      uploadStatus.textContent = `Failed to load documents: ${error.message}`;
    });
  };
  const debouncedTrigger = debounce(trigger, 220);

  filterStatus.addEventListener("change", trigger);
  filterDepartment.addEventListener("input", debouncedTrigger);
  filterSearch.addEventListener("input", debouncedTrigger);
}

function bindDatabaseImport() {
  if (!dbImportForm) return;

  dbImportForm.addEventListener("submit", async (event) => {
    event.preventDefault();

    const databaseUrl = optionalInputValue(dbUrlInput);
    const query = optionalInputValue(dbQueryInput);
    const filenameColumn = optionalInputValue(dbFilenameColumnInput) || "filename";
    const contentColumn = optionalInputValue(dbContentColumnInput);
    const filePathColumn = optionalInputValue(dbPathColumnInput);

    if (!databaseUrl) {
      dbImportStatus.textContent = "Database URL is required.";
      return;
    }

    if (!query) {
      dbImportStatus.textContent = "SQL query is required.";
      return;
    }

    if (!contentColumn && !filePathColumn) {
      dbImportStatus.textContent = "Set a content column or file path column.";
      return;
    }

    const parsedLimit = Number(dbLimitInput?.value || 500);
    const limit = Math.max(1, Math.min(Number.isFinite(parsedLimit) ? parsedLimit : 500, 5000));

    const payload = {
      database_url: databaseUrl,
      query,
      filename_column: filenameColumn,
      content_column: contentColumn,
      file_path_column: filePathColumn,
      content_type_column: optionalInputValue(dbContentTypeColumnInput),
      source_channel: "database_import_ui",
      actor: "dashboard_admin",
      process_async: Boolean(dbProcessAsync?.checked),
      limit,
    };

    dbImportStatus.textContent = "Importing from database...";
    setButtonBusy(dbImportSubmit, "Importing...", true);

    try {
      const result = await parseJSON(
        await fetch("/api/documents/import/database", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        }),
      );

      const baseMessage =
        `Imported ${result.imported_count} row(s). ` +
        `Failed ${result.failed_count}. ` +
        `Processed now ${result.processed_sync_count}, scheduled async ${result.scheduled_async_count}.`;

      if (result.errors && result.errors.length) {
        dbImportStatus.textContent = `${baseMessage} First error: ${result.errors[0]}`;
      } else {
        dbImportStatus.textContent = baseMessage;
      }

      await loadAll();
    } catch (error) {
      dbImportStatus.textContent = `Database import failed: ${error.message}`;
    } finally {
      setButtonBusy(dbImportSubmit, "Importing...", false);
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
    if (!(target instanceof HTMLElement)) return;
    if (!target.classList.contains("rule-remove")) return;

    const row = target.closest(".rule-row");
    if (!row) return;
    row.remove();
    syncJsonFromBuilder(false);
  });

  rulesBuilder.addEventListener("input", () => {
    syncJsonFromBuilder(false);
  });
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
          await fetch("/api/platform/connectivity/check", {
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
          await fetch("/api/platform/deployments/manual", {
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
          await fetch("/api/platform/invitations", {
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
          await fetch("/api/platform/api-keys", {
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
          await fetch("/api/documents/upload", {
            method: "POST",
            body: formData,
          }),
        );
        uploadStatus.textContent = `Uploaded ${file.name}`;
      } catch (error) {
        uploadStatus.textContent = `Upload failed for ${file.name}: ${error.message}`;
        break;
      }
    }

    fileInput.value = "";
    await loadAll();
  } catch (error) {
    uploadStatus.textContent = `Upload flow failed: ${error.message}`;
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
      await fetch(`/api/documents/${reviewId.value}/review`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
    reviewStatus.textContent = "Review saved as approved.";
    await loadAll();

    const audit = await parseJSON(await fetch(`/api/documents/${updated.id}/audit?limit=30`));
    renderReviewDocument(updated, audit.items || []);
  } catch (error) {
    reviewStatus.textContent = `Review failed: ${error.message}`;
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
      await fetch(`/api/documents/${reviewId.value}/review`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
    reviewStatus.textContent = "Document kept in review queue.";
    await loadAll();

    const audit = await parseJSON(await fetch(`/api/documents/${updated.id}/audit?limit=30`));
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
      await fetch(`/api/documents/${reviewId.value}/reprocess`, {
        method: "POST",
      }),
    );
    reviewStatus.textContent = "Document reprocessed with latest rules/providers.";
    await loadAll();
    const audit = await parseJSON(await fetch(`/api/documents/${updated.id}/audit?limit=30`));
    renderReviewDocument(updated, audit.items || []);
  } catch (error) {
    reviewStatus.textContent = `Reprocess failed: ${error.message}`;
  }
});

refreshButton.addEventListener("click", async () => {
  setButtonBusy(refreshButton, "Refreshing...", true);
  try {
    await Promise.all([loadAll(), loadRulesConfig(), loadPlatformSummary()]);

    if (selectedDocumentId) {
      try {
        const [doc, audit] = await Promise.all([
          parseJSON(await fetch(`/api/documents/${selectedDocumentId}`)),
          parseJSON(await fetch(`/api/documents/${selectedDocumentId}/audit?limit=30`)),
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

bindFilters();
bindDocumentClicks();
bindDatabaseImport();
bindRulesActions();
bindPlatformActions();
clearReviewSelection("Select a document from the worklist.");

Promise.all([loadAll(), loadRulesConfig(), loadPlatformSummary()]).catch((error) => {
  uploadStatus.textContent = `Failed to load dashboard: ${error.message}`;
});
