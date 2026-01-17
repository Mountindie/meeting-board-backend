import { google } from "googleapis";

const ALLOWED_ORIGIN = "https://meeting-board-sl.vercel.app";
const DEFAULT_TAB = "ActiveBoard";
const ACTIVITY_LOG_TAB = "ActivityLog";

function generateId() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `row-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
}

function normalizeHeader(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function normalizeCell(value) {
  return String(value ?? "").trim();
}

function normalizeKey(value) {
  return normalizeCell(value).toLowerCase();
}

function findRowIndexByValue(values, rawTarget) {
  const target = normalizeCell(rawTarget);
  if (!target) return -1;
  const exactIndex = values.findIndex((row) => normalizeCell(row && row[0]) === target);
  if (exactIndex !== -1) return exactIndex;
  const lowerTarget = target.toLowerCase();
  return values.findIndex(
    (row) => normalizeCell(row && row[0]).toLowerCase() === lowerTarget
  );
}

function buildHeaderIndex(headers) {
  const indexMap = {};
  headers.forEach((header, index) => {
    const normalized = normalizeHeader(header);
    if (normalized) indexMap[normalized] = index;
  });
  return indexMap;
}

function buildCompositeLookup(data, headerIndex) {
  const fields = [
    { keys: ["client", "clientname"], value: data.client || data.client_name },
    { keys: ["jobtitle"], value: data.jobTitle || data.job_title },
    { keys: ["candidate", "candidatename"], value: data.candidate || data.candidate_name },
    { keys: ["recruiter"], value: data.recruiter },
    { keys: ["businessline"], value: data.businessLine || data.business_line },
  ];

  return fields
    .map(({ keys, value }) => {
      const index = keys
        .map((key) => headerIndex[key])
        .find((keyIndex) => keyIndex !== undefined);
      const normalizedValue = normalizeKey(value);
      if (index === undefined || !normalizedValue) return null;
      return { index, value: normalizedValue };
    })
    .filter(Boolean);
}

function columnLetter(index) {
  let result = "";
  let value = index + 1;
  while (value > 0) {
    const mod = (value - 1) % 26;
    result = String.fromCharCode(65 + mod) + result;
    value = Math.floor((value - 1) / 26);
  }
  return result;
}

function mapPayloadToRow(headers, payload) {
  const row = new Array(headers.length).fill("");
  headers.forEach((header, index) => {
    const key = normalizeHeader(header);
    switch (key) {
      case "id":
        row[index] = payload.id || "";
        break;
      case "key":
        row[index] = payload.key || payload.id || "";
        break;
      case "client":
        row[index] = payload.client || "";
        break;
      case "clientname":
        row[index] = payload.client || payload.clientName || "";
        break;
      case "firstseendate":
        row[index] = payload.first_seen_date || payload.firstSeenDate || "";
        break;
      case "lastactivedate":
        row[index] = payload.last_active_date || payload.lastActiveDate || "";
        break;
      case "pumpkintier":
        row[index] = payload.pumpkin_tier || payload.pumpkinTier || "";
        break;
      case "pumpkinoverridereason":
        row[index] =
          payload.pumpkin_override_reason || payload.pumpkinOverrideReason || "";
        break;
      case "crfstatus":
        row[index] = payload.crf_status || payload.crfStatus || "";
        break;
      case "crfscore":
        row[index] = payload.crf_score || payload.crfScore || "";
        break;
      case "crflastevaluatedat":
        row[index] =
          payload.crf_last_evaluated_at || payload.crfLastEvaluatedAt || "";
        break;
      case "positiveevents30d":
        row[index] =
          payload.positive_events_30d || payload.positiveEvents30d || "";
        break;
      case "negativeevents30d":
        row[index] =
          payload.negative_events_30d || payload.negativeEvents30d || "";
        break;
      case "hires12m":
        row[index] = payload.hires_12m || payload.hires12m || "";
        break;
      case "hires24m":
        row[index] = payload.hires_24m || payload.hires24m || "";
        break;
      case "lasthiredate":
        row[index] = payload.last_hire_date || payload.lastHireDate || "";
        break;
      case "canonical":
        row[index] = payload.canonical || payload.client || "";
        break;
      case "alias":
        row[index] = payload.alias || "";
        break;
      case "jobtitle":
        row[index] = payload.jobTitle || payload.job_title || "";
        break;
      case "businessline":
        row[index] = payload.businessLine || payload.business_line || "";
        break;
      case "recruiter":
        row[index] = payload.recruiter || "";
        break;
      case "candidate":
        row[index] = payload.candidate || payload.candidate_name || "";
        break;
      case "stage":
        row[index] = payload.stage || "";
        break;
      case "stagedate":
        row[index] = payload.stageDate || payload.stage_date || "";
        break;
      case "risk":
        row[index] = payload.risk || "";
        break;
      case "notes":
        row[index] = payload.notes || payload.action_notes || "";
        break;
      case "noteslog":
      case "noteshistory":
        row[index] = payload.notesLog || payload.notes_log || "";
        break;
      case "tasktext":
        row[index] = payload.taskText || payload.task_text || "";
        break;
      case "taskstatus":
        row[index] = payload.taskStatus || payload.task_status || "";
        break;
      case "taskcreatedat":
        row[index] = payload.taskCreatedAt || payload.task_created_at || "";
        break;
      case "tasklastnotifiedat":
        row[index] =
          payload.taskLastNotifiedAt || payload.task_last_notified_at || "";
        break;
      case "taskfirstnotifiedat":
        row[index] =
          payload.taskFirstNotifiedAt || payload.task_first_notified_at || "";
        break;
      case "tasksecondnotifiedat":
        row[index] =
          payload.taskSecondNotifiedAt || payload.task_second_notified_at || "";
        break;
      case "taskthirdnotifiedat":
        row[index] =
          payload.taskThirdNotifiedAt || payload.task_third_notified_at || "";
        break;
      case "taskcompletedat":
        row[index] = payload.taskCompletedAt || payload.task_completed_at || "";
        break;
      case "taskcompletednotifiedat":
        row[index] =
          payload.taskCompletedNotifiedAt ||
          payload.task_completed_notified_at ||
          "";
        break;
      case "rate":
        row[index] = payload.rate || "";
        break;
      case "createdat":
        row[index] = payload.createdAt || payload.created_at || "";
        break;
      case "createdby":
        row[index] = payload.createdBy || payload.created_by || "";
        break;
      case "updatedat":
        row[index] = payload.updatedAt || payload.updated_at || "";
        break;
      case "updatedby":
        row[index] = payload.updatedBy || payload.updated_by || "";
        break;
      case "startdate":
        row[index] = payload.startDate || payload.start_date || "";
        break;
      case "startyear":
        row[index] = payload.startYear || payload.start_year || "";
        break;
      case "status":
        row[index] = payload.status || "";
        break;
      case "submittedtoam":
        row[index] = payload.submitted_to_am || payload.submittedToAm || "";
        break;
      case "submittedtoclient":
        row[index] = payload.submitted_to_client || payload.submittedToClient || "";
        break;
      case "interviewrequested":
        row[index] = payload.interview_requested || payload.interviewRequested || "";
        break;
      case "interviewrequesteddate":
        row[index] = payload.interview_requested_date || payload.interviewRequestedDate || "";
        break;
      case "interview1":
        row[index] = payload.interview_1 || payload.interview1 || "";
        break;
      case "interview2":
        row[index] = payload.interview_2 || payload.interview2 || "";
        break;
      case "interview3":
        row[index] = payload.interview_3 || payload.interview3 || "";
        break;
      case "offerdate":
        row[index] = payload.offer_date || payload.offerDate || "";
        break;
      case "colextendeddate":
        row[index] = payload.col_extended_date || payload.colExtendedDate || "";
        break;
      case "colsigneddate":
        row[index] = payload.col_signed_date || payload.colSignedDate || "";
        break;
      case "verbalofferdate":
        row[index] = payload.verbal_offer_date || payload.verbalOfferDate || "";
        break;
      case "offerextendeddate":
        row[index] = payload.offer_extended_date || payload.offerExtendedDate || "";
        break;
      case "offeraccepteddate":
        row[index] = payload.offer_accepted_date || payload.offerAcceptedDate || "";
        break;
      case "pendingstartdate":
        row[index] = payload.pendingStartDate || payload.pending_start_date || "";
        break;
      case "clearancecrossoverdate":
        row[index] =
          payload.clearanceCrossoverDate || payload.clearance_crossover_date || "";
        break;
      case "startedworkdate":
        row[index] = payload.startedWorkDate || payload.started_work_date || "";
        break;
      default:
        break;
    }
  });
  return row;
}

function hasOwn(obj, key) {
  return Object.prototype.hasOwnProperty.call(obj, key);
}

function applyPatchToRow(headers, existingRow, patch) {
  const row = existingRow ? [...existingRow] : [];
  while (row.length < headers.length) row.push("");
  headers.forEach((header, index) => {
    const key = normalizeHeader(header);
    switch (key) {
      case "id":
        if (hasOwn(patch, "id")) row[index] = patch.id ?? "";
        break;
      case "key":
        if (hasOwn(patch, "key")) row[index] = patch.key ?? "";
        if (hasOwn(patch, "id")) row[index] = patch.id ?? "";
        break;
      case "client":
        if (hasOwn(patch, "client")) row[index] = patch.client ?? "";
        break;
      case "clientname":
        if (hasOwn(patch, "client")) row[index] = patch.client ?? "";
        if (hasOwn(patch, "clientName")) row[index] = patch.clientName ?? "";
        break;
      case "firstseendate":
        if (hasOwn(patch, "first_seen_date")) {
          row[index] = patch.first_seen_date ?? "";
        }
        if (hasOwn(patch, "firstSeenDate")) row[index] = patch.firstSeenDate ?? "";
        break;
      case "lastactivedate":
        if (hasOwn(patch, "last_active_date")) {
          row[index] = patch.last_active_date ?? "";
        }
        if (hasOwn(patch, "lastActiveDate")) row[index] = patch.lastActiveDate ?? "";
        break;
      case "pumpkintier":
        if (hasOwn(patch, "pumpkin_tier")) row[index] = patch.pumpkin_tier ?? "";
        if (hasOwn(patch, "pumpkinTier")) row[index] = patch.pumpkinTier ?? "";
        break;
      case "pumpkinoverridereason":
        if (hasOwn(patch, "pumpkin_override_reason")) {
          row[index] = patch.pumpkin_override_reason ?? "";
        }
        if (hasOwn(patch, "pumpkinOverrideReason")) {
          row[index] = patch.pumpkinOverrideReason ?? "";
        }
        break;
      case "crfstatus":
        if (hasOwn(patch, "crf_status")) row[index] = patch.crf_status ?? "";
        if (hasOwn(patch, "crfStatus")) row[index] = patch.crfStatus ?? "";
        break;
      case "crfscore":
        if (hasOwn(patch, "crf_score")) row[index] = patch.crf_score ?? "";
        if (hasOwn(patch, "crfScore")) row[index] = patch.crfScore ?? "";
        break;
      case "crflastevaluatedat":
        if (hasOwn(patch, "crf_last_evaluated_at")) {
          row[index] = patch.crf_last_evaluated_at ?? "";
        }
        if (hasOwn(patch, "crfLastEvaluatedAt")) {
          row[index] = patch.crfLastEvaluatedAt ?? "";
        }
        break;
      case "positiveevents30d":
        if (hasOwn(patch, "positive_events_30d")) {
          row[index] = patch.positive_events_30d ?? "";
        }
        if (hasOwn(patch, "positiveEvents30d")) {
          row[index] = patch.positiveEvents30d ?? "";
        }
        break;
      case "negativeevents30d":
        if (hasOwn(patch, "negative_events_30d")) {
          row[index] = patch.negative_events_30d ?? "";
        }
        if (hasOwn(patch, "negativeEvents30d")) {
          row[index] = patch.negativeEvents30d ?? "";
        }
        break;
      case "hires12m":
        if (hasOwn(patch, "hires_12m")) row[index] = patch.hires_12m ?? "";
        if (hasOwn(patch, "hires12m")) row[index] = patch.hires12m ?? "";
        break;
      case "hires24m":
        if (hasOwn(patch, "hires_24m")) row[index] = patch.hires_24m ?? "";
        if (hasOwn(patch, "hires24m")) row[index] = patch.hires24m ?? "";
        break;
      case "lasthiredate":
        if (hasOwn(patch, "last_hire_date")) {
          row[index] = patch.last_hire_date ?? "";
        }
        if (hasOwn(patch, "lastHireDate")) row[index] = patch.lastHireDate ?? "";
        break;
      case "canonical":
        if (hasOwn(patch, "canonical")) row[index] = patch.canonical ?? "";
        if (hasOwn(patch, "client")) row[index] = patch.client ?? "";
        break;
      case "alias":
        if (hasOwn(patch, "alias")) row[index] = patch.alias ?? "";
        break;
      case "jobtitle":
        if (hasOwn(patch, "jobTitle")) row[index] = patch.jobTitle ?? "";
        if (hasOwn(patch, "job_title")) row[index] = patch.job_title ?? "";
        break;
      case "businessline":
        if (hasOwn(patch, "businessLine")) row[index] = patch.businessLine ?? "";
        if (hasOwn(patch, "business_line")) row[index] = patch.business_line ?? "";
        break;
      case "recruiter":
        if (hasOwn(patch, "recruiter")) row[index] = patch.recruiter ?? "";
        break;
      case "candidate":
        if (hasOwn(patch, "candidate")) row[index] = patch.candidate ?? "";
        if (hasOwn(patch, "candidate_name")) row[index] = patch.candidate_name ?? "";
        break;
      case "stage":
        if (hasOwn(patch, "stage")) row[index] = patch.stage ?? "";
        break;
      case "stagedate":
        if (hasOwn(patch, "stageDate")) row[index] = patch.stageDate ?? "";
        if (hasOwn(patch, "stage_date")) row[index] = patch.stage_date ?? "";
        break;
      case "risk":
        if (hasOwn(patch, "risk")) row[index] = patch.risk ?? "";
        break;
      case "notes":
        if (hasOwn(patch, "notes")) row[index] = patch.notes ?? "";
        if (hasOwn(patch, "action_notes")) row[index] = patch.action_notes ?? "";
        break;
      case "noteslog":
      case "noteshistory":
        if (hasOwn(patch, "notesLog")) row[index] = patch.notesLog ?? "";
        if (hasOwn(patch, "notes_log")) row[index] = patch.notes_log ?? "";
        break;
      case "tasktext":
        if (hasOwn(patch, "taskText")) row[index] = patch.taskText ?? "";
        if (hasOwn(patch, "task_text")) row[index] = patch.task_text ?? "";
        break;
      case "taskstatus":
        if (hasOwn(patch, "taskStatus")) row[index] = patch.taskStatus ?? "";
        if (hasOwn(patch, "task_status")) row[index] = patch.task_status ?? "";
        break;
      case "taskcreatedat":
        if (hasOwn(patch, "taskCreatedAt")) row[index] = patch.taskCreatedAt ?? "";
        if (hasOwn(patch, "task_created_at")) row[index] = patch.task_created_at ?? "";
        break;
      case "tasklastnotifiedat":
        if (hasOwn(patch, "taskLastNotifiedAt")) row[index] = patch.taskLastNotifiedAt ?? "";
        if (hasOwn(patch, "task_last_notified_at")) row[index] = patch.task_last_notified_at ?? "";
        break;
      case "taskfirstnotifiedat":
        if (hasOwn(patch, "taskFirstNotifiedAt")) row[index] = patch.taskFirstNotifiedAt ?? "";
        if (hasOwn(patch, "task_first_notified_at")) row[index] = patch.task_first_notified_at ?? "";
        break;
      case "tasksecondnotifiedat":
        if (hasOwn(patch, "taskSecondNotifiedAt")) row[index] = patch.taskSecondNotifiedAt ?? "";
        if (hasOwn(patch, "task_second_notified_at")) row[index] = patch.task_second_notified_at ?? "";
        break;
      case "taskthirdnotifiedat":
        if (hasOwn(patch, "taskThirdNotifiedAt")) row[index] = patch.taskThirdNotifiedAt ?? "";
        if (hasOwn(patch, "task_third_notified_at")) row[index] = patch.task_third_notified_at ?? "";
        break;
      case "taskcompletedat":
        if (hasOwn(patch, "taskCompletedAt")) row[index] = patch.taskCompletedAt ?? "";
        if (hasOwn(patch, "task_completed_at")) row[index] = patch.task_completed_at ?? "";
        break;
      case "taskcompletednotifiedat":
        if (hasOwn(patch, "taskCompletedNotifiedAt")) row[index] = patch.taskCompletedNotifiedAt ?? "";
        if (hasOwn(patch, "task_completed_notified_at")) {
          row[index] = patch.task_completed_notified_at ?? "";
        }
        break;
      case "rate":
        if (hasOwn(patch, "rate")) row[index] = patch.rate ?? "";
        break;
      case "createdat":
        if (hasOwn(patch, "createdAt")) row[index] = patch.createdAt ?? "";
        if (hasOwn(patch, "created_at")) row[index] = patch.created_at ?? "";
        break;
      case "createdby":
        if (hasOwn(patch, "createdBy")) row[index] = patch.createdBy ?? "";
        if (hasOwn(patch, "created_by")) row[index] = patch.created_by ?? "";
        break;
      case "updatedat":
        if (hasOwn(patch, "updatedAt")) row[index] = patch.updatedAt ?? "";
        if (hasOwn(patch, "updated_at")) row[index] = patch.updated_at ?? "";
        break;
      case "updatedby":
        if (hasOwn(patch, "updatedBy")) row[index] = patch.updatedBy ?? "";
        if (hasOwn(patch, "updated_by")) row[index] = patch.updated_by ?? "";
        break;
      case "startdate":
        if (hasOwn(patch, "startDate")) row[index] = patch.startDate ?? "";
        if (hasOwn(patch, "start_date")) row[index] = patch.start_date ?? "";
        break;
      case "startyear":
        if (hasOwn(patch, "startYear")) row[index] = patch.startYear ?? "";
        if (hasOwn(patch, "start_year")) row[index] = patch.start_year ?? "";
        break;
      case "status":
        if (hasOwn(patch, "status")) row[index] = patch.status ?? "";
        if (hasOwn(patch, "pending_status")) row[index] = patch.pending_status ?? "";
        break;
      case "submittedtoam":
        if (hasOwn(patch, "submitted_to_am")) row[index] = patch.submitted_to_am ?? "";
        if (hasOwn(patch, "submittedToAm")) row[index] = patch.submittedToAm ?? "";
        break;
      case "submittedtoclient":
        if (hasOwn(patch, "submitted_to_client")) row[index] = patch.submitted_to_client ?? "";
        if (hasOwn(patch, "submittedToClient")) row[index] = patch.submittedToClient ?? "";
        break;
      case "interviewrequested":
        if (hasOwn(patch, "interview_requested")) row[index] = patch.interview_requested ?? "";
        if (hasOwn(patch, "interviewRequested")) row[index] = patch.interviewRequested ?? "";
        break;
      case "interviewrequesteddate":
        if (hasOwn(patch, "interview_requested_date")) {
          row[index] = patch.interview_requested_date ?? "";
        }
        if (hasOwn(patch, "interviewRequestedDate")) {
          row[index] = patch.interviewRequestedDate ?? "";
        }
        break;
      case "interview1":
        if (hasOwn(patch, "interview_1")) row[index] = patch.interview_1 ?? "";
        if (hasOwn(patch, "interview1")) row[index] = patch.interview1 ?? "";
        break;
      case "interview2":
        if (hasOwn(patch, "interview_2")) row[index] = patch.interview_2 ?? "";
        if (hasOwn(patch, "interview2")) row[index] = patch.interview2 ?? "";
        break;
      case "interview3":
        if (hasOwn(patch, "interview_3")) row[index] = patch.interview_3 ?? "";
        if (hasOwn(patch, "interview3")) row[index] = patch.interview3 ?? "";
        break;
      case "offerdate":
        if (hasOwn(patch, "offer_date")) row[index] = patch.offer_date ?? "";
        if (hasOwn(patch, "offerDate")) row[index] = patch.offerDate ?? "";
        break;
      case "colextendeddate":
        if (hasOwn(patch, "col_extended_date")) row[index] = patch.col_extended_date ?? "";
        if (hasOwn(patch, "colExtendedDate")) row[index] = patch.colExtendedDate ?? "";
        break;
      case "colsigneddate":
        if (hasOwn(patch, "col_signed_date")) row[index] = patch.col_signed_date ?? "";
        if (hasOwn(patch, "colSignedDate")) row[index] = patch.colSignedDate ?? "";
        break;
      case "verbalofferdate":
        if (hasOwn(patch, "verbal_offer_date")) row[index] = patch.verbal_offer_date ?? "";
        if (hasOwn(patch, "verbalOfferDate")) row[index] = patch.verbalOfferDate ?? "";
        break;
      case "offerextendeddate":
        if (hasOwn(patch, "offer_extended_date")) row[index] = patch.offer_extended_date ?? "";
        if (hasOwn(patch, "offerExtendedDate")) row[index] = patch.offerExtendedDate ?? "";
        break;
      case "offeraccepteddate":
        if (hasOwn(patch, "offer_accepted_date")) row[index] = patch.offer_accepted_date ?? "";
        if (hasOwn(patch, "offerAcceptedDate")) row[index] = patch.offerAcceptedDate ?? "";
        break;
      case "pendingstartdate":
        if (hasOwn(patch, "pendingStartDate")) row[index] = patch.pendingStartDate ?? "";
        if (hasOwn(patch, "pending_start_date")) row[index] = patch.pending_start_date ?? "";
        break;
      case "clearancecrossoverdate":
        if (hasOwn(patch, "clearanceCrossoverDate")) {
          row[index] = patch.clearanceCrossoverDate ?? "";
        }
        if (hasOwn(patch, "clearance_crossover_date")) {
          row[index] = patch.clearance_crossover_date ?? "";
        }
        break;
      case "startedworkdate":
        if (hasOwn(patch, "startedWorkDate")) row[index] = patch.startedWorkDate ?? "";
        if (hasOwn(patch, "started_work_date")) row[index] = patch.started_work_date ?? "";
        break;
      default:
        break;
    }
  });
  return row;
}

function toLogRow(event, payload) {
  const timestamp = event.timestamp || new Date().toISOString();
  return [
    timestamp,
    event.rowId || payload.id || "",
    event.client || payload.client || "",
    event.jobTitle || payload.jobTitle || payload.job_title || "",
    event.candidate || payload.candidate || payload.candidate_name || "",
    event.recruiter || payload.recruiter || "",
    event.businessLine || payload.businessLine || payload.business_line || "",
    event.eventType || "",
    event.field || "",
    event.previousValue ?? "",
    event.newValue ?? "",
    event.eventDate || "",
    event.source || "",
    event.updatedBy || "",
  ];
}

export default async function handler(req, res) {
  // ---- CORS HEADERS ----
  res.setHeader("Access-Control-Allow-Origin", ALLOWED_ORIGIN);
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  // ---- HANDLE PREFLIGHT ----
  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }

  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "POST only" });
  }

  try {
    console.log("write-row: start");

    const {
      GOOGLE_CLIENT_EMAIL,
      GOOGLE_PRIVATE_KEY,
      GOOGLE_SHEET_ID
    } = process.env;

    console.log("env vars present", {
      email: !!GOOGLE_CLIENT_EMAIL,
      key: !!GOOGLE_PRIVATE_KEY,
      sheet: !!GOOGLE_SHEET_ID
    });

    const auth = new google.auth.JWT({
      email: GOOGLE_CLIENT_EMAIL,
      key: GOOGLE_PRIVATE_KEY.replace(/\\n/g, "\n"),
      scopes: ["https://www.googleapis.com/auth/spreadsheets"]
    });

    await auth.authorize();
    console.log("auth success");

    const sheets = google.sheets({ version: "v4", auth });

    const payload = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const rowData = payload.row && typeof payload.row === "object" ? payload.row : null;
    const data = rowData ? { ...payload, ...rowData } : payload;
    let id = data.id || data.client;
    const sheetTitle = payload.tab || DEFAULT_TAB;
    const mode = payload.mode || "";
    if (mode === "patch" && !id) {
      return res.status(400).json({ ok: false, error: "Missing id" });
    }
    if (!id) {
      id = generateId();
    }
    const logEvents = Array.isArray(payload.logEvents) ? payload.logEvents : [];
    if (logEvents.length === 0) {
      console.warn("write-row: missing logEvents", {
        sheetTitle,
        id,
        mode,
      });
    }
    payload.id = id;
    data.id = id;

    const sheetMeta = await sheets.spreadsheets.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      ranges: [sheetTitle],
      includeGridData: false
    });
    const sheet = sheetMeta.data.sheets?.find(
      (item) => item.properties?.title === sheetTitle
    );

    if (!sheet?.properties?.sheetId && sheet?.properties?.sheetId !== 0) {
      throw new Error(`Sheet not found: ${sheetTitle}`);
    }

    const headerResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: `${sheetTitle}!1:1`
    });
    const headers = headerResponse.data.values?.[0] || [];
    if (!headers.length) {
      throw new Error(`Missing header row in ${sheetTitle}`);
    }
    const lastColumnLetter = columnLetter(headers.length - 1);
    const headerIndex = buildHeaderIndex(headers);

    let idColumnIndex = headers.findIndex(
      (header) => normalizeHeader(header) === "id"
    );
    if (idColumnIndex === -1 && sheetTitle === "CRF_Summary") {
      idColumnIndex = headers.findIndex((header) => {
        const normalized = normalizeHeader(header);
        return normalized === "client" || normalized === "clientname";
      });
    }
    if (idColumnIndex === -1) {
      throw new Error(`Missing id column in ${sheetTitle}`);
    }

    const keyColumnIndex = headerIndex.key ?? -1;
    const idColumnLetter = columnLetter(idColumnIndex);
    const idValuesResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: `${sheetTitle}!${idColumnLetter}:${idColumnLetter}`
    });
    const idValues = idValuesResponse.data.values || [];
    const normalizedId = normalizeCell(id);
    let rowIndex = findRowIndexByValue(idValues, normalizedId);
    if (rowIndex < 0 && keyColumnIndex !== -1) {
      const keyColumnLetter = columnLetter(keyColumnIndex);
      const keyValuesResponse = await sheets.spreadsheets.values.get({
        spreadsheetId: GOOGLE_SHEET_ID,
        range: `${sheetTitle}!${keyColumnLetter}:${keyColumnLetter}`
      });
      const keyValues = keyValuesResponse.data.values || [];
      rowIndex = findRowIndexByValue(keyValues, normalizedId);
    }
    if (rowIndex < 0 && mode === "patch") {
      const lookupPairs = buildCompositeLookup(data, headerIndex);
      if (lookupPairs.length >= 2) {
        const rowsResponse = await sheets.spreadsheets.values.get({
          spreadsheetId: GOOGLE_SHEET_ID,
          range: `${sheetTitle}!A2:${lastColumnLetter}`
        });
        const rows = rowsResponse.data.values || [];
        const compositeIndex = rows.findIndex((row) =>
          lookupPairs.every(
            ({ index, value }) => normalizeKey(row && row[index]) === value
          )
        );
        if (compositeIndex >= 0) {
          rowIndex = compositeIndex + 1;
        }
      }
    }
    const rowNumber = rowIndex >= 0 ? rowIndex + 1 : 2;

    if (logEvents.length > 0) {
      const logRows = logEvents.map((event) => toLogRow(event, data));
      await sheets.spreadsheets.values.append({
        spreadsheetId: GOOGLE_SHEET_ID,
        range: `${ACTIVITY_LOG_TAB}!A:Z`,
        valueInputOption: "RAW",
        requestBody: { values: logRows },
      });
    }

    if (payload.action === "delete") {
      if (sheetTitle === ACTIVITY_LOG_TAB) {
        return res.status(400).json({ ok: false, error: "Refusing to delete ActivityLog rows." });
      }
      if (rowIndex < 0) {
        return res.status(200).json({ ok: true, skipped: true });
      }
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: GOOGLE_SHEET_ID,
        requestBody: {
          requests: [
            {
              deleteDimension: {
                range: {
                  sheetId: sheet.properties.sheetId,
                  dimension: "ROWS",
                  startIndex: rowIndex,
                  endIndex: rowIndex + 1
                }
              }
            }
          ]
        }
      });
      return res.status(200).json({ ok: true, deleted: true });
    }

    if (mode === "patch" && rowIndex < 0) {
      return res.status(404).json({ ok: false, error: "Row not found for patch update" });
    }

    if (rowIndex === -1) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: GOOGLE_SHEET_ID,
        requestBody: {
          requests: [
            {
              insertDimension: {
                range: {
                  sheetId: sheet.properties.sheetId,
                  dimension: "ROWS",
                  startIndex: 1,
                  endIndex: 2
                },
                inheritFromBefore: false
              }
            }
          ]
        }
      });
    }

    if (mode === "patch") {
      const existingResponse = await sheets.spreadsheets.values.get({
        spreadsheetId: GOOGLE_SHEET_ID,
        range: `${sheetTitle}!A${rowNumber}:${lastColumnLetter}${rowNumber}`,
      });
      const existingRow = (existingResponse.data.values || [])[0] || [];
      const patch = payload.patch || {};
      const patchWithId = { ...patch };
      const existingId = normalizeCell(existingRow[idColumnIndex]);
      if (!existingId && normalizedId && !hasOwn(patchWithId, "id")) {
        patchWithId.id = normalizedId;
      }
      const mergedRow = applyPatchToRow(headers, existingRow, patchWithId);
      await sheets.spreadsheets.values.update({
        spreadsheetId: GOOGLE_SHEET_ID,
        range: `${sheetTitle}!A${rowNumber}:${lastColumnLetter}${rowNumber}`,
        valueInputOption: "USER_ENTERED",
        requestBody: {
          values: [mergedRow],
        },
      });
    } else {
      const values = mapPayloadToRow(headers, data);
      await sheets.spreadsheets.values.update({
        spreadsheetId: GOOGLE_SHEET_ID,
        range: `${sheetTitle}!A${rowNumber}:${lastColumnLetter}${rowNumber}`,
        valueInputOption: "USER_ENTERED",
        requestBody: {
          values: [values]
        }
      });
    }

    console.log("sheet write success");

    res.status(200).json({ ok: true, id });
  } catch (err) {
    console.error("write-row error", err);
    res.status(500).json({
      ok: false,
      error: err.message
    });
  }
}

// deploy-marker-backend: 2026-01-17
