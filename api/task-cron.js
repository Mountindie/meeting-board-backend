import { google } from "googleapis";
import fs from "fs/promises";
import path from "path";

const TASK_INTERVAL_MS = 30 * 60 * 1000;
const RESEND_API_URL = "https://api.resend.com/emails";
const SECOND_REMINDER_HOUR = 11;
const SECOND_REMINDER_MINUTE = 30;
const THIRD_REMINDER_HOUR = 15;
const THIRD_REMINDER_MINUTE = 0;

function normalizeHeader(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function normalizeRecruiterKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function parseIsoDate(value) {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function toIsoString(date = new Date()) {
  return date.toISOString();
}

async function loadRecruiterEmails() {
  const filePath = path.join(process.cwd(), "data", "recruiters.json");
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function buildAuth() {
  const SHEET_ID = process.env.GOOGLE_SHEET_ID;
  const CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL;
  const PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY;

  if (!SHEET_ID || !CLIENT_EMAIL || !PRIVATE_KEY) {
    throw new Error("Missing Google Sheets credentials.");
  }

  return new google.auth.JWT(
    CLIENT_EMAIL,
    null,
    PRIVATE_KEY.replace(/\\n/g, "\n"),
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
}

function shouldNotify(createdAt, lastNotifiedAt, now) {
  if (!createdAt) return false;
  const created = parseIsoDate(createdAt);
  if (!created) return false;
  const last = lastNotifiedAt ? parseIsoDate(lastNotifiedAt) : null;
  const reference = last || created;
  if (!reference) return false;
  return now - reference >= TASK_INTERVAL_MS;
}

function buildRowFromHeaders(headers, values) {
  const row = {};
  headers.forEach((header, index) => {
    const key = normalizeHeader(header);
    if (!key) return;
    row[key] = values[index] ?? "";
  });
  return row;
}

function getValue(row, key) {
  return row[key] || row[normalizeHeader(key)] || "";
}

function updateRowValues({ headers, values, updates }) {
  const updated = new Array(headers.length).fill("");
  values.forEach((value, index) => {
    updated[index] = value;
  });
  const headerKeys = headers.map((header) => normalizeHeader(header));
  Object.entries(updates).forEach(([key, value]) => {
    const normalized = normalizeHeader(key);
    const index = headerKeys.indexOf(normalized);
    if (index === -1) return;
    updated[index] = value;
  });
  return updated;
}

async function sendEmail({ apiKey, to, from, subject, text }) {
  const response = await fetch(RESEND_API_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from,
      to,
      subject,
      text,
    }),
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || "Resend request failed.");
  }
  return response.json();
}

function getTimeZoneOffsetMinutes(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    timeZoneName: "shortOffset",
  });
  const parts = formatter.formatToParts(date);
  const tz = parts.find((part) => part.type === "timeZoneName")?.value || "GMT";
  const match = tz.match(/GMT([+-]\\d{1,2})(?::(\\d{2}))?/);
  if (!match) return 0;
  const hours = Number(match[1]);
  const minutes = match[2] ? Number(match[2]) : 0;
  return hours * 60 + Math.sign(hours) * minutes;
}

function makeDateInTimeZone({ year, month, day, hour, minute, timeZone }) {
  const utcMillis = Date.UTC(year, month - 1, day, hour, minute, 0, 0);
  const offset = getTimeZoneOffsetMinutes(new Date(utcMillis), timeZone);
  return new Date(utcMillis - offset * 60 * 1000);
}

function getLocalParts(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = formatter.formatToParts(date);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return {
    year: Number(values.year),
    month: Number(values.month),
    day: Number(values.day),
    hour: Number(values.hour),
    minute: Number(values.minute),
  };
}

function addDays(date, days) {
  const copy = new Date(date);
  copy.setDate(copy.getDate() + days);
  return copy;
}

function scheduleNextReminder(createdAt, timeZone) {
  const createdParts = getLocalParts(createdAt, timeZone);
  const secondCandidate = makeDateInTimeZone({
    year: createdParts.year,
    month: createdParts.month,
    day: createdParts.day,
    hour: SECOND_REMINDER_HOUR,
    minute: SECOND_REMINDER_MINUTE,
    timeZone,
  });
  const secondAt = createdAt <= secondCandidate ? secondCandidate : addDays(secondCandidate, 1);
  const secondLocal = getLocalParts(secondAt, timeZone);
  const thirdAt = makeDateInTimeZone({
    year: secondLocal.year,
    month: secondLocal.month,
    day: secondLocal.day,
    hour: THIRD_REMINDER_HOUR,
    minute: THIRD_REMINDER_MINUTE,
    timeZone,
  });
  return { secondAt, thirdAt };
}

export default async function handler(req, res) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ ok: false, error: "GET only" });
  }

  if (process.env.CRON_SECRET && req.query?.secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  try {
    const resendKey = process.env.RESEND_API_KEY;
    if (!resendKey) {
      throw new Error("Missing RESEND_API_KEY");
    }

    const leadershipEmail = process.env.LEADERSHIP_EMAIL || "operations@mountindie.com";
    const sender = process.env.TASK_FROM_EMAIL || "missioncontrol@mountindie.com";
    const timeZone = process.env.TASK_TIMEZONE || "America/Los_Angeles";

    const recruiterEmails = await loadRecruiterEmails();
    const auth = buildAuth();
    const sheets = google.sheets({ version: "v4", auth });
    const sheetId = process.env.GOOGLE_SHEET_ID;
    const tab = "ActiveBoard";

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: `${tab}!A:Z`,
    });

    const rows = response.data.values || [];
    if (!rows.length) {
      return res.status(200).json({ ok: true, sent: 0 });
    }

    const [headers, ...values] = rows;
    const idIndex = headers.findIndex((header) => normalizeHeader(header) === "id");
    if (idIndex === -1) {
      throw new Error("Missing id column in ActiveBoard");
    }

    const now = new Date();
    let sent = 0;
    let updated = 0;
    const reminderQueue = new Map();
    const completedQueue = [];

    for (let i = 0; i < values.length; i += 1) {
      const rowValues = values[i];
      const rowNumber = i + 2;
      const row = buildRowFromHeaders(headers, rowValues);
      const taskStatus = String(getValue(row, "taskStatus") || "").toLowerCase();
      const taskText = getValue(row, "taskText");
      const recruiter = getValue(row, "recruiter");

      if (taskStatus === "open" && taskText) {
        const createdAt = getValue(row, "taskCreatedAt");
        const createdDate = parseIsoDate(createdAt);
        if (createdDate) {
          const firstNotifiedAt = parseIsoDate(getValue(row, "taskFirstNotifiedAt"));
          const secondNotifiedAt = parseIsoDate(getValue(row, "taskSecondNotifiedAt"));
          const thirdNotifiedAt = parseIsoDate(getValue(row, "taskThirdNotifiedAt"));
          const { secondAt, thirdAt } = scheduleNextReminder(createdDate, timeZone);

          let stage = "";
          if (!thirdNotifiedAt && now >= thirdAt) {
            stage = "third";
          } else if (!secondNotifiedAt && now >= secondAt) {
            stage = "second";
          } else if (!firstNotifiedAt && now - createdDate >= TASK_INTERVAL_MS) {
            stage = "first";
          }

          if (stage) {
            const recruiterKey = normalizeRecruiterKey(recruiter);
            if (!recruiterKey) {
              continue;
            }
            if (!reminderQueue.has(recruiterKey)) {
              reminderQueue.set(recruiterKey, {
                recruiter: recruiter || "Unassigned",
                tasks: [],
              });
            }
            reminderQueue.get(recruiterKey).tasks.push({
              rowNumber,
              rowValues,
              taskText,
              stage,
              client: getValue(row, "client") || "Unknown",
              candidate: getValue(row, "candidate") || "Unknown",
              job: getValue(row, "jobTitle") || getValue(row, "jobtitle") || "Unknown",
            });
          }
        }
      }

      if (taskStatus === "done" && taskText) {
        const completedAt = getValue(row, "taskCompletedAt");
        const completionNotified = getValue(row, "taskCompletedNotifiedAt");
        if (completedAt && !completionNotified) {
          completedQueue.push({
            rowNumber,
            rowValues,
            taskText,
            recruiter: recruiter || "Unassigned",
            client: getValue(row, "client") || "Unknown",
            candidate: getValue(row, "candidate") || "Unknown",
            job: getValue(row, "jobTitle") || getValue(row, "jobtitle") || "Unknown",
          });
        }
      }
    }

    for (const [recruiterKey, group] of reminderQueue.entries()) {
      const email = recruiterEmails[recruiterKey];
      if (!email) continue;
      const subject = `Task reminders (${group.tasks.length})`;
      const lines = group.tasks.map((task) => {
        const label =
          task.stage === "first" ? "30-min" : task.stage === "second" ? "11:30" : "3:00 PM";
        return `• [${label}] ${task.taskText} — ${task.client} · ${task.candidate} · ${task.job}`;
      });
      const text = [
        `Hi ${group.recruiter},`,
        "",
        "Pending tasks:",
        ...lines,
        "",
        "Please update the task status when complete.",
      ].join("\n");
      await sendEmail({
        apiKey: resendKey,
        to: email,
        from: sender,
        subject,
        text,
      });
      sent += 1;

      for (const task of group.tasks) {
        const updates = {};
        if (task.stage === "first") updates.taskFirstNotifiedAt = toIsoString(now);
        if (task.stage === "second") updates.taskSecondNotifiedAt = toIsoString(now);
        if (task.stage === "third") updates.taskThirdNotifiedAt = toIsoString(now);
        updates.taskLastNotifiedAt = toIsoString(now);
        const rowUpdate = updateRowValues({
          headers,
          values: task.rowValues,
          updates,
        });
        await sheets.spreadsheets.values.update({
          spreadsheetId: sheetId,
          range: `${tab}!A${task.rowNumber}:${columnLetter(headers.length - 1)}${task.rowNumber}`,
          valueInputOption: "USER_ENTERED",
          requestBody: { values: [rowUpdate] },
        });
        updated += 1;
      }
    }

    if (completedQueue.length) {
      const subject = `Tasks completed (${completedQueue.length})`;
      const lines = completedQueue.map((task) => {
        return `• ${task.taskText} — ${task.recruiter} · ${task.client} · ${task.candidate} · ${task.job}`;
      });
      const text = ["Completed tasks:", ...lines].join("\n");
      await sendEmail({
        apiKey: resendKey,
        to: leadershipEmail,
        from: sender,
        subject,
        text,
      });
      sent += 1;

      for (const task of completedQueue) {
        const updates = updateRowValues({
          headers,
          values: task.rowValues,
          updates: { taskCompletedNotifiedAt: toIsoString(now) },
        });
        await sheets.spreadsheets.values.update({
          spreadsheetId: sheetId,
          range: `${tab}!A${task.rowNumber}:${columnLetter(headers.length - 1)}${task.rowNumber}`,
          valueInputOption: "USER_ENTERED",
          requestBody: { values: [updates] },
        });
        updated += 1;
      }
    }

    return res.status(200).json({ ok: true, sent, updated });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
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
