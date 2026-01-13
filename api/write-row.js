import { google } from "googleapis";

export default async function handler(req, res) {
  // ---- CORS ----
  res.setHeader(
    "Access-Control-Allow-Origin",
    "https://meeting-board-sl.vercel.app"
  );
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

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

    const auth = new google.auth.JWT({
      email: GOOGLE_CLIENT_EMAIL,
      key: GOOGLE_PRIVATE_KEY.replace(/\\n/g, "\n"),
      scopes: ["https://www.googleapis.com/auth/spreadsheets"]
    });

    await auth.authorize();

    const sheets = google.sheets({ version: "v4", auth });

    const {
      tab,
      id,
      client,
      recruiter,
      candidate,
      stage,
      stageDate,
      businessLine,
      risk,
      notes,
      createdAt,
      updatedAt,
      startDate,
      status,
      jobTitle,
      start_date
    } = req.body;

    const startDateValue = startDate || start_date || "";
    const targetSheet = startDateValue ? "PendingHires" : "ActiveBoard";

    await sheets.spreadsheets.values.append({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: `${targetSheet}!A1`,
      valueInputOption: "USER_ENTERED",
      requestBody: {
        values:
          targetSheet === "PendingHires"
            ? [
                [
                  id,
                  candidate,
                  client,
                  jobTitle,
                  recruiter,
                  businessLine,
                  startDateValue,
                  status,
                  notes,
                  createdAt,
                  updatedAt
                ]
              ]
            : [
                [
                  id,
                  client,
                  jobTitle,
                  businessLine,
                  recruiter,
                  candidate,
                  stage,
                  stageDate,
                  risk,
                  notes,
                  createdAt,
                  updatedAt
                ]
              ]
      }
    });

    res.status(200).json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: err.message });
  }
}
