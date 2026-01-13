import { google } from "googleapis";

export default async function handler(req, res) {
  // ---- CORS ----
  res.setHeader(
    "Access-Control-Allow-Origin",
    "https://meeting-board-sl.vercel.app"
  );
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }

  if (req.method !== "GET") {
    return res.status(405).json({ ok: false, error: "GET only" });
  }

  try {
    console.log("get-rows: start");

    const {
      GOOGLE_CLIENT_EMAIL,
      GOOGLE_PRIVATE_KEY,
      GOOGLE_SHEET_ID
    } = process.env;

    const auth = new google.auth.JWT({
      email: GOOGLE_CLIENT_EMAIL,
      key: GOOGLE_PRIVATE_KEY.replace(/\\n/g, "\n"),
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    });

    await auth.authorize();

    const sheets = google.sheets({ version: "v4", auth });

    const startDateValue =
      (req.query && (req.query.startDate || req.query.start_date)) || "";
    const targetSheet = startDateValue ? "PendingHires" : "ActiveBoard";
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: targetSheet
    });

    const values = response.data.values || [];
    const [headers, ...rows] = values;

    if (!headers) {
      return res.status(200).json({ ok: true, rows: [] });
    }

    const normalizedRows =
      targetSheet === "PendingHires"
        ? rows.map((row) => ({
            id: row[0] || "",
            candidate: row[1] || "",
            client: row[2] || "",
            jobTitle: row[3] || "",
            recruiter: row[4] || "",
            businessLine: row[5] || "",
            startDate: row[6] || "",
            status: row[7] || "",
            notes: row[8] || "",
            createdAt: row[9] || "",
            updatedAt: row[10] || ""
          }))
        : rows.map((row) => ({
            id: row[0] || "",
            client: row[1] || "",
            jobTitle: row[2] || "",
            businessLine: row[3] || "",
            recruiter: row[4] || "",
            candidate: row[5] || "",
            stage: row[6] || "",
            stageDate: row[7] || "",
            risk: row[8] || "",
            notes: row[9] || "",
            createdAt: row[10] || "",
            updatedAt: row[11] || ""
          }));

    return res.status(200).json({ ok: true, rows: normalizedRows });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}
