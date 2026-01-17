const { google } = require("googleapis");

/**
 * REQUIRED ENV VARS (Vercel → Project → Settings → Environment Variables)
 *
 * GOOGLE_SHEET_ID
 * GOOGLE_CLIENT_EMAIL
 * GOOGLE_PRIVATE_KEY
 */

function buildAuth() {
  const SHEET_ID = process.env.GOOGLE_SHEET_ID;
  const CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL;
  const PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY;

  console.log("ENV CHECK", {
    hasSheetId: Boolean(SHEET_ID),
    hasClientEmail: Boolean(CLIENT_EMAIL),
    hasPrivateKey: Boolean(PRIVATE_KEY),
  });

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

function getTab(req) {
  return req.query?.tab || "ActiveBoard";
}

module.exports = async (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Type", "application/json");

  try {
    const auth = buildAuth();
    await auth.authorize();
    const sheets = google.sheets({ version: "v4", auth });

    const SHEET_ID = process.env.GOOGLE_SHEET_ID;
    const tab = getTab(req);

    // -----------------------
    // GET → Read sheet
    // -----------------------
    if (req.method === "GET") {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: `${tab}!A:Z`,
      });

      return res.status(200).json({
        tab,
        rows: response.data.values || [],
      });
    }

    // -----------------------
    // POST → Append rows
    // -----------------------
    if (req.method === "POST") {
      const body =
        typeof req.body === "string" ? JSON.parse(req.body) : req.body;

      const rows = Array.isArray(body) ? body : body?.rows;

      if (!Array.isArray(rows) || rows.length === 0) {
        return res.status(400).json({
          error: "POST body must be an array of rows",
        });
      }

      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${tab}!A:Z`,
        valueInputOption: "RAW",
        requestBody: { values: rows },
      });

      return res.status(200).json({
        ok: true,
        tab,
        appended: rows.length,
      });
    }

    // -----------------------
    // Method not allowed
    // -----------------------
    res.setHeader("Allow", "GET, POST");
    return res.status(405).json({ error: "Method not allowed" });
  } catch (err) {
    console.error("Sheets API ERROR:", err);
    return res.status(500).json({
      error: err.message,
      stack:
        process.env.NODE_ENV === "production" ? undefined : err.stack,
    });
  }
};
