const { google } = require("googleapis");

module.exports = async (req, res) => {
  console.log("ping-sheet: start");

  const PROJECT_ID = process.env.GOOGLE_PROJECT_ID;
  const CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL;
  const PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY;
  const SHEET_ID = process.env.GOOGLE_SHEET_ID;

  console.log("env vars present", {
    hasProjectId: Boolean(PROJECT_ID),
    hasClientEmail: Boolean(CLIENT_EMAIL),
    hasPrivateKey: Boolean(PRIVATE_KEY),
    hasSheetId: Boolean(SHEET_ID),
  });

  try {
    if (!PROJECT_ID || !CLIENT_EMAIL || !PRIVATE_KEY || !SHEET_ID) {
      throw new Error("Missing Google Sheets credentials.");
    }

    const auth = new google.auth.JWT({
      email: CLIENT_EMAIL,
      key: PRIVATE_KEY.replace(/\\n/g, "\n"),
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });

    console.log("auth success");

    const sheets = google.sheets({ version: "v4", auth });
    const value = `PING ${new Date().toISOString()}`;

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: "PingTest!A1",
      valueInputOption: "RAW",
      requestBody: { values: [[value]] },
    });

    console.log("sheet write success");

    return res.status(200).json({ ok: true });
  } catch (err) {
    console.error("ping-sheet: error", err);
    if (err && err.stack) {
      console.error(err.stack);
    }
    return res.status(500).json({
      ok: false,
      error: err && err.message ? err.message : String(err),
    });
  }
};
