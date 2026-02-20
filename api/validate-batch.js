export default async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const { vat_numbers = [], case_ref = "" } = req.body || {};
  // TODO: plak hier je logic uit server.js (validate-batch)
  return res.status(200).json({ ok: true });
}
