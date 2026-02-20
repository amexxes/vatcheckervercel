export default async function handler(req, res) {
  const { id } = req.query;
  if (!id) return res.status(400).json({ error: "Missing id" });

  // TODO: lees job status + resultaten en return JSON
  return res.status(200).json({ ok: true, id });
}
