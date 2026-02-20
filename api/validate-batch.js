export default async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const vat_numbers = Array.isArray(req.body?.vat_numbers) ? req.body.vat_numbers : [];
  const case_ref = (req.body?.case_ref || "").toString().slice(0, 80);

  // tijdelijke response zodat je frontend niet meer 404 krijgt
  return res.status(200).json({
    duplicates_ignored: 0,
    vies_status: [],
    results: vat_numbers.map((input) => ({
      input,
      case_ref,
      state: "queued",
      vat_number: String(input || ""),
      country_code: String(input || "").slice(0, 2).toUpperCase(),
      valid: null,
      name: "",
      address: "",
      error_code: "",
      error: "",
      details: "stub (Vercel function ok)",
    })),
    fr_job_id: null,
  });
}
