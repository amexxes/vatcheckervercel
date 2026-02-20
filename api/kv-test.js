import { kv } from "@vercel/kv";

export default async function handler(req, res) {
  await kv.set("health:last", Date.now());
  const v = await kv.get("health:last");
  res.status(200).json({ ok: true, value: v });
}
