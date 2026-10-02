// Same-origin CORS proxy for Yahoo Finance's public read-only endpoints.
//
// Yahoo sends no Access-Control-Allow-Origin header, so the browser cannot
// call it directly. This function is the first entry in the PROXIES chain in
// index.html; when it is missing (GitHub Pages, file://) the page falls back
// to the public readers, which work but are slower and flakier.
//
// Allow-list only. An open proxy on a public domain gets found and abused.

const ALLOWED_HOST = /^query[12]\.finance\.yahoo\.com$/;
const ALLOWED_PATH = /^\/v\d+\/finance\/(spark|chart|quote|quoteSummary)(\/|$)/;

// Keep this exactly as-is. Yahoo 429s a full Chrome UA string while answering
// the bare token instantly — same URL, same IP, same second (verified while
// building refresh_quotes.py, see the note there).
const UA = 'Mozilla/5.0';

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'public, max-age=0, must-revalidate');

  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    return res.status(204).end();
  }

  const raw = Array.isArray(req.query.url) ? req.query.url[0] : req.query.url;
  if (!raw) return res.status(400).json({ error: 'Missing url' });

  let target;
  try {
    target = new URL(raw);
  } catch {
    return res.status(403).json({ error: 'Only Yahoo Finance query endpoints are allowed' });
  }

  if (
    target.protocol !== 'https:' ||
    !ALLOWED_HOST.test(target.hostname) ||
    !ALLOWED_PATH.test(target.pathname)
  ) {
    return res.status(403).json({ error: 'Only Yahoo Finance query endpoints are allowed' });
  }

  try {
    const upstream = await fetch(target.toString(), {
      headers: { 'user-agent': UA, accept: 'application/json,text/plain,*/*' },
      signal: AbortSignal.timeout(12000),
    });
    const body = await upstream.text();
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    return res.status(upstream.ok ? 200 : upstream.status).send(body);
  } catch (err) {
    return res.status(502).json({ error: 'Upstream fetch failed' });
  }
};
