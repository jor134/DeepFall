// DEEPFALL — room state backend
//
// Commit this as  api/room.js  in the repo root alongside index.html.
// Vercel picks up /api automatically; no package.json and no dependencies are
// needed, since this talks to the store over its REST interface using fetch.
//
// Bind a store in the Vercel dashboard (Storage -> Upstash Redis, or the older
// Vercel KV). Either one injects the env vars below automatically on the next
// deploy. Nothing here is secret to the client: the token stays server-side,
// which is the entire reason this route exists instead of calling the store
// straight from the browser.

// Credentials are discovered rather than hardcoded. Vercel lets you attach a
// marketplace resource with an arbitrary env-var prefix
// (`vercel integration resource connect <db> --prefix FOO_`), and Upstash and
// the legacy Vercel KV integration use different names again. Scanning for the
// suffix pair means this works whatever the store ended up called.
const URL_SUFFIXES = ['REST_API_URL', 'REST_URL'];
const TOK_SUFFIXES = ['REST_API_TOKEN', 'REST_TOKEN'];

function stripSuffix(key, suffixes) {
  for (const suf of suffixes) {
    if (key.endsWith(suf)) return key.slice(0, key.length - suf.length);
  }
  return null;
}

function discoverStore() {
  const env = process.env;
  const urls = [], toks = [];
  for (const k of Object.keys(env)) {
    const v = env[k];
    if (!v) continue;
    const K = k.toUpperCase();
    // must be an HTTPS REST endpoint; a redis:// TCP string is no use to fetch
    if (stripSuffix(K, URL_SUFFIXES) !== null && /^https:\/\//.test(v)) {
      urls.push({ key: k, prefix: stripSuffix(K, URL_SUFFIXES), value: v });
    }
    if (stripSuffix(K, TOK_SUFFIXES) !== null) {
      toks.push({ key: k, prefix: stripSuffix(K, TOK_SUFFIXES), value: v });
    }
  }
  // prefer a URL and token that share a prefix — that is one resource
  for (const u of urls) {
    const t = toks.find(x => x.prefix === u.prefix);
    if (t) return { url: u.value, token: t.value, urlVar: u.key, tokenVar: t.key };
  }
  // otherwise, if there is exactly one of each, they must belong together
  if (urls.length === 1 && toks.length === 1) {
    return {
      url: urls[0].value, token: toks[0].value,
      urlVar: urls[0].key, tokenVar: toks[0].key
    };
  }
  return {
    url: null, token: null, urlVar: null, tokenVar: null,
    seenUrls: urls.map(u => u.key), seenToks: toks.map(t => t.key)
  };
}

const STORE = discoverStore();
const STORE_URL = STORE.url;
const STORE_TOKEN = STORE.token;

const TTL = 1800;              // rooms evaporate 30 min after the last write
const SIG_TTL = 300;           // handshakes are short-lived by nature
const MAX_BYTES = 24000;       // reject oversized payloads outright

async function redis(cmd) {
  const r = await fetch(STORE_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${STORE_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(cmd)
  });
  if (!r.ok) throw new Error('store ' + r.status);
  const j = await r.json();
  return j.result;
}

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store');

  // GET is the capability probe the client runs at boot.
  if (req.method === 'GET') {
    // Names only. Never echo a credential value, even to help debugging.
    return res.status(200).json({
      ok: !!(STORE_URL && STORE_TOKEN),
      backend: 'redis-rest',
      urlVar: STORE.urlVar || null,
      tokenVar: STORE.tokenVar || null,
      candidates: STORE.url ? undefined
        : { urlLike: STORE.seenUrls || [], tokenLike: STORE.seenToks || [] }
    });
  }
  if (req.method !== 'POST') return res.status(405).json({ error: 'method' });
  if (!STORE_URL || !STORE_TOKEN) {
    return res.status(503).json({
      error: 'no Redis REST credentials found in the environment',
      hint: 'Connect an Upstash Redis store to this project and redeploy. '
          + 'If the store is Neon (Postgres) rather than Redis, this route '
          + 'cannot talk to it.',
      urlLike: STORE.seenUrls || [], tokenLike: STORE.seenToks || []
    });
  }

  let b = req.body;
  if (typeof b === 'string') {
    if (b.length > MAX_BYTES) return res.status(413).json({ error: 'too large' });
    try { b = JSON.parse(b); } catch (e) { return res.status(400).json({ error: 'bad json' }); }
  }
  const { code, op, pid, data } = b || {};

  // Room codes are the only thing addressing state, so validate them strictly:
  // an unvalidated code is a key-injection hole into the whole keyspace.
  if (!code || !/^[A-Z0-9]{4}$/.test(code)) {
    return res.status(400).json({ error: 'bad code' });
  }
  if (pid && !/^[a-z0-9]{1,12}$/.test(pid)) {
    return res.status(400).json({ error: 'bad pid' });
  }

  const PK = 'df:' + code + ':p';   // hash of player id -> snapshot
  const WK = 'df:' + code + ':w';   // host-authored world doc
  const OK = 'df:' + code + ':o';   // hash of player id -> SDP offer
  const AK = 'df:' + code + ':a';   // hash of player id -> SDP answer

  try {
    switch (op) {

      // Player writes its own field. One hash means the host reads every
      // player in a single round trip instead of a list plus N gets.
      case 'push': {
        await redis(['HSET', PK, pid, JSON.stringify(data)]);
        // Refresh the TTL only now and then; doing it every tick would double
        // the command count for no benefit.
        if (Math.random() < 0.06) await redis(['EXPIRE', PK, TTL]);
        return res.status(200).json({ ok: 1 });
      }

      // Host publishes the authoritative world.
      case 'world': {
        await redis(['SET', WK, JSON.stringify(data), 'EX', TTL]);
        return res.status(200).json({ ok: 1 });
      }

      // Clients only need the world doc — one command.
      case 'wget': {
        const w = await redis(['GET', WK]);
        return res.status(200).json({ world: w ? JSON.parse(w) : null });
      }

      // Host needs every player snapshot.
      case 'pull': {
        const [w, ps] = await Promise.all([
          redis(['GET', WK]),
          redis(['HGETALL', PK])
        ]);
        const players = [];
        if (Array.isArray(ps)) {
          for (let i = 0; i < ps.length; i += 2) {
            try { players.push(JSON.parse(ps[i + 1])); } catch (e) { /* skip */ }
          }
        }
        return res.status(200).json({ world: w ? JSON.parse(w) : null, players });
      }

      case 'leave': {
        await redis(['HDEL', PK, pid, 'x']);
        await redis(['HDEL', OK, pid, 'x']);
        await redis(['HDEL', AK, pid, 'x']);
        return res.status(200).json({ ok: 1 });
      }

      // ---- WebRTC signalling ----
      // Low frequency by design: a handful of calls to establish each peer,
      // then position traffic leaves the store entirely and rides the data
      // channel. Non-trickle ICE, so one blob each way per peer.

      case 'offer': {                        // client publishes its offer
        await redis(['HSET', OK, pid, JSON.stringify(data)]);
        await redis(['EXPIRE', OK, SIG_TTL]);
        return res.status(200).json({ ok: 1 });
      }
      case 'offers': {                       // host collects pending offers
        const os = await redis(['HGETALL', OK]);
        const out = {};
        if (Array.isArray(os)) {
          for (let i = 0; i < os.length; i += 2) {
            try { out[os[i]] = JSON.parse(os[i + 1]); } catch (e) { /* skip */ }
          }
        }
        return res.status(200).json({ offers: out });
      }
      case 'answer': {                       // host publishes an answer
        await redis(['HSET', AK, pid, JSON.stringify(data)]);
        await redis(['EXPIRE', AK, SIG_TTL]);
        return res.status(200).json({ ok: 1 });
      }
      case 'answer-get': {                   // client waits for its answer
        const a = await redis(['HGET', AK, pid]);
        return res.status(200).json({ answer: a ? JSON.parse(a) : null });
      }
      case 'sig-clear': {                    // drop a consumed handshake
        await redis(['HDEL', OK, pid, 'x']);
        await redis(['HDEL', AK, pid, 'x']);
        return res.status(200).json({ ok: 1 });
      }

      default:
        return res.status(400).json({ error: 'bad op' });
    }
  } catch (e) {
    return res.status(500).json({ error: String((e && e.message) || e) });
  }
}
