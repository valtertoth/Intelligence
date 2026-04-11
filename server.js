/**
 * Toth Intelligence v2 — Server
 * Multi-AI Router + Meta Ads Read/Write + Streaming + Local Persistence
 * Zero dependencies — Node.js native modules only
 * 
 * Run:  node server.js
 * Open: http://localhost:8765
 */

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const crypto = require('crypto');

// Optional: xlsx for Excel parsing (npm install xlsx)
let XLSX;
try { XLSX = require('xlsx'); } catch { XLSX = null; }

const PORT = 8765;
const DATA_DIR = path.join(__dirname, '.data');
const META_API_VERSION = 'v21.0';
const GOOGLE_ADS_API_VERSION = 'v23';

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// ═══════════════════════════════════════════════════════════
// CACHE SYSTEM
// ═══════════════════════════════════════════════════════════

const CACHE = {};
const CACHE_TTL = { campaigns: 15 * 60 * 1000, shopify: 30 * 60 * 1000, default: 15 * 60 * 1000 };

function cacheGet(key) {
  const entry = CACHE[key];
  if (!entry) return null;
  if (Date.now() > entry.expiry) { delete CACHE[key]; return null; }
  return entry.data;
}

function cacheSet(key, data, ttlMs) {
  CACHE[key] = { data, expiry: Date.now() + (ttlMs || CACHE_TTL.default) };
}

function cacheClear(prefix) {
  for (const key of Object.keys(CACHE)) {
    if (!prefix || key.startsWith(prefix)) delete CACHE[key];
  }
}

// ═══════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', 'http://localhost:8765');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function json(res, code, data) {
  cors(res);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(data));
}

function log(icon, msg) {
  const ts = new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  console.log(`  ${icon} [${ts}] ${msg}`);
}

// ═══════════════════════════════════════════════════════════
// HTTPS REQUEST HELPERS
// ═══════════════════════════════════════════════════════════

function httpsRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const opts = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: options.method || 'GET',
      headers: options.headers || {},
      timeout: options.timeout || 30000,
    };

    const req = https.request(opts, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks).toString('utf8');
        try { resolve({ status: res.statusCode, data: JSON.parse(buf), raw: buf }); }
        catch { resolve({ status: res.statusCode, data: { raw: buf }, raw: buf }); }
      });
    });

    req.setTimeout(options.timeout || 30000, () => {
      req.destroy();
      reject(new Error(`Timeout: ${opts.hostname} (${options.timeout || 30000}ms)`));
    });
    req.on('error', reject);

    if (options.body) {
      const bodyStr = typeof options.body === 'string' ? options.body : JSON.stringify(options.body);
      if (!(options.headers && options.headers['Content-Type'])) {
        req.setHeader('Content-Type', 'application/json');
      }
      req.setHeader('Content-Length', Buffer.byteLength(bodyStr));
      req.write(bodyStr);
    }

    req.end();
  });
}

// Streaming HTTPS request — pipes chunks to callback
function httpsStream(url, options, onChunk, onDone) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const opts = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: options.method || 'POST',
      headers: options.headers || {},
      timeout: options.timeout || 180000,
    };

    const req = https.request(opts, res => {
      res.setEncoding('utf8');
      res.on('data', chunk => {
        try { onChunk(chunk); } catch (e) { /* ignore */ }
      });
      res.on('end', () => {
        if (onDone) onDone();
        resolve(res.statusCode);
      });
    });

    req.setTimeout(options.timeout || 180000, () => {
      req.destroy();
      reject(new Error('Stream timeout'));
    });
    req.on('error', reject);

    if (options.body) {
      const bodyStr = typeof options.body === 'string' ? options.body : JSON.stringify(options.body);
      if (!(options.headers && options.headers['Content-Type'])) {
        req.setHeader('Content-Type', 'application/json');
      }
      req.setHeader('Content-Length', Buffer.byteLength(bodyStr));
      req.write(bodyStr);
    }

    req.end();
  });
}

// ═══════════════════════════════════════════════════════════
// AI ROUTER — Multi-provider abstraction
// ═══════════════════════════════════════════════════════════

const AI_ROUTES = {
  // Claude — Strategic brain
  'audit.global':       { provider: 'claude',  model: 'claude-sonnet-4-20250514' },
  'audit.campaign':     { provider: 'claude',  model: 'claude-sonnet-4-20250514' },
  'strategy.create':    { provider: 'claude',  model: 'claude-sonnet-4-20250514' },
  'chat.strategist':    { provider: 'claude',  model: 'claude-sonnet-4-20250514' },
  'monitor.impact':     { provider: 'claude',  model: 'claude-sonnet-4-20250514' },
  'skills.learn':       { provider: 'claude',  model: 'claude-sonnet-4-20250514' },
  'budget.optimize':    { provider: 'claude',  model: 'claude-sonnet-4-20250514' },
  'detail.analyze':     { provider: 'claude',  model: 'claude-sonnet-4-20250514' },
  'adaptive.recommendations': { provider: 'claude', model: 'claude-sonnet-4-20250514' },
  'skills.update':      { provider: 'claude',  model: 'claude-sonnet-4-20250514' },
  'actions.generate':   { provider: 'claude',  model: 'claude-sonnet-4-20250514' },

  // GPT-4o — Copywriter
  'copy.headline':      { provider: 'openai',  model: 'gpt-4o' },
  'copy.body':          { provider: 'openai',  model: 'gpt-4o' },
  'copy.variations':    { provider: 'openai',  model: 'gpt-4o' },
  'copy.catalog':       { provider: 'openai',  model: 'gpt-4o' },

  // Gemini — Processor
  'extract.pdf':        { provider: 'gemini',  model: 'gemini-2.5-flash' },
  'extract.sales':      { provider: 'gemini',  model: 'gemini-2.5-flash' },
  'analyze.creative':   { provider: 'gemini',  model: 'gemini-2.5-flash' },
  'summarize.report':   { provider: 'gemini',  model: 'gemini-2.5-flash' },

  // GPT Image — Visual generator
  'image.generate':     { provider: 'openai',  model: 'gpt-image-1' },
  'image.mockup':       { provider: 'openai',  model: 'gpt-image-1' },
};

// Keys stored in memory (set via /config endpoint)
const KEYS = { anthropic: '', openai: '', gemini: '', meta: '', nexusUrl: '', nexusApiKey: '' };

// Load persisted keys
try {
  const saved = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'keys.json'), 'utf8'));
  Object.assign(KEYS, saved);
} catch { /* no saved keys */ }

function getRoute(taskType) {
  const route = AI_ROUTES[taskType];
  if (!route) throw new Error(`Unknown task type: ${taskType}`);

  // Check if provider key is available, fallback to Claude
  if (route.provider === 'openai' && !KEYS.openai) {
    if (taskType.startsWith('image.')) throw new Error('OpenAI API key required for image generation');
    return { provider: 'claude', model: 'claude-sonnet-4-20250514', fallback: true };
  }
  if (route.provider === 'gemini' && !KEYS.gemini) {
    return { provider: 'claude', model: 'claude-sonnet-4-20250514', fallback: true };
  }
  return route;
}

// ═══════════════════════════════════════════════════════════
// AI PROVIDER IMPLEMENTATIONS
// ═══════════════════════════════════════════════════════════

async function callClaude(payload, stream = false, res = null) {
  if (!KEYS.anthropic) throw new Error('Anthropic API key not configured');

  const body = {
    model: payload.model || 'claude-sonnet-4-20250514',
    max_tokens: payload.max_tokens || 2000,
    system: payload.system || '',
    messages: payload.messages || [],
  };

  if (payload.tools) body.tools = payload.tools;

  if (stream && res) {
    body.stream = true;
    cors(res);
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    });

    await httpsStream(
      'https://api.anthropic.com/v1/messages',
      {
        method: 'POST',
        headers: {
          'x-api-key': KEYS.anthropic,
          'anthropic-version': '2023-06-01',
        },
        body,
        timeout: 180000,
      },
      chunk => res.write(chunk),
      () => res.end()
    );
    return null; // Response already sent via stream
  }

  const result = await httpsRequest('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': KEYS.anthropic,
      'anthropic-version': '2023-06-01',
    },
    body,
    timeout: 180000,
  });

  if (result.data?.error) throw new Error(result.data.error.message);
  return result.data;
}

async function callOpenAI(payload) {
  if (!KEYS.openai) throw new Error('OpenAI API key not configured');

  // Image generation
  if (payload.model === 'gpt-image-1') {
    const result = await httpsRequest('https://api.openai.com/v1/images/generations', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${KEYS.openai}` },
      body: {
        model: 'gpt-image-1',
        prompt: payload.prompt,
        n: payload.n || 1,
        size: payload.size || '1024x1024',
        quality: payload.quality || 'high',
      },
      timeout: 120000,
    });
    if (result.data?.error) throw new Error(result.data.error.message);
    return result.data;
  }

  // Chat completion
  const result = await httpsRequest('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${KEYS.openai}` },
    body: {
      model: payload.model || 'gpt-4o',
      messages: payload.messages || [],
      max_tokens: payload.max_tokens || 1500,
      temperature: payload.temperature ?? 0.7,
    },
    timeout: 60000,
  });

  if (result.data?.error) throw new Error(result.data.error.message);
  return result.data;
}

async function callGemini(payload) {
  if (!KEYS.gemini) throw new Error('Gemini API key not configured');

  const model = payload.model || 'gemini-2.5-flash';
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${KEYS.gemini}`;

  const contents = [];
  for (const msg of (payload.messages || [])) {
    const parts = [];
    if (typeof msg.content === 'string') {
      parts.push({ text: msg.content });
    } else if (Array.isArray(msg.content)) {
      for (const c of msg.content) {
        if (c.type === 'text') parts.push({ text: c.text });
        if (c.type === 'image_url') parts.push({ inlineData: { mimeType: 'image/jpeg', data: c.image_url.url.split(',')[1] || '' } });
      }
    }
    contents.push({ role: msg.role === 'assistant' ? 'model' : 'user', parts });
  }

  const result = await httpsRequest(url, {
    method: 'POST',
    body: {
      contents,
      systemInstruction: payload.system ? { parts: [{ text: payload.system }] } : undefined,
      generationConfig: { maxOutputTokens: payload.max_tokens || 2000, temperature: payload.temperature ?? 0.4 },
    },
    timeout: 60000,
  });

  if (result.data?.error) throw new Error(result.data.error.message);
  return result.data;
}

// ═══════════════════════════════════════════════════════════
// META GRAPH API
// ═══════════════════════════════════════════════════════════

let _metaLastCall = 0;
const META_MIN_INTERVAL = 200; // ms between calls to avoid rate limiting

async function metaRead(apiPath, _retry = 0) {
  if (!KEYS.meta) throw new Error('Meta access token not configured');
  // Rate limiting: enforce minimum interval between calls
  const now = Date.now();
  const wait = META_MIN_INTERVAL - (now - _metaLastCall);
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  _metaLastCall = Date.now();

  // Ensure path starts with / to avoid v21.0act_xxx concatenation bug
  const safePath = apiPath.startsWith('/') ? apiPath : '/' + apiPath;
  const sep = safePath.includes('?') ? '&' : '?';
  const url = `https://graph.facebook.com/${META_API_VERSION}${safePath}${sep}access_token=${KEYS.meta}`;
  log('📡', `Meta GET ${apiPath.split('?')[0]}`);
  const result = await httpsRequest(url, { timeout: 25000 });
  if (result.data?.error) {
    const code = result.data.error.code;
    // Rate limit (code 4 or 17) or "User request limit reached" — retry with backoff
    if ((code === 4 || code === 17 || (result.data.error.message || '').includes('request limit')) && _retry < 3) {
      const delay = Math.pow(2, _retry + 1) * 1000; // 2s, 4s, 8s
      log('⏳', `Meta rate limited, retrying in ${delay/1000}s (attempt ${_retry + 1}/3)`);
      await new Promise(r => setTimeout(r, delay));
      return metaRead(apiPath, _retry + 1);
    }
    log('❌', `Meta error: ${result.data.error.message}`);
    throw new Error(result.data.error.message);
  }
  return result.data;
}

async function metaWrite(apiPath, body) {
  if (!KEYS.meta) throw new Error('Meta access token not configured');
  const url = `https://graph.facebook.com/${META_API_VERSION}${apiPath}`;

  // Parse any JSON strings back to objects for proper encoding
  const cleanBody = {};
  for (const [k, v] of Object.entries(body)) {
    if (typeof v === 'string' && (v.startsWith('{') || v.startsWith('['))) {
      try { cleanBody[k] = JSON.parse(v); } catch { cleanBody[k] = v; }
    } else {
      cleanBody[k] = v;
    }
  }
  cleanBody.access_token = KEYS.meta;

  log('✏️', `Meta POST ${apiPath} — keys: ${Object.keys(cleanBody).filter(k=>k!=='access_token').join(', ')}`);

  const parsed = new URL(url);
  return new Promise((resolve, reject) => {
    const bodyStr = JSON.stringify(cleanBody);
    const opts = {
      hostname: parsed.hostname,
      path: parsed.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(bodyStr),
      },
      timeout: 25000,
    };

    const req = https.request(opts, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks).toString('utf8');
        try {
          const data = JSON.parse(buf);
          if (data.error) {
            log('❌', `Meta write error: ${JSON.stringify(data.error)}`);
            resolve(data); // Return full error to caller instead of rejecting
          } else {
            log('✅', `Meta write success: ${apiPath}`);
            resolve(data);
          }
        } catch {
          reject(new Error(`Unexpected response: ${buf.substring(0, 200)}`));
        }
      });
    });

    req.setTimeout(25000, () => { req.destroy(); reject(new Error('Timeout Meta write')); });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
}

// ═══════════════════════════════════════════════════════════
// GOOGLE OAUTH TOKEN REFRESH
// ═══════════════════════════════════════════════════════════

let GOOGLE_ACCESS_TOKEN = '';
let GOOGLE_TOKEN_EXPIRY = 0;
let GOOGLE_REFRESH_TOKEN_UPDATED_AT = 0; // timestamp when OAuth callback updated refresh token

async function googleRefreshToken() {
  if (GOOGLE_ACCESS_TOKEN && Date.now() < GOOGLE_TOKEN_EXPIRY) {
    return GOOGLE_ACCESS_TOKEN;
  }
  if (!KEYS.googleClientId || !KEYS.googleClientSecret || !KEYS.googleRefreshToken) {
    throw new Error('Google OAuth credentials not configured (clientId, clientSecret, refreshToken)');
  }
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    client_id: KEYS.googleClientId,
    client_secret: KEYS.googleClientSecret,
    refresh_token: KEYS.googleRefreshToken,
  }).toString();

  log('🔑', 'Refreshing Google access token...');
  const result = await httpsRequest('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
    timeout: 15000,
  });

  if (result.data?.error) {
    throw new Error(`Google OAuth: ${result.data.error_description || result.data.error}`);
  }
  GOOGLE_ACCESS_TOKEN = result.data.access_token;
  GOOGLE_TOKEN_EXPIRY = Date.now() + ((result.data.expires_in - 60) * 1000);
  log('✅', 'Google access token refreshed');
  return GOOGLE_ACCESS_TOKEN;
}

// ═══════════════════════════════════════════════════════════
// GOOGLE ADS API (GAQL)
// ═══════════════════════════════════════════════════════════

async function googleAdsQuery(gaql) {
  const token = await googleRefreshToken();
  const customerId = (KEYS.googleCustomerId || '').replace(/-/g, '');
  if (!customerId) throw new Error('Google Ads Customer ID not configured');
  if (!KEYS.googleDevToken) throw new Error('Google Ads Developer Token not configured');

  const url = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${customerId}/googleAds:searchStream`;
  const headers = {
    'Authorization': `Bearer ${token}`,
    'developer-token': KEYS.googleDevToken,
    'Content-Type': 'application/json',
  };
  if (KEYS.googleLoginCustomerId) {
    headers['login-customer-id'] = KEYS.googleLoginCustomerId.replace(/-/g, '');
  }

  log('📡', `Google Ads GAQL: ${gaql.substring(0, 60)}...`);
  const result = await httpsRequest(url, {
    method: 'POST',
    headers,
    body: JSON.stringify({ query: gaql }),
    timeout: 60000,
  });

  if (result.status >= 400) {
    const errMsg = result.data?.error?.message || JSON.stringify(result.data).substring(0, 300);
    log('❌', `Google Ads error: ${errMsg}`);
    throw new Error(`Google Ads API: ${errMsg}`);
  }

  // searchStream returns array of batches, flatten results
  const results = [];
  const batches = Array.isArray(result.data) ? result.data : [result.data];
  for (const batch of batches) {
    if (batch?.results) results.push(...batch.results);
  }
  log('✅', `Google Ads: ${results.length} results`);
  return results;
}

// ═══════════════════════════════════════════════════════════
// GA4 DATA API
// ═══════════════════════════════════════════════════════════

async function ga4Report(reportBody) {
  const token = await googleRefreshToken();
  if (!KEYS.ga4PropertyId) throw new Error('GA4 Property ID not configured');

  const url = `https://analyticsdata.googleapis.com/v1beta/properties/${KEYS.ga4PropertyId}:runReport`;
  log('📡', `GA4 report: ${(reportBody.dimensions || []).map(d => d.name).join(', ')}`);

  const result = await httpsRequest(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(reportBody),
    timeout: 30000,
  });

  if (result.status >= 400) {
    const errMsg = result.data?.error?.message || JSON.stringify(result.data).substring(0, 300);
    log('❌', `GA4 error: ${errMsg}`);
    throw new Error(`GA4 API: ${errMsg}`);
  }

  log('✅', `GA4: ${result.data?.rowCount || 0} rows`);
  return result.data;
}

// ═══════════════════════════════════════════════════════════
// SHOPIFY GRAPHQL API
// ═══════════════════════════════════════════════════════════

async function shopifyGraphQL(query, variables = {}) {
  const store = KEYS.shopifyStore;
  const token = KEYS.shopifyToken;
  if (!store) throw new Error('Shopify store name not configured');
  if (!token) throw new Error('Shopify access token not configured');

  const url = `https://${store}.myshopify.com/admin/api/2026-01/graphql.json`;
  log('📡', `Shopify GraphQL: ${query.substring(0, 60)}...`);

  const result = await httpsRequest(url, {
    method: 'POST',
    headers: {
      'X-Shopify-Access-Token': token,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query, variables }),
    timeout: 30000,
  });

  if (result.status >= 400) {
    const errMsg = result.data?.errors?.[0]?.message || JSON.stringify(result.data).substring(0, 300);
    log('❌', `Shopify error: ${errMsg}`);
    throw new Error(`Shopify API: ${errMsg}`);
  }

  if (result.data?.errors) {
    const errMsg = result.data.errors.map(e => e.message).join('; ');
    log('❌', `Shopify GraphQL errors: ${errMsg}`);
    throw new Error(`Shopify GraphQL: ${errMsg}`);
  }

  log('✅', 'Shopify GraphQL OK');
  return result.data?.data || result.data;
}

// ═══════════════════════════════════════════════════════════
// RETRY WITH BACKOFF
// ═══════════════════════════════════════════════════════════

// Build GAQL date clause from days parameter
function gaqlDateClause(days) {
  if (!days || days === 30) return 'segments.date DURING LAST_30_DAYS';
  if (days === 7) return 'segments.date DURING LAST_7_DAYS';
  if (days === 14) return 'segments.date DURING LAST_14_DAYS';
  // For 60d, 90d — use explicit date range
  const end = new Date();
  const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000);
  const fmt = d => d.toISOString().split('T')[0].replace(/-/g, '');
  // Google Ads uses YYYY-MM-DD format in quotes
  const fmtQ = d => `'${d.toISOString().split('T')[0]}'`;
  return `segments.date BETWEEN ${fmtQ(start)} AND ${fmtQ(end)}`;
}

async function withRetry(fn, maxRetries = 3) {
  let lastErr;
  for (let i = 0; i < maxRetries; i++) {
    try { return await fn(); }
    catch (err) {
      lastErr = err;
      if (i < maxRetries - 1) {
        const delay = Math.pow(2, i) * 1000; // 1s, 2s, 4s
        log('🔄', `Retry ${i + 1}/${maxRetries} in ${delay}ms: ${err.message}`);
        await new Promise(r => setTimeout(r, delay));
      }
    }
  }
  throw lastErr;
}

// ═══════════════════════════════════════════════════════════
// LOCAL PERSISTENCE
// ═══════════════════════════════════════════════════════════

function sanitizeKey(key) {
  // Prevent path traversal
  return String(key).replace(/[^a-zA-Z0-9_-]/g, '');
}

function saveData(key, data) {
  const safe = sanitizeKey(key);
  if (!safe) throw new Error('Invalid key');
  const file = path.join(DATA_DIR, `${safe}.json`);
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
}

function loadData(key, fallback = null) {
  try {
    const safe = sanitizeKey(key);
    if (!safe) return fallback;
    const file = path.join(DATA_DIR, `${safe}.json`);
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch { return fallback; }
}

// ═══════════════════════════════════════════════════════════
// STATIC FILE SERVING
// ═══════════════════════════════════════════════════════════

const MIME = {
  '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript',
  '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg',
  '.svg': 'image/svg+xml', '.ico': 'image/x-icon',
};

function serveStatic(res, filePath) {
  const ext = path.extname(filePath);
  const mime = MIME[ext] || 'application/octet-stream';

  fs.readFile(filePath, (err, data) => {
    if (err) { json(res, 404, { error: { message: 'File not found' } }); return; }
    cors(res);
    res.writeHead(200, { 'Content-Type': `${mime}; charset=utf-8` });
    res.end(data);
  });
}

// ═══════════════════════════════════════════════════════════
// NEXUS STATE (must be declared before server handler)
// ═══════════════════════════════════════════════════════════
// GLOBAL HELPERS (used by both server handler and Nexus sync)
// ═══════════════════════════════════════════════════════════
function removeAccents(str) {
  return str.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

function hashForMeta(value, type) {
  if (!value || value === '(em branco)') return '';
  var normalized = String(value).trim().toLowerCase();
  normalized = removeAccents(normalized);
  if (!normalized) return '';
  if (type === 'phone') { normalized = normalized.replace(/\D/g, ''); if (!normalized || normalized.length < 8) return ''; }
  else if (type === 'email') { normalized = normalized.replace(/\s/g, ''); if (!normalized.includes('@')) return ''; }
  else if (type === 'name') { normalized = normalized.replace(/[^a-z]/g, ''); if (!normalized) return ''; }
  else if (type === 'city') { normalized = normalized.replace(/[^a-z]/g, ''); if (!normalized) return ''; }
  else if (type === 'state') { normalized = normalized.replace(/[^a-z]/g, ''); if (normalized.length > 2) normalized = normalized.substring(0, 2); }
  else if (type === 'country') { normalized = normalized.replace(/[^a-z]/g, ''); }
  else { normalized = normalized.replace(/[^a-z0-9]/g, ''); if (!normalized) return ''; }
  return crypto.createHash('sha256').update(normalized).digest('hex');
}

function splitName(fullName) {
  if (!fullName) return { fn: '', ln: '' };
  var parts = fullName.trim().split(/\s+/);
  return { fn: parts[0] || '', ln: parts[parts.length - 1] || '' };
}

// ═══════════════════════════════════════════════════════════
// INTELLIGENCE BRAIN — Cross-platform analysis & execution
// ═══════════════════════════════════════════════════════════
var _brainCache = null;
var _brainCacheExpiry = 0;
var _brainInterval = null;
var _brainAnalyzing = false;
var _brainConfig = null;
try { _brainConfig = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'brain-config.json'), 'utf8')); } catch { _brainConfig = { enabled: false, intervalHours: 6, autoExecuteRisk: 'low' }; }

async function collectAllIntelligenceData() {
  // Check cache (10 min TTL)
  if (_brainCache && Date.now() < _brainCacheExpiry) return _brainCache;

  log('🧠', 'Collecting intelligence data from all sources...');
  const results = {};

  // Safe wrapper for direct function calls
  function safe(fn) { return fn().catch(function(e) { log('⚠️', 'Brain data source error: ' + e.message); return null; }); }

  const aid = (KEYS.adAccountId || '').replace('act_', '');
  const siteUrl = KEYS.gscSiteUrl || 'https://www.tothmoveis.com.br/';
  const merchantId = KEYS.merchantId || '243128782';
  const dateClause = gaqlDateClause(30);

  // Collect in parallel using DIRECT function calls (no self-HTTP)
  const [meta, google, ga4Channels, ga4Funnel, ga4Devices, ga4Landing, gscQueries, gscPages, merchant, shopifyOrders, shopifyAbandoned] = await Promise.allSettled([
    KEYS.meta ? safe(() => metaRead('/act_' + aid + '/insights?date_preset=last_30d&fields=campaign_name,campaign_id,objective,spend,impressions,reach,clicks,ctr,actions,frequency&level=campaign&limit=30&sort=spend_descending')) : Promise.resolve(null),
    KEYS.googleDevToken ? safe(() => withRetry(() => googleAdsQuery('SELECT campaign.name, campaign.id, campaign.status, campaign.advertising_channel_type, campaign.bidding_strategy_type, metrics.cost_micros, metrics.clicks, metrics.impressions, metrics.conversions_value, metrics.search_impression_share FROM campaign WHERE ' + dateClause + ' AND campaign.status != \'REMOVED\''))) : Promise.resolve(null),
    KEYS.ga4PropertyId ? safe(() => ga4Report({ dateRanges: [{ startDate: '30daysAgo', endDate: 'today' }], dimensions: [{ name: 'sessionDefaultChannelGroup' }], metrics: [{ name: 'sessions' }, { name: 'totalUsers' }, { name: 'conversions' }, { name: 'bounceRate' }] })) : Promise.resolve(null),
    KEYS.ga4PropertyId ? safe(() => ga4Report({ dateRanges: [{ startDate: '30daysAgo', endDate: 'today' }], dimensions: [{ name: 'eventName' }], metrics: [{ name: 'eventCount' }, { name: 'totalUsers' }], dimensionFilter: { filter: { fieldName: 'eventName', inListFilter: { values: ['page_view','view_item','add_to_cart','begin_checkout','purchase','session_start'] } } } })) : Promise.resolve(null),
    KEYS.ga4PropertyId ? safe(() => ga4Report({ dateRanges: [{ startDate: '30daysAgo', endDate: 'today' }], dimensions: [{ name: 'deviceCategory' }], metrics: [{ name: 'sessions' }, { name: 'bounceRate' }] })) : Promise.resolve(null),
    KEYS.ga4PropertyId ? safe(() => ga4Report({ dateRanges: [{ startDate: '30daysAgo', endDate: 'today' }], dimensions: [{ name: 'landingPagePlusQueryString' }], metrics: [{ name: 'sessions' }, { name: 'conversions' }], orderBys: [{ metric: { metricName: 'sessions' }, desc: true }], limit: '20' })) : Promise.resolve(null),
    KEYS.googleRefreshToken ? safe(async () => { const token = await googleRefreshToken(); const encodedUrl = encodeURIComponent(siteUrl); const r = await httpsRequest('https://www.googleapis.com/webmasters/v3/sites/' + encodedUrl + '/searchAnalytics/query', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ startDate: new Date(Date.now()-30*86400000).toISOString().substring(0,10), endDate: new Date().toISOString().substring(0,10), dimensions: ['query'], rowLimit: 200 }), timeout: 30000 }); return r.data; }) : Promise.resolve(null),
    KEYS.googleRefreshToken ? safe(async () => { const token = await googleRefreshToken(); const encodedUrl = encodeURIComponent(siteUrl); const r = await httpsRequest('https://www.googleapis.com/webmasters/v3/sites/' + encodedUrl + '/searchAnalytics/query', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ startDate: new Date(Date.now()-30*86400000).toISOString().substring(0,10), endDate: new Date().toISOString().substring(0,10), dimensions: ['page'], rowLimit: 100 }), timeout: 30000 }); return r.data; }) : Promise.resolve(null),
    KEYS.googleRefreshToken ? safe(async () => { const token = await googleRefreshToken(); const r = await httpsRequest('https://shoppingcontent.googleapis.com/content/v2.1/' + merchantId + '/productstatuses?maxResults=250', { headers: { 'Authorization': 'Bearer ' + token }, timeout: 30000 }); return r.data; }) : Promise.resolve(null),
    (KEYS.shopifyStore && KEYS.shopifyToken) ? safe(async () => { const r = await shopifyGraphQL('{ orders(first:50,sortKey:CREATED_AT,reverse:true,query:"created_at:>' + new Date(Date.now()-90*86400000).toISOString().split('T')[0] + '"){edges{node{id name createdAt totalPriceSet{shopMoney{amount}} customer{firstName lastName}}}}}'); return (r.orders?.edges||[]).map(e=>e.node); }) : Promise.resolve(null),
    (KEYS.shopifyStore && KEYS.shopifyToken) ? safe(async () => { const r = await shopifyGraphQL('{ abandonedCheckouts(first:100,sortKey:CREATED_AT,reverse:true){edges{node{id createdAt totalPriceSet{shopMoney{amount}} lineItems(first:5){edges{node{title quantity}}}}}}}}'); return (r.abandonedCheckouts?.edges||[]).map(e=>e.node); }) : Promise.resolve(null),
  ]);

  // Extract results (handle failures)
  results.meta = meta.status === 'fulfilled' ? (meta.value?.data ? meta.value : { data: meta.value }) : null;
  results.google = google.status === 'fulfilled' ? (Array.isArray(google.value) ? google.value : []) : null;
  results.ga4Channels = ga4Channels.status === 'fulfilled' ? ga4Channels.value : null;
  results.ga4Funnel = ga4Funnel.status === 'fulfilled' ? ga4Funnel.value : null;
  results.ga4Devices = ga4Devices.status === 'fulfilled' ? ga4Devices.value : null;
  results.ga4Landing = ga4Landing.status === 'fulfilled' ? ga4Landing.value : null;
  results.gscQueries = gscQueries.status === 'fulfilled' ? gscQueries.value : null;
  results.gscPages = gscPages.status === 'fulfilled' ? gscPages.value : null;
  results.merchant = merchant.status === 'fulfilled' ? merchant.value : null;
  results.shopifyOrders = shopifyOrders.status === 'fulfilled' ? shopifyOrders.value : null;
  results.shopifyAbandoned = shopifyAbandoned.status === 'fulfilled' ? shopifyAbandoned.value : null;

  // Sales data (local)
  try {
    const conv = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'conversions.json'), 'utf8'));
    const orders = conv.orders || [];
    // Aggregate by product
    const byProduct = {};
    orders.forEach(o => { const p = o.product || 'N/A'; if (!byProduct[p]) byProduct[p] = { count: 0, revenue: 0 }; byProduct[p].count++; byProduct[p].revenue += o.revenue || 0; });
    // Aggregate by state
    const byState = {};
    orders.forEach(o => { const s = o.state || 'N/A'; if (!byState[s]) byState[s] = { count: 0, revenue: 0 }; byState[s].count++; byState[s].revenue += o.revenue || 0; });
    // Aggregate by month
    const byMonth = {};
    orders.forEach(o => { if (!o.date) return; const m = o.date.substring(0, 7); if (!byMonth[m]) byMonth[m] = { count: 0, revenue: 0 }; byMonth[m].count++; byMonth[m].revenue += o.revenue || 0; });
    // Customers
    const byCpf = {};
    orders.forEach(o => { const c = o.cpf || ''; if (!c) return; if (!byCpf[c]) byCpf[c] = { count: 0, revenue: 0 }; byCpf[c].count++; byCpf[c].revenue += o.revenue || 0; });
    const repeatBuyers = Object.values(byCpf).filter(c => c.count > 1).length;
    const totalCustomers = Object.keys(byCpf).length;

    results.sales = {
      totalOrders: orders.length,
      totalRevenue: orders.reduce((s, o) => s + (o.revenue || 0), 0),
      avgTicket: orders.length ? orders.reduce((s, o) => s + (o.revenue || 0), 0) / orders.length : 0,
      topProducts: Object.entries(byProduct).sort((a, b) => b[1].revenue - a[1].revenue).slice(0, 15).map(([p, d]) => ({ product: p, orders: d.count, revenue: d.revenue })),
      topStates: Object.entries(byState).sort((a, b) => b[1].revenue - a[1].revenue).slice(0, 10).map(([s, d]) => ({ state: s, orders: d.count, revenue: d.revenue })),
      monthlyTrend: Object.entries(byMonth).sort().map(([m, d]) => ({ month: m, orders: d.count, revenue: d.revenue })),
      totalCustomers,
      repeatBuyers,
      repeatRate: totalCustomers ? (repeatBuyers / totalCustomers * 100).toFixed(1) + '%' : '0%',
    };
  } catch { results.sales = null; }

  // ROAS history
  try { results.roasHistory = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'roas-history.json'), 'utf8')); } catch { results.roasHistory = null; }

  // Strategy
  try { results.strategy = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'estrategia-meta.json'), 'utf8')); } catch { results.strategy = null; }

  results.collectedAt = new Date().toISOString();
  _brainCache = results;
  _brainCacheExpiry = Date.now() + 10 * 60 * 1000;
  log('🧠', 'Intelligence data collected from ' + Object.keys(results).filter(k => results[k] !== null && k !== 'collectedAt').length + ' sources');
  return results;
}

function compressForClaude(data) {
  const sections = [];

  sections.push('=== VENDAS REAIS (CRM) ===');
  if (data.sales) {
    sections.push('Total: ' + data.sales.totalOrders + ' pedidos | R$' + data.sales.totalRevenue.toFixed(0) + ' | Ticket médio: R$' + data.sales.avgTicket.toFixed(0));
    sections.push('Clientes: ' + data.sales.totalCustomers + ' únicos | ' + data.sales.repeatBuyers + ' recorrentes (' + data.sales.repeatRate + ')');
    sections.push('Top produtos: ' + data.sales.topProducts.slice(0, 10).map(p => p.product.substring(0, 30) + ' R$' + p.revenue.toFixed(0) + ' (' + p.orders + 'x)').join(' | '));
    sections.push('Top estados: ' + data.sales.topStates.slice(0, 8).map(s => s.state + ':R$' + s.revenue.toFixed(0)).join(' | '));
    sections.push('Tendência mensal: ' + data.sales.monthlyTrend.slice(-6).map(m => m.month + ':' + m.orders + '/R$' + m.revenue.toFixed(0)).join(' | '));
  }

  sections.push('\n=== META ADS (30 dias) ===');
  if (data.meta && data.meta.data) {
    data.meta.data.forEach(c => {
      const msgs = (c.actions || []).find(a => a.action_type === 'onsite_conversion.total_messaging_connection');
      const lpv = (c.actions || []).find(a => a.action_type === 'landing_page_view');
      const atc = (c.actions || []).find(a => a.action_type === 'add_to_cart');
      sections.push(c.campaign_name + ' | spend:R$' + c.spend + ' | impr:' + c.impressions + ' | reach:' + c.reach + ' | freq:' + parseFloat(c.frequency || 0).toFixed(1) + ' | msgs:' + (msgs?.value || 0) + ' | lpv:' + (lpv?.value || 0) + ' | atc:' + (atc?.value || 0));
    });
  }

  sections.push('\n=== GOOGLE ADS (30 dias) ===');
  if (data.google && Array.isArray(data.google)) {
    data.google.forEach(r => {
      const c = r.campaign || {}; const m = r.metrics || {};
      sections.push(c.name + ' [' + c.status + '] | type:' + c.advertisingChannelType + ' | spend:R$' + (parseInt(m.costMicros || 0) / 1000000).toFixed(0) + ' | clicks:' + (m.clicks || 0) + ' | impr:' + (m.impressions || 0) + ' | searchShare:' + (m.searchImpressionShare ? (m.searchImpressionShare * 100).toFixed(0) + '%' : 'N/A'));
    });
  }

  sections.push('\n=== GA4 CANAIS ===');
  if (data.ga4Channels && data.ga4Channels.rows) {
    data.ga4Channels.rows.forEach(r => sections.push(r.dimensionValues[0].value + ' | sess:' + r.metricValues[0].value + ' | users:' + r.metricValues[1].value + ' | conv:' + r.metricValues[2].value + ' | bounce:' + (+r.metricValues[3].value * 100).toFixed(1) + '%'));
  }

  sections.push('\n=== GA4 FUNIL ===');
  if (data.ga4Funnel && data.ga4Funnel.rows) {
    data.ga4Funnel.rows.sort((a, b) => +b.metricValues[0].value - +a.metricValues[0].value).forEach(r => sections.push(r.dimensionValues[0].value + ' | count:' + r.metricValues[0].value + ' | users:' + r.metricValues[1].value));
  }

  sections.push('\n=== GSC TOP QUERIES ===');
  if (data.gscQueries && data.gscQueries.rows) {
    data.gscQueries.rows.slice(0, 30).forEach(r => sections.push(r.keys[0] + ' | clicks:' + r.clicks + ' | impr:' + r.impressions + ' | ctr:' + (r.ctr * 100).toFixed(1) + '% | pos:' + r.position.toFixed(1)));
  }

  sections.push('\n=== MERCHANT CENTER ===');
  if (data.merchant && data.merchant.resources) {
    const prods = data.merchant.resources;
    let approved = 0, disapproved = 0;
    const issues = {};
    prods.forEach(p => {
      const itemIssues = p.itemLevelIssues || [];
      if (itemIssues.some(i => i.servability === 'disapproved')) { disapproved++; itemIssues.filter(i => i.servability === 'disapproved').forEach(i => { issues[i.description] = (issues[i.description] || 0) + 1; }); }
      else approved++;
    });
    sections.push('Total: ' + prods.length + ' | Approved: ' + approved + ' | Disapproved: ' + disapproved);
    Object.entries(issues).sort((a, b) => b[1] - a[1]).forEach(([desc, count]) => sections.push('  Issue [' + count + 'x]: ' + desc));
  }

  sections.push('\n=== SHOPIFY ===');
  if (data.shopifyOrders && Array.isArray(data.shopifyOrders)) {
    sections.push('Orders (90d): ' + data.shopifyOrders.length);
  }
  if (data.shopifyAbandoned && Array.isArray(data.shopifyAbandoned)) {
    let abandonedValue = 0;
    data.shopifyAbandoned.forEach(c => { abandonedValue += parseFloat(c.totalPriceSet?.shopMoney?.amount || 0); });
    sections.push('Abandoned checkouts: ' + data.shopifyAbandoned.length + ' | Value: R$' + abandonedValue.toFixed(0));
  }

  if (data.roasHistory && data.roasHistory.months) {
    sections.push('\n=== ROAS HISTORICO ===');
    data.roasHistory.months.slice(-6).forEach(m => sections.push(m.month + ' | vendas:' + m.orders + ' | receita:R$' + m.revenue.toFixed(0) + ' | meta:R$' + m.metaSpend.toFixed(0) + ' | google:R$' + m.googleSpend.toFixed(0) + ' | ROAS:' + m.roas.toFixed(1) + 'x'));
  }

  return sections.join('\n');
}

async function runBrainAnalysis() {
  if (_brainAnalyzing) return { error: 'Analysis already in progress' };
  _brainAnalyzing = true;

  try {
    const data = await collectAllIntelligenceData();
    var compressed = compressForClaude(data);

    // Add learning loop: past actions and their results
    try {
      var pastActions = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'intelligence-actions.json'), 'utf8'));
      if (pastActions.length > 0) {
        compressed += '\n\n=== AÇÕES EXECUTADAS ANTERIORMENTE (aprender com resultados) ===';
        pastActions.slice(-10).forEach(function(a) {
          compressed += '\n' + a.executedAt?.substring(0,10) + ' | ' + a.action + ' | ' + (a.success ? 'SUCESSO' : 'FALHOU: ' + (a.error || ''));
        });
      }
    } catch {}

    // Add autopilot alerts
    try {
      var autopilotAlerts = loadData('autopilot-alerts') || [];
      var lastAlerts = autopilotAlerts.slice(-1)[0];
      if (lastAlerts && lastAlerts.alerts && lastAlerts.alerts.length > 0) {
        compressed += '\n\n=== ALERTAS AUTOPILOT (problemas detectados automaticamente) ===';
        lastAlerts.alerts.forEach(function(a) { compressed += '\n⚠️ ' + a.message; });
      }
    } catch {}

    log('🧠', 'Sending ' + compressed.length + ' chars to Claude for analysis...');

    const systemPrompt = `You are the Intelligence Brain of Toth Intelligence, a marketing analytics platform for Toth Móveis — a premium furniture store in Brazil.

Your job: analyze ALL data from 8 marketing platforms, cross-reference them, and generate ACTIONABLE execution plans.

## Business Context
- Currency: BRL (Brazilian Real)
- Business: Premium solid wood furniture (average ticket ~R$5,942)
- Sales funnel: Ad → Site → WhatsApp → Conversation → Purchase
- ALL sales happen via WhatsApp (zero online checkout)
- Monthly revenue: ~R$142k (March 2026)
- Total ad budget: R$300/day (Meta + Google)
- Google PMax has proven ROAS 25x
- Meta campaigns: some work (R$6/msg), others waste money (R$154/msg)

## Analysis Framework
Cross ALL data sources and find:

1. PRODUCT: Which products have demand (GSC/sales) but no ad investment? Which are advertised but don't sell?
2. AUDIENCE: Where are real buyers (by state/city)? Is the ad budget allocated there? Is the audience quality degrading?
3. TIMING: When do sales peak (day of week, week of month)? Are ads running at those times?
4. FUNNEL: Where does the funnel break? View→Cart rate? Cart→WhatsApp rate? What's causing drop-offs?
5. OPPORTUNITY: High-impression GSC queries with no ads? Products in Merchant Center with issues? Abandoned carts to recover?

## Output Format
Return ONLY valid JSON (no markdown, no backticks, no explanation outside JSON):
{
  "summary": "2-3 sentence executive summary in Portuguese",
  "healthScore": 0-100,
  "insights": [
    {
      "dimension": "product|audience|timing|funnel|opportunity",
      "severity": "critical|warning|opportunity",
      "title": "Short title in Portuguese",
      "detail": "Detailed explanation with specific numbers in Portuguese"
    }
  ],
  "actions": [
    {
      "priority": 1-5,
      "title": "Action title in Portuguese",
      "description": "What to do and why in Portuguese",
      "estimatedImpact": "Expected result in R$",
      "type": "meta_pause|meta_budget|google_budget|capi_send|manual",
      "executable": true or false,
      "risk": "low|medium|high",
      "params": {}
    }
  ],
  "budgetReallocation": {
    "currentMeta": 0,
    "currentGoogle": 0,
    "recommendedMeta": 0,
    "recommendedGoogle": 0,
    "reasoning": "Why in Portuguese"
  }
}`;

    const aiBody = JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 4000,
      system: systemPrompt,
      messages: [{ role: 'user', content: compressed }]
    });

    const aiResult = await httpsRequest('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': KEYS.anthropic, 'anthropic-version': '2023-06-01' },
      body: aiBody,
      timeout: 60000,
    });

    if (aiResult.status >= 400) {
      throw new Error('Claude API error: ' + (aiResult.data?.error?.message || aiResult.status));
    }

    const text = aiResult.data?.content?.[0]?.text || '';
    let analysis;
    try { analysis = JSON.parse(text); }
    catch { analysis = { summary: text, healthScore: 50, insights: [], actions: [], budgetReallocation: {} }; }

    analysis.analyzedAt = new Date().toISOString();
    analysis.dataTokens = compressed.length;

    // Save to history
    const historyPath = path.join(DATA_DIR, 'intelligence-history.json');
    let history = [];
    try { history = JSON.parse(fs.readFileSync(historyPath, 'utf8')); } catch {}
    history.push(analysis);
    if (history.length > 20) history.splice(0, history.length - 20);
    fs.writeFileSync(historyPath, JSON.stringify(history, null, 2));

    log('🧠', 'Brain analysis complete: score=' + analysis.healthScore + ' insights=' + (analysis.insights || []).length + ' actions=' + (analysis.actions || []).length);
    return analysis;
  } catch (e) {
    log('❌', 'Brain analysis error: ' + e.message);
    return { error: e.message };
  } finally {
    _brainAnalyzing = false;
  }
}

async function executeBrainAction(action) {
  log('🧠', 'Executing action: ' + action.title);
  const logEntry = { action: action.title, type: action.type, executedAt: new Date().toISOString(), success: false };

  try {
    let result;
    if (action.type === 'meta_pause' && action.params?.campaignId) {
      result = await new Promise((resolve, reject) => {
        const body = JSON.stringify({ path: '/' + action.params.campaignId, params: { status: 'PAUSED' } });
        const req = http.request({ hostname: 'localhost', port: PORT, path: '/meta/write', method: 'POST', headers: { 'Content-Type': 'application/json' } }, (res) => {
          let d = ''; res.on('data', c => d += c); res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve({ raw: d }); } });
        });
        req.on('error', reject); req.write(body); req.end();
      });
    } else if (action.type === 'meta_budget' && action.params?.campaignId && action.params?.dailyBudget) {
      result = await new Promise((resolve, reject) => {
        const body = JSON.stringify({ path: '/' + action.params.campaignId, params: { daily_budget: action.params.dailyBudget } });
        const req = http.request({ hostname: 'localhost', port: PORT, path: '/meta/write', method: 'POST', headers: { 'Content-Type': 'application/json' } }, (res) => {
          let d = ''; res.on('data', c => d += c); res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve({ raw: d }); } });
        });
        req.on('error', reject); req.write(body); req.end();
      });
    } else if (action.type === 'capi_send') {
      result = { message: 'CAPI send requires manual trigger with specific order data' };
    } else {
      result = { message: 'Action type ' + action.type + ' requires manual execution' };
    }

    logEntry.success = !result?.error;
    logEntry.result = result;
    log('🧠', 'Action executed: ' + (logEntry.success ? 'SUCCESS' : 'FAILED'));
  } catch (e) {
    logEntry.error = e.message;
    log('❌', 'Action execution error: ' + e.message);
  }

  // Save action log
  const logPath = path.join(DATA_DIR, 'intelligence-actions.json');
  let actions = [];
  try { actions = JSON.parse(fs.readFileSync(logPath, 'utf8')); } catch {}
  actions.push(logEntry);
  if (actions.length > 100) actions.splice(0, actions.length - 100);
  fs.writeFileSync(logPath, JSON.stringify(actions, null, 2));

  return logEntry;
}

function _startBrainAutoAnalysis() {
  if (_brainInterval) clearInterval(_brainInterval);
  if (!_brainConfig || !_brainConfig.enabled) return;
  var intervalMs = (_brainConfig.intervalHours || 6) * 60 * 60 * 1000;
  _brainInterval = setInterval(function() {
    runBrainAnalysis().then(function(analysis) {
      if (analysis && !analysis.error && _brainConfig.autoExecuteRisk !== 'none') {
        (analysis.actions || []).forEach(function(action) {
          if (action.executable && action.risk === 'low') {
            executeBrainAction(action).catch(function() {});
          }
        });
      }
    }).catch(function() {});
  }, intervalMs);
  log('🧠', 'Brain auto-analysis started (every ' + _brainConfig.intervalHours + 'h)');
}

// ═══════════════════════════════════════════════════════════
// AUTOPILOT — Rule-based campaign protection
// Checks every 30 min for campaigns that need intervention
// ═══════════════════════════════════════════════════════════
var _autopilotInterval = null;
var _autopilotRunning = false;

var AUTOPILOT_RULES = [
  {
    name: 'Frequência > 5 → Pausar',
    check: function(campaign) {
      return parseFloat(campaign.frequency || 0) > 5;
    },
    action: 'ALERT',
    message: function(c) { return 'Campanha "' + c.campaign_name + '" com frequência ' + parseFloat(c.frequency).toFixed(1) + ' (público saturado)'; }
  },
  {
    name: 'Custo/msg > R$50 com > R$100 gasto',
    check: function(campaign) {
      var msgs = (campaign.actions || []).find(function(a) { return a.action_type === 'onsite_conversion.total_messaging_connection'; });
      var msgCount = parseInt(msgs?.value || 0);
      var spend = parseFloat(campaign.spend || 0);
      return spend > 100 && msgCount > 0 && (spend / msgCount) > 50;
    },
    action: 'ALERT',
    message: function(c) {
      var msgs = (c.actions || []).find(function(a) { return a.action_type === 'onsite_conversion.total_messaging_connection'; });
      var cost = parseFloat(c.spend) / parseInt(msgs?.value || 1);
      return 'Campanha "' + c.campaign_name + '" com custo/msg R$' + cost.toFixed(0) + ' (desperdiçando budget)';
    }
  },
  {
    name: 'Gasto > R$200 com 0 mensagens',
    check: function(campaign) {
      var msgs = (campaign.actions || []).find(function(a) { return a.action_type === 'onsite_conversion.total_messaging_connection'; });
      return parseFloat(campaign.spend || 0) > 200 && (!msgs || parseInt(msgs.value) === 0);
    },
    action: 'ALERT',
    message: function(c) { return 'Campanha "' + c.campaign_name + '" gastou R$' + c.spend + ' sem gerar nenhuma mensagem WhatsApp'; }
  }
];

async function runAutopilotCheck() {
  if (_autopilotRunning || !KEYS.meta) return;
  _autopilotRunning = true;

  try {
    var aid = (KEYS.adAccountId || '').replace('act_', '');
    var data = await metaRead('/act_' + aid + '/insights?date_preset=last_7d&fields=campaign_name,campaign_id,spend,impressions,reach,frequency,actions&level=campaign&limit=20&sort=spend_descending');
    var campaigns = data?.data || (Array.isArray(data) ? data : []);

    var alerts = [];
    for (var c of campaigns) {
      for (var rule of AUTOPILOT_RULES) {
        if (rule.check(c)) {
          alerts.push({ rule: rule.name, campaign: c.campaign_name, campaignId: c.campaign_id, message: rule.message(c), action: rule.action });
        }
      }
    }

    if (alerts.length > 0) {
      log('🤖', 'Autopilot: ' + alerts.length + ' alerts triggered');
      alerts.forEach(function(a) { log('⚠️', 'AUTOPILOT: ' + a.message); });

      // Save alerts
      var alertHistory = loadData('autopilot-alerts') || [];
      alertHistory.push({ timestamp: new Date().toISOString(), alerts: alerts });
      if (alertHistory.length > 50) alertHistory.splice(0, alertHistory.length - 50);
      saveData('autopilot-alerts', alertHistory);
    }
  } catch (e) {
    log('❌', 'Autopilot error: ' + e.message);
  } finally {
    _autopilotRunning = false;
  }
}

function _startAutopilot() {
  if (_autopilotInterval) clearInterval(_autopilotInterval);
  _autopilotInterval = setInterval(function() { runAutopilotCheck().catch(function() {}); }, 30 * 60 * 1000); // Every 30 min
  log('🤖', 'Autopilot started (checks every 30 min)');
  // Run first check after 2 min (let server warm up)
  setTimeout(function() { runAutopilotCheck().catch(function() {}); }, 2 * 60 * 1000);
}

// ═══════════════════════════════════════════════════════════
// NEXUS AUTO-SYNC ENGINE (must be before http.createServer)
// ═══════════════════════════════════════════════════════════
var _nexusSyncInterval = null;
var _nexusSyncing = false;

function _nexusRequest(endpoint, method, body) {
  method = method || 'GET';
  const url = KEYS.nexusUrl + endpoint;
  const headers = { 'x-nexus-api-key': KEYS.nexusApiKey, 'Content-Type': 'application/json' };
  const parsed = new URL(url);
  const mod = parsed.protocol === 'https:' ? https : http;
  return new Promise(function(resolve, reject) {
    var req = mod.request({ hostname: parsed.hostname, port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80), path: parsed.pathname + parsed.search, method: method, headers: headers, timeout: 30000 }, function(res) {
      var chunks = [];
      res.on('data', function(c) { chunks.push(c); });
      res.on('end', function() { var raw = Buffer.concat(chunks).toString('utf8'); try { resolve({ status: res.statusCode, data: JSON.parse(raw) }); } catch(e) { resolve({ status: res.statusCode, data: { raw: raw } }); } });
    });
    req.on('error', reject);
    req.setTimeout(30000, function() { req.destroy(); reject(new Error('Timeout')); });
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

function _nexusSyncConversions() {
  if (!KEYS.nexusUrl || !KEYS.nexusApiKey) return Promise.resolve({ success: false, error: 'Nexus não conectado' });
  if (_nexusSyncing) return Promise.resolve({ success: false, error: 'Sincronização já em andamento' });
  _nexusSyncing = true;

  var syncResult = { timestamp: new Date().toISOString(), meta: { fetched: 0, sent: 0, errors: [] }, google: { fetched: 0, sent: 0, errors: [] } };

  return (async function() {
    try {
      // Fetch pending Meta conversions
      if (KEYS.meta && KEYS.pixelId) {
        try {
          var metaResp = await _nexusRequest('/api/intelligence/nexus/conversions/pending?platform=meta');
          var metaEvents = metaResp.data?.events || [];
          syncResult.meta.fetched = metaEvents.length;
          if (metaEvents.length > 0) {
            log('📥', 'Nexus → ' + metaEvents.length + ' conversões pendentes (Meta)');
            var sevenDaysAgo = new Date(); sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
            var cutoffStr = sevenDaysAgo.toISOString().substring(0, 10);
            var ackResults = [];
            for (var ev of metaEvents) {
              var orderDate = ev.created_at ? ev.created_at.substring(0, 10) : new Date().toISOString().substring(0, 10);
              if (orderDate < cutoffStr) { ackResults.push({ id: ev.id, success: false, error: 'Older than 7 days' }); continue; }
              var eventTime = Math.floor(new Date(orderDate + 'T18:00:00-03:00').getTime() / 1000);
              var userData = { country: [hashForMeta('br', 'country')] };
              if (ev.contact_email) { var he = hashForMeta(ev.contact_email, 'email'); if (he) userData.em = [he]; }
              if (ev.contact_phone || ev.contact_wa_id) { var ph = String(ev.contact_phone || ev.contact_wa_id).replace(/\D/g, ''); if (ph.length === 10 || ph.length === 11) ph = '55' + ph; if (ph.length >= 12) { var hp = hashForMeta(ph, 'phone'); if (hp) userData.ph = [hp]; } }
              if (ev.contact_wa_id) { var hid = hashForMeta(ev.contact_wa_id); if (hid) userData.external_id = [hid]; }
              // Add fbc/fbp from Nexus attribution if available
              if (ev.attr_fbc) userData.fbc = ev.attr_fbc;
              if (ev.attr_fbp) userData.fbp = ev.attr_fbp;
              var actionSource = ev.attr_fbc ? 'website' : 'business_messaging';
              var capiEvent = { event_name: 'Purchase', event_time: eventTime, action_source: actionSource, user_data: userData, custom_data: { value: Number(ev.value || 0), currency: ev.currency || 'BRL', content_name: ev.product_name || '', order_id: ev.id, content_type: 'product' } };
              try {
                var capiUrl = 'https://graph.facebook.com/' + META_API_VERSION + '/' + KEYS.pixelId + '/events';
                var capiResult = await httpsRequest(capiUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ data: [capiEvent], access_token: KEYS.meta }), timeout: 30000 });
                if (capiResult.data?.events_received > 0) { ackResults.push({ id: ev.id, success: true, eventId: capiResult.data?.fbtrace_id || '' }); syncResult.meta.sent++; }
                else { var errMsg = capiResult.data?.error?.message || 'No events received'; ackResults.push({ id: ev.id, success: false, error: errMsg }); syncResult.meta.errors.push(errMsg); }
              } catch (capiErr) { ackResults.push({ id: ev.id, success: false, error: capiErr.message }); syncResult.meta.errors.push(capiErr.message); }
            }
            if (ackResults.length > 0) { try { await _nexusRequest('/api/intelligence/nexus/conversions/acknowledge', 'POST', { platform: 'meta', results: ackResults }); log('✅', 'Nexus Meta ack: ' + ackResults.filter(function(r){return r.success}).length + '/' + ackResults.length); } catch (e) { log('❌', 'Nexus Meta ack failed: ' + e.message); } }
          }
        } catch (metaErr) { syncResult.meta.errors.push(metaErr.message); log('❌', 'Nexus Meta sync error: ' + metaErr.message); }
      }

      // Fetch pending Google conversions
      if (KEYS.googleDevToken && KEYS.googleRefreshToken && KEYS.googleCustomerId) {
        try {
          var googleResp = await _nexusRequest('/api/intelligence/nexus/conversions/pending?platform=google');
          var googleEvents = googleResp.data?.events || [];
          syncResult.google.fetched = googleEvents.length;
          if (googleEvents.length > 0) {
            log('📥', 'Nexus → ' + googleEvents.length + ' conversões pendentes (Google)');
            var orders = googleEvents.map(function(ev) { return { orderId: ev.id, revenue: ev.value || 0, product: ev.product_name || '', phone: ev.contact_phone || ev.contact_wa_id || '', email: ev.contact_email || '', customer: '', date: ev.created_at ? ev.created_at.substring(0, 10) : new Date().toISOString().substring(0, 10) }; });
            var googleBody = JSON.stringify({ orders: orders });
            var googleResult = await new Promise(function(resolve, reject) {
              var req = http.request({ hostname: 'localhost', port: PORT, path: '/conversions/send-google', method: 'POST', headers: { 'Content-Type': 'application/json' } }, function(res) { var c = []; res.on('data', function(d){c.push(d)}); res.on('end', function(){ try{resolve(JSON.parse(Buffer.concat(c).toString()))}catch(e){resolve({success:false})} }); });
              req.on('error', reject); req.write(googleBody); req.end();
            });
            var gAck = googleEvents.map(function(ev) { return { id: ev.id, success: googleResult.success !== false, error: googleResult.error || undefined }; });
            syncResult.google.sent = googleResult.success ? googleEvents.length : 0;
            try { await _nexusRequest('/api/intelligence/nexus/conversions/acknowledge', 'POST', { platform: 'google', results: gAck }); log('✅', 'Nexus Google ack: ' + gAck.filter(function(r){return r.success}).length + '/' + gAck.length); } catch (e) { log('❌', 'Nexus Google ack failed: ' + e.message); }
          }
        } catch (googleErr) { syncResult.google.errors.push(googleErr.message); log('❌', 'Nexus Google sync error: ' + googleErr.message); }
      }

      // Save history
      var history = loadData('nexus-sync-history') || [];
      history.push(syncResult);
      if (history.length > 100) history.splice(0, history.length - 100);
      saveData('nexus-sync-history', history);
      var totalFetched = syncResult.meta.fetched + syncResult.google.fetched;
      var totalSent = syncResult.meta.sent + syncResult.google.sent;
      if (totalFetched > 0) log('📊', 'Nexus sync: ' + totalSent + '/' + totalFetched + ' conversões enviadas');
      return { success: true, timestamp: syncResult.timestamp, meta: syncResult.meta, google: syncResult.google };
    } catch (e) { log('❌', 'Nexus sync error: ' + e.message); return { success: false, error: e.message }; }
    finally { _nexusSyncing = false; }
  })();
}

function _startNexusAutoSync() {
  if (_nexusSyncInterval) clearInterval(_nexusSyncInterval);
  if (!KEYS.nexusUrl || !KEYS.nexusApiKey) return;
  _nexusSyncInterval = setInterval(function() { _nexusSyncConversions().catch(function(e) { log('❌', 'Nexus auto-sync error: ' + e.message); }); }, 5 * 60 * 1000);
  log('🔄', 'Nexus auto-sync started (every 5 min) → ' + KEYS.nexusUrl);
}

// ═══════════════════════════════════════════════════════════
// HTTP SERVER
// ═══════════════════════════════════════════════════════════

http.createServer(async (req, res) => {
  cors(res);
  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

  const url = new URL(req.url, `http://localhost:${PORT}`);
  const route = url.pathname;

  // Request logging (skip static files)
  if (route !== '/' && route !== '/index.html' && route !== '/health' && !route.startsWith('/favicon')) {
    log('→', `${req.method} ${route}`);
  }

  try {
    // ── Static files ──
    if (req.method === 'GET' && (route === '/' || route === '/index.html')) {
      serveStatic(res, path.join(__dirname, 'index.html'));
      return;
    }

    // ── Health check ──
    if (req.method === 'GET' && route === '/health') {
      json(res, 200, {
        status: 'ok',
        keys: {
          anthropic: !!KEYS.anthropic,
          openai: !!KEYS.openai,
          gemini: !!KEYS.gemini,
          meta: !!KEYS.meta,
          google: !!(KEYS.googleDevToken && KEYS.googleRefreshToken),
          ga4: !!KEYS.ga4PropertyId,
          shopify: !!(KEYS.shopifyStore && KEYS.shopifyToken),
          nexus: !!(KEYS.nexusUrl && KEYS.nexusApiKey),
        },
        nexus: {
          connected: !!(KEYS.nexusUrl && KEYS.nexusApiKey),
          url: KEYS.nexusUrl || null,
          autoSyncActive: !!_nexusSyncInterval,
        },
        savedKeys: {
          anthropic: KEYS.anthropic ? '***' + KEYS.anthropic.slice(-4) : '',
          openai: KEYS.openai ? '***' + KEYS.openai.slice(-4) : '',
          gemini: KEYS.gemini ? '***' + KEYS.gemini.slice(-4) : '',
          meta: KEYS.meta ? '***' + KEYS.meta.slice(-4) : '',
          adAccountId: KEYS.adAccountId || '',
          pixelId: KEYS.pixelId || '',
          googleDevToken: KEYS.googleDevToken ? '***' + KEYS.googleDevToken.slice(-4) : '',
          googleClientId: KEYS.googleClientId || '',
          googleClientSecret: KEYS.googleClientSecret ? '***' : '',
          googleRefreshToken: KEYS.googleRefreshToken ? '***' + KEYS.googleRefreshToken.slice(-4) : '',
          googleCustomerId: KEYS.googleCustomerId || '',
          googleLoginCustomerId: KEYS.googleLoginCustomerId || '',
          ga4PropertyId: KEYS.ga4PropertyId || '',
          shopifyStore: KEYS.shopifyStore || '',
          shopifyToken: KEYS.shopifyToken ? '***' + KEYS.shopifyToken.slice(-4) : '',
          shopifyClientId: KEYS.shopifyClientId || '',
          shopifyClientSecret: KEYS.shopifyClientSecret ? '***' : '',
          nexusUrl: KEYS.nexusUrl || '',
          nexusApiKey: KEYS.nexusApiKey ? '***' + KEYS.nexusApiKey.slice(-4) : '',
        },
      });
      return;
    }

    // ── Configure keys ──
    if (req.method === 'POST' && route === '/config') {
      const body = JSON.parse(await readBody(req));
      const keyFields = [
        'anthropic', 'openai', 'gemini', 'meta', 'adAccountId', 'pixelId',
        'googleDevToken', 'googleClientId', 'googleClientSecret', 'googleRefreshToken',
        'googleCustomerId', 'googleLoginCustomerId',
        'ga4PropertyId',
        'shopifyStore', 'shopifyToken', 'shopifyClientId', 'shopifyClientSecret',
        'nexusUrl', 'nexusApiKey',
        'gscSiteUrl', 'merchantId',
      ];
      // Protect googleRefreshToken from being overwritten by stale frontend values
      // when it was recently updated by the OAuth callback
      const oauthRecentlyUpdated = GOOGLE_REFRESH_TOKEN_UPDATED_AT && (Date.now() - GOOGLE_REFRESH_TOKEN_UPDATED_AT < 5 * 60 * 1000);
      for (const field of keyFields) {
        if (body[field] !== undefined) {
          // NEVER accept masked values (***) — they corrupt real keys
          if (typeof body[field] === 'string' && body[field].startsWith('***')) {
            log('🛡️', 'BLOCKED masked value for ' + field + ' — keeping original');
            continue;
          }
          // Skip empty values if we already have a real value
          if (!body[field] && KEYS[field] && KEYS[field].length > 5) {
            continue;
          }
          // Skip if OAuth recently set a new refresh token and frontend sends a different (stale) value
          if (field === 'googleRefreshToken' && oauthRecentlyUpdated && body[field] !== KEYS.googleRefreshToken) {
            log('🔑', 'Skipping stale googleRefreshToken from frontend (OAuth recently updated)');
            continue;
          }
          KEYS[field] = body[field];
        }
      }
      // Clear Google token cache when credentials change (but not if OAuth just updated the token)
      if (body.googleClientId || body.googleClientSecret || (body.googleRefreshToken && !oauthRecentlyUpdated)) {
        GOOGLE_ACCESS_TOKEN = '';
        GOOGLE_TOKEN_EXPIRY = 0;
        cacheClear('google');
        cacheClear('ga4');
      }
      if (body.shopifyStore || body.shopifyToken) {
        cacheClear('shopify');
      }
      // Persist all keys locally
      const persist = {};
      for (const field of keyFields) { persist[field] = KEYS[field] || ''; }
      saveData('keys', persist);
      log('🔑', 'Keys updated');
      json(res, 200, {
        ok: true,
        keys: {
          anthropic: !!KEYS.anthropic, openai: !!KEYS.openai, gemini: !!KEYS.gemini,
          meta: !!KEYS.meta, google: !!(KEYS.googleDevToken && KEYS.googleRefreshToken),
          ga4: !!KEYS.ga4PropertyId, shopify: !!(KEYS.shopifyStore && KEYS.shopifyToken),
          nexus: !!(KEYS.nexusUrl && KEYS.nexusApiKey),
        },
      });
      return;
    }

    // ── Meta Read ──
    if (req.method === 'POST' && route === '/meta/read') {
      const { path: apiPath } = JSON.parse(await readBody(req));
      if (!apiPath) { json(res, 400, { error: { message: 'path required' } }); return; }
      const data = await metaRead(apiPath);
      json(res, 200, data);
      return;
    }

    // ── Meta Write ──
    if (req.method === 'POST' && route === '/meta/write') {
      const { path: apiPath, params } = JSON.parse(await readBody(req));
      if (!apiPath || !params) { json(res, 400, { error: { message: 'path and params required' } }); return; }
      const data = await metaWrite(apiPath, params);
      json(res, 200, data);
      return;
    }

    // ── AI Router (non-streaming) ──
    if (req.method === 'POST' && route === '/ai') {
      const body = JSON.parse(await readBody(req));
      const { task, ...payload } = body;
      if (!task) { json(res, 400, { error: { message: 'task type required' } }); return; }

      const routeConfig = getRoute(task);
      log('🧠', `AI [${routeConfig.provider}/${routeConfig.model}] ${task}${routeConfig.fallback ? ' (fallback)' : ''}`);

      let result;
      switch (routeConfig.provider) {
        case 'claude':
          result = await callClaude({ ...payload, model: routeConfig.model });
          break;
        case 'openai':
          if (routeConfig.model === 'gpt-image-1') {
            result = await callOpenAI({ ...payload, model: routeConfig.model });
          } else {
            // Convert Anthropic-style messages to OpenAI format
            const msgs = [];
            if (payload.system) msgs.push({ role: 'system', content: payload.system });
            for (const m of (payload.messages || [])) msgs.push(m);
            result = await callOpenAI({ ...payload, model: routeConfig.model, messages: msgs });
          }
          break;
        case 'gemini':
          result = await callGemini({ ...payload, model: routeConfig.model });
          break;
        default:
          throw new Error(`Unknown provider: ${routeConfig.provider}`);
      }

      json(res, 200, { provider: routeConfig.provider, model: routeConfig.model, fallback: !!routeConfig.fallback, result });
      return;
    }

    // ── AI Stream (Claude only — streaming) ──
    if (req.method === 'POST' && route === '/ai/stream') {
      const body = JSON.parse(await readBody(req));
      const { task, ...payload } = body;

      log('🧠', `AI Stream [claude] ${task || 'chat'}`);
      await callClaude({ ...payload, model: 'claude-sonnet-4-20250514' }, true, res);
      return;
    }

    // ── Meta CAPI Events ──
    if (req.method === 'POST' && route === '/meta/capi') {
      const { pixelId, events } = JSON.parse(await readBody(req));
      if (!pixelId || !events) { json(res, 400, { error: { message: 'pixelId and events required' } }); return; }
      if (!KEYS.meta) { json(res, 400, { error: { message: 'Meta access token not configured' } }); return; }
      log('📡', `CAPI: sending ${events.length} events to pixel ${pixelId}`);

      // Send CAPI via JSON body with access_token in query string (more reliable than form-encoded)
      const capiUrl = `https://graph.facebook.com/${META_API_VERSION}/${pixelId}/events?access_token=${encodeURIComponent(KEYS.meta)}`;
      const capiBody = JSON.stringify({ data: events });
      const parsed = new URL(capiUrl);

      try {
        const capiResult = await new Promise((resolve, reject) => {
          const capiOpts = {
            hostname: parsed.hostname,
            path: parsed.pathname + parsed.search,
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Content-Length': Buffer.byteLength(capiBody),
            },
            timeout: 30000,
          };

          const capiReq = https.request(capiOpts, capiRes => {
            const chunks = [];
            capiRes.on('data', c => chunks.push(c));
            capiRes.on('end', () => {
              const buf = Buffer.concat(chunks).toString('utf8');
              try {
                const result = JSON.parse(buf);
                if (result.error) {
                  log('❌', `CAPI error: code=${result.error.code} subcode=${result.error.error_subcode || '?'} type=${result.error.type || '?'} msg=${result.error.message}`);
                  reject(new Error(`Meta CAPI: ${result.error.message} (code: ${result.error.code})`));
                } else {
                  log('✅', `CAPI success: ${result.events_received || 0} events received, fbtrace=${result.fbtrace_id || '?'}`);
                  resolve(result);
                }
              } catch {
                log('❌', `CAPI unexpected response: ${buf.substring(0, 300)}`);
                reject(new Error(`Unexpected CAPI response: ${buf.substring(0, 200)}`));
              }
            });
          });

          capiReq.setTimeout(30000, () => { capiReq.destroy(); reject(new Error('CAPI request timeout')); });
          capiReq.on('error', e => { log('❌', `CAPI network error: ${e.message}`); reject(e); });
          capiReq.write(capiBody);
          capiReq.end();
        });

        json(res, 200, capiResult);
      } catch (capiErr) {
        json(res, 400, { error: { message: capiErr.message } });
      }
      return;
    }

    // ── Excel/CSV Parser (local, no AI needed) ──
    if (req.method === 'POST' && route === '/upload/parse-excel') {
      const { dataBase64, filename } = JSON.parse(await readBody(req));
      if (!dataBase64) { json(res, 400, { error: { message: 'dataBase64 required' } }); return; }
      if (!XLSX) { json(res, 500, { error: { message: 'xlsx module not installed. Run: npm install xlsx' } }); return; }

      log('📊', `Parsing ${filename} locally with xlsx`);
      const buf = Buffer.from(dataBase64, 'base64');
      const wb = XLSX.read(buf, { cellDates: false });
      const result = { sheets: {}, sheetNames: wb.SheetNames };

      wb.SheetNames.forEach(name => {
        const sheet = wb.Sheets[name];
        const raw = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });
        result.sheets[name] = raw;
      });

      json(res, 200, result);
      return;
    }

    // ── SHA-256 hash for CAPI user_data ──
    if (req.method === 'POST' && route === '/util/hash') {
      const { values } = JSON.parse(await readBody(req));
      if (!values || !Array.isArray(values)) { json(res, 400, { error: { message: 'values array required' } }); return; }
      const hashed = values.map(v => {
        if (!v || v === '(em branco)') return '';
        const normalized = String(v).trim().toLowerCase().replace(/[^a-z0-9@._+\-]/g, '');
        return crypto.createHash('sha256').update(normalized).digest('hex');
      });
      json(res, 200, { hashed });
      return;
    }

    // ═══════════════════════════════════════════════════════════
    // OFFLINE CONVERSION UPLOAD — Meta CAPI + Google Ads
    // ═══════════════════════════════════════════════════════════

    // (removeAccents, hashForMeta, splitName are defined globally at line ~689)

    // ── Send offline conversions to Meta CAPI ──
    if (req.method === 'POST' && route === '/conversions/send-meta') {
      const body = JSON.parse(await readBody(req));
      const { orders, pixelId } = body;
      if (!orders || !Array.isArray(orders) || !orders.length) {
        json(res, 400, { error: { message: 'orders array required' } }); return;
      }
      const pid = pixelId || KEYS.pixelId;
      if (!pid) { json(res, 400, { error: { message: 'pixelId not configured' } }); return; }
      if (!KEYS.meta) { json(res, 400, { error: { message: 'Meta access token not configured' } }); return; }

      // Meta CAPI only accepts events within the last 7 days
      const sevenDaysAgo = new Date();
      sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
      const cutoffStr = sevenDaysAgo.toISOString().substring(0, 10);
      const recentOrders = orders.filter(o => !o.date || o.date >= cutoffStr);
      const skippedOld = orders.length - recentOrders.length;
      if (skippedOld > 0) log('⚠️', `Skipping ${skippedOld} orders older than 7 days (Meta CAPI limit)`);
      if (!recentOrders.length) {
        log('⚠️', `All ${orders.length} orders are older than 7 days — cannot send to Meta CAPI`);
        json(res, 200, {
          success: true,
          totalSent: 0, totalReceived: 0,
          skippedOld,
          message: `Todos os ${orders.length} pedidos sao mais antigos que 7 dias. Meta CAPI so aceita eventos dos ultimos 7 dias. Novos pedidos serao enviados automaticamente.`
        });
        return;
      }

      log('📤', `Building ${recentOrders.length} Purchase events for Meta CAPI (${skippedOld} skipped - too old)`);

      // Build CAPI events from orders
      const events = [];
      for (const order of recentOrders) {
        const { fn, ln } = splitName(order.customer);
        const eventTime = order.date ? Math.floor(new Date(order.date + 'T12:00:00-03:00').getTime() / 1000) : Math.floor(Date.now() / 1000);

        const userData = {};
        // Hash all PII fields per Meta spec
        if (order.email && order.email !== '(em branco)') { const h = hashForMeta(order.email, 'email'); if (h) userData.em = [h]; }
        if (order.phone && order.phone !== '(em branco)') {
          let phone = String(order.phone).replace(/\D/g, '');
          if (phone.length === 10 || phone.length === 11) phone = '55' + phone;
          if (phone.length >= 12) { const h = hashForMeta(phone, 'phone'); if (h) userData.ph = [h]; }
        }
        if (fn) { const h = hashForMeta(fn, 'name'); if (h) userData.fn = [h]; }
        if (ln) { const h = hashForMeta(ln, 'name'); if (h) userData.ln = [h]; }
        if (order.city) { const h = hashForMeta(order.city, 'city'); if (h) userData.ct = [h]; }
        if (order.state) { const h = hashForMeta(order.state, 'state'); if (h) userData.st = [h]; }
        userData.country = [hashForMeta('br', 'country')];
        if (order.cpf && order.cpf !== '(em branco)') { const h = hashForMeta(order.cpf); if (h) userData.external_id = [h]; }

        // Add fbc/fbp if available (critical for EMQ 8+)
        if (order.fbc) userData.fbc = order.fbc;
        if (order.fbp) userData.fbp = order.fbp;

        events.push({
          event_name: 'Purchase',
          event_time: eventTime,
          action_source: order.fbc ? 'website' : 'business_messaging',
          user_data: userData,
          custom_data: {
            value: Number(order.revenue || 0),
            currency: 'BRL',
            content_name: order.product || '',
            order_id: order.orderId || order._key || '',
            content_type: 'product',
          },
        });
      }

      // Send in batches of 1000 (Meta limit)
      const batchSize = 1000;
      let totalReceived = 0;
      const errors = [];

      for (let i = 0; i < events.length; i += batchSize) {
        const batch = events.slice(i, i + batchSize);
        const capiUrl = `https://graph.facebook.com/${META_API_VERSION}/${pid}/events`;
        const capiBody = JSON.stringify({ data: batch, access_token: KEYS.meta });

        try {
          const result = await httpsRequest(capiUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: capiBody,
            timeout: 60000,
          });

          if (result.status >= 400 || result.data?.error) {
            const errMsg = result.data?.error?.message || JSON.stringify(result.data).substring(0, 500);
            const errDetail = result.data?.error?.error_user_msg || result.data?.error?.error_subcode || '';
            log('❌', `CAPI batch error: ${errMsg}${errDetail ? ' | Detail: ' + errDetail : ''} | Status: ${result.status}`);
            errors.push(errMsg);
          } else {
            totalReceived += result.data?.events_received || 0;
            log('✅', `CAPI batch: ${result.data?.events_received || 0} events received`);
          }
        } catch (e) {
          log('❌', `CAPI batch error: ${e.message}`);
          errors.push(e.message);
        }
      }

      log('📊', `Meta CAPI total: ${totalReceived}/${events.length} events sent (${skippedOld} skipped)`);
      json(res, 200, {
        success: true,
        totalSent: events.length,
        totalReceived,
        skippedOld,
        errors: errors.length ? errors : undefined,
      });
      return;
    }

    // ── Send offline conversions to Google Ads ──
    if (req.method === 'POST' && route === '/conversions/send-google') {
      const body = JSON.parse(await readBody(req));
      const { orders, conversionActionId } = body;
      if (!orders || !Array.isArray(orders) || !orders.length) {
        json(res, 400, { error: { message: 'orders array required' } }); return;
      }

      const token = await googleRefreshToken();
      const customerId = (KEYS.googleCustomerId || '').replace(/-/g, '');
      if (!customerId) throw new Error('Google Ads Customer ID not configured');

      // First, get or find the Purchase conversion action (must be UPLOAD_CLICKS type for offline upload)
      let convActionResourceName = '';
      if (conversionActionId) {
        convActionResourceName = `customers/${customerId}/conversionActions/${conversionActionId}`;
      } else {
        // Find existing IMPORT (UPLOAD_CLICKS) Purchase conversion action
        log('🔍', 'Looking for Import-type Purchase conversion action in Google Ads');
        const actions = await googleAdsQuery(`
          SELECT conversion_action.id, conversion_action.name, conversion_action.type, conversion_action.status, conversion_action.resource_name
          FROM conversion_action
          WHERE conversion_action.status = 'ENABLED'
        `);
        // Prefer UPLOAD_CLICKS type (Import) — required for offline conversion upload
        const importAction = actions.find(a => {
          const name = (a.conversionAction?.name || '').toLowerCase();
          const type = a.conversionAction?.type;
          return type === 'UPLOAD_CLICKS' && (name.includes('purchase') || name.includes('compra') || name.includes('venda') || name.includes('offline') || name.includes('import'));
        });
        if (importAction) {
          convActionResourceName = importAction.conversionAction.resourceName;
          log('✅', `Found Import conversion action: ${importAction.conversionAction.name} (${convActionResourceName})`);
        } else {
          // No Import-type action found — auto-create one
          log('⚠️', 'No Import-type Purchase conversion action found. Creating one automatically...');
          try {
            const createToken = await googleRefreshToken();
            const mutateUrl = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${customerId}/googleAds:mutate`;
            const mutateHeaders = {
              'Authorization': `Bearer ${createToken}`,
              'developer-token': KEYS.googleDevToken,
              'Content-Type': 'application/json',
            };
            if (KEYS.googleLoginCustomerId) {
              mutateHeaders['login-customer-id'] = KEYS.googleLoginCustomerId.replace(/-/g, '');
            }
            const createResult = await httpsRequest(mutateUrl, {
              method: 'POST',
              headers: mutateHeaders,
              body: JSON.stringify({
                mutateOperations: [{
                  conversionActionOperation: {
                    create: {
                      name: 'Compra Offline (Import)',
                      type: 'UPLOAD_CLICKS',
                      category: 'PURCHASE',
                      status: 'ENABLED',
                      valueSettings: {
                        defaultValue: 0,
                        alwaysUseDefaultValue: false,
                      },
                    }
                  }
                }]
              }),
              timeout: 30000,
            });
            if (createResult.status >= 400) {
              const errMsg = createResult.data?.error?.message || JSON.stringify(createResult.data).substring(0, 500);
              log('❌', `Failed to create Import conversion action: ${errMsg}`);
              json(res, 200, {
                success: false,
                error: `Nao foi possivel criar acao de conversao Import automaticamente: ${errMsg}`,
                hint: 'Crie manualmente uma acao de conversao do tipo "Import" no Google Ads.'
              });
              return;
            }
            // Extract the created resource name
            const createdResourceName = createResult.data?.mutateOperationResponses?.[0]?.conversionActionResult?.resourceName;
            if (createdResourceName) {
              convActionResourceName = createdResourceName;
              log('✅', `Created Import conversion action: ${convActionResourceName}`);
            } else {
              log('❌', 'Created conversion action but could not extract resource name');
              json(res, 200, { success: false, error: 'Acao criada mas resource name nao retornado.' });
              return;
            }
          } catch (createErr) {
            log('❌', `Error creating Import conversion action: ${createErr.message}`);
            json(res, 200, { success: false, error: createErr.message });
            return;
          }
        }
      }

      // Build enhanced conversion adjustments with user identifiers
      log('📤', `Building ${orders.length} offline conversions for Google Ads`);

      const conversions = [];
      for (const order of orders) {
        const { fn, ln } = splitName(order.customer);
        const convTime = order.date ? order.date + ' 12:00:00-03:00' : new Date().toISOString().replace('T', ' ').substring(0, 19) + '-03:00';

        const conv = {
          conversionAction: convActionResourceName,
          conversionDateTime: convTime,
          conversionValue: Number(order.revenue || 0),
          currencyCode: 'BRL',
          orderId: order.orderId || order._key || '',
        };

        // User identifiers for enhanced conversions (Google Ads spec)
        const userIdentifiers = [];
        if (order.email && order.email !== '(em branco)') {
          // Google: lowercase, trim. Gmail/Googlemail: remove dots from username, remove +suffix
          let email = order.email.trim().toLowerCase();
          const atIdx = email.indexOf('@');
          if (atIdx > 0) {
            const domain = email.substring(atIdx + 1);
            if (domain === 'gmail.com' || domain === 'googlemail.com') {
              let user = email.substring(0, atIdx).replace(/\./g, '');
              const plusIdx = user.indexOf('+');
              if (plusIdx > 0) user = user.substring(0, plusIdx);
              email = user + '@' + domain;
            }
          }
          if (email.includes('@')) {
            const h = crypto.createHash('sha256').update(email).digest('hex');
            userIdentifiers.push({ hashedEmail: h });
          }
        }
        if (order.phone && order.phone !== '(em branco)') {
          // Google: E.164 format WITH + sign, then hash the whole string including +
          let phone = String(order.phone).replace(/\D/g, '');
          if (phone.length === 10 || phone.length === 11) phone = '55' + phone;
          if (phone.length >= 12) {
            const e164 = '+' + phone;
            const h = crypto.createHash('sha256').update(e164).digest('hex');
            userIdentifiers.push({ hashedPhoneNumber: h });
          }
        }
        if (fn || ln) {
          const addr = {};
          // Google: lowercase, remove spaces, then hash
          if (fn) { const n = removeAccents(fn).toLowerCase().replace(/[^a-z]/g, ''); if (n) addr.hashedFirstName = crypto.createHash('sha256').update(n).digest('hex'); }
          if (ln) { const n = removeAccents(ln).toLowerCase().replace(/[^a-z]/g, ''); if (n) addr.hashedLastName = crypto.createHash('sha256').update(n).digest('hex'); }
          // Google: city, state, countryCode are PLAIN TEXT (NOT hashed)
          if (order.city) addr.city = order.city;
          if (order.state) addr.state = order.state;
          addr.countryCode = 'BR';
          userIdentifiers.push({ addressInfo: addr });
        }

        if (userIdentifiers.length) conv.userIdentifiers = userIdentifiers;
        conversions.push(conv);
      }

      // Upload via Google Ads API
      const url = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${customerId}:uploadClickConversions`;
      const headers = {
        'Authorization': `Bearer ${token}`,
        'developer-token': KEYS.googleDevToken,
        'Content-Type': 'application/json',
      };
      if (KEYS.googleLoginCustomerId) {
        headers['login-customer-id'] = KEYS.googleLoginCustomerId.replace(/-/g, '');
      }

      const uploadBody = JSON.stringify({
        conversions: conversions,
        partialFailure: true,
      });

      log('📡', `Uploading ${conversions.length} conversions to Google Ads`);
      const result = await httpsRequest(url, {
        method: 'POST',
        headers,
        body: uploadBody,
        timeout: 60000,
      });

      if (result.status >= 400) {
        const errMsg = result.data?.error?.message || JSON.stringify(result.data).substring(0, 500);
        log('❌', `Google conversion upload error: ${errMsg}`);
        json(res, 200, { success: false, error: errMsg });
      } else {
        const partialErrors = result.data?.partialFailureError;
        log('✅', `Google conversion upload: ${conversions.length} sent`);
        json(res, 200, {
          success: true,
          totalSent: conversions.length,
          results: result.data?.results || [],
          partialErrors: partialErrors || undefined,
        });
      }
      return;
    }

    // ── File Upload for AI processing (generic) ──
    if (req.method === 'POST' && route === '/upload/process') {
      const { filename, mimeType, dataBase64, textContent, task } = JSON.parse(await readBody(req));
      if (!dataBase64 && !textContent) { json(res, 400, { error: { message: 'dataBase64 or textContent required' } }); return; }
      const routeConfig = getRoute(task || 'extract.pdf');
      log('📄', `Processing ${filename || 'text'} via ${routeConfig.provider}`);

      // For text content (already extracted data), send as text only
      const messages = [{
        role: 'user',
        content: textContent
          ? textContent
          : `Extraia os dados deste documento (${filename}). Retorne JSON.`
      }];

      let result;
      switch (routeConfig.provider) {
        case 'gemini': result = await callGemini({ messages, system: 'Analyze data. Return valid JSON only.', model: routeConfig.model }); break;
        case 'claude': result = await callClaude({ messages, system: 'Analyze data. Return valid JSON only.', model: routeConfig.model, max_tokens: 4000 }); break;
        default: result = await callClaude({ messages, system: 'Analyze data.', model: 'claude-sonnet-4-20250514', max_tokens: 4000 }); break;
      }
      json(res, 200, { provider: routeConfig.provider, result });
      return;
    }

    // ── Data persistence ──
    if (req.method === 'POST' && route === '/data/save') {
      const { key, data } = JSON.parse(await readBody(req));
      if (!key) { json(res, 400, { error: { message: 'key required' } }); return; }
      saveData(key, data);
      json(res, 200, { ok: true });
      return;
    }

    if (req.method === 'GET' && route === '/data/load') {
      const key = url.searchParams.get('key');
      if (!key) { json(res, 400, { error: { message: 'key param required' } }); return; }
      const data = loadData(key);
      json(res, 200, { data });
      return;
    }

    // ═══════════════════════════════════════════════════════════
    // GOOGLE OAUTH FLOW
    // ═══════════════════════════════════════════════════════════

    // ── Google OAuth: Start authorization flow ──
    if (req.method === 'GET' && route === '/google/oauth/start') {
      if (!KEYS.googleClientId || !KEYS.googleClientSecret) {
        res.writeHead(400, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end('<html><body style="background:#0a0a0a;color:#fff;font-family:system-ui;display:flex;align-items:center;justify-content:center;height:100vh;margin:0"><div style="text-align:center"><h1 style="color:#ff4444">Erro</h1><p>Google Client ID e Client Secret nao configurados. Configure em Config primeiro.</p></div></body></html>');
        return;
      }
      const redirectUri = `http://localhost:${PORT}/google/oauth/callback`;
      const scopes = [
        'https://www.googleapis.com/auth/adwords',
        'https://www.googleapis.com/auth/analytics.readonly',
        'https://www.googleapis.com/auth/webmasters.readonly',
        'https://www.googleapis.com/auth/content',
      ].join(' ');
      const authUrl = `https://accounts.google.com/o/oauth2/v2/auth?client_id=${encodeURIComponent(KEYS.googleClientId)}&redirect_uri=${encodeURIComponent(redirectUri)}&response_type=code&scope=${encodeURIComponent(scopes)}&access_type=offline&prompt=consent`;
      log('🔑', 'Google OAuth: Redirecting to consent screen');
      res.writeHead(302, { Location: authUrl });
      res.end();
      return;
    }

    // ── Google OAuth: Callback — exchange code for tokens ──
    if (req.method === 'GET' && route === '/google/oauth/callback') {
      const urlObj = new URL(req.url, `http://localhost:${PORT}`);
      const code = urlObj.searchParams.get('code');
      const error = urlObj.searchParams.get('error');
      if (error || !code) {
        log('❌', `Google OAuth callback error: ${error || 'no code'}`);
        res.writeHead(400, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(`<html><body style="background:#0a0a0a;color:#fff;font-family:system-ui;display:flex;align-items:center;justify-content:center;height:100vh;margin:0"><div style="text-align:center"><h1 style="color:#ff4444">Erro na autorizacao</h1><p>${error || 'Nenhum codigo recebido'}</p></div></body></html>`);
        return;
      }
      const redirectUri = `http://localhost:${PORT}/google/oauth/callback`;
      const tokenBody = new URLSearchParams({
        grant_type: 'authorization_code',
        code,
        client_id: KEYS.googleClientId,
        client_secret: KEYS.googleClientSecret,
        redirect_uri: redirectUri,
      }).toString();
      const tokenResult = await httpsRequest('https://oauth2.googleapis.com/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: tokenBody,
        timeout: 15000,
      });
      if (tokenResult.data?.refresh_token) {
        KEYS.googleRefreshToken = tokenResult.data.refresh_token;
        GOOGLE_ACCESS_TOKEN = tokenResult.data.access_token || '';
        GOOGLE_TOKEN_EXPIRY = Date.now() + ((tokenResult.data.expires_in || 3600) - 60) * 1000;
        GOOGLE_REFRESH_TOKEN_UPDATED_AT = Date.now();
        // Persist
        const keyFields = [
          'anthropic', 'openai', 'gemini', 'meta', 'adAccountId', 'pixelId',
          'googleDevToken', 'googleClientId', 'googleClientSecret', 'googleRefreshToken',
          'googleCustomerId', 'googleLoginCustomerId', 'ga4PropertyId',
          'shopifyStore', 'shopifyToken', 'shopifyClientId', 'shopifyClientSecret',
        ];
        const persist = {};
        for (const field of keyFields) { persist[field] = KEYS[field] || ''; }
        saveData('keys', persist);
        cacheClear('google');
        cacheClear('ga4');
        log('✅', `Google OAuth: refresh token obtained and saved`);
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(`<html><body style="background:#0a0a0a;color:#fff;font-family:system-ui;display:flex;align-items:center;justify-content:center;height:100vh;margin:0">
          <div style="text-align:center">
            <h1 style="color:#34A853">&#10003; Google Autorizado!</h1>
            <p>Refresh token salvo com sucesso. Scopes: Google Ads + GA4.</p>
            <p style="color:#888;font-size:13px">Recarregando Toth Intelligence...</p>
            <script>
              if (window.opener) { try { window.opener.location.reload(); } catch(e) {} }
              setTimeout(()=>window.close(),2000);
            </script>
          </div></body></html>`);
        return;
      } else {
        const errMsg = tokenResult.data?.error_description || tokenResult.data?.error || 'Token exchange failed';
        log('❌', `Google OAuth error: ${errMsg}`);
        res.writeHead(400, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(`<html><body style="background:#0a0a0a;color:#fff;font-family:system-ui;display:flex;align-items:center;justify-content:center;height:100vh;margin:0">
          <div style="text-align:center">
            <h1 style="color:#ff4444">&#10007; Erro na autorizacao Google</h1>
            <p>${errMsg}</p>
          </div></body></html>`);
        return;
      }
    }

    // ═══════════════════════════════════════════════════════════
    // GOOGLE ADS ROUTES
    // ═══════════════════════════════════════════════════════════

    // ── Google Ads: Verify credentials ──
    if (req.method === 'POST' && route === '/google/verify') {
      const token = await googleRefreshToken();
      const customerId = (KEYS.googleCustomerId || '').replace(/-/g, '');
      // Simple query to verify access
      const results = await googleAdsQuery(`SELECT customer.descriptive_name, customer.id FROM customer LIMIT 1`);
      const name = results[0]?.customer?.descriptiveName || customerId;
      json(res, 200, { ok: true, customerName: name, customerId });
      return;
    }

    // ── Google Ads: Campaigns with all metrics ──
    if (req.method === 'POST' && route === '/google/campaigns') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:campaigns:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT campaign.id, campaign.name, campaign.status,
          campaign.advertising_channel_type, campaign.bidding_strategy_type,
          campaign_budget.amount_micros, campaign.optimization_score,
          metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc,
          metrics.cost_micros, metrics.conversions, metrics.conversions_value,
          metrics.cost_per_conversion, metrics.search_impression_share,
          metrics.search_rank_lost_impression_share,
          metrics.search_budget_lost_impression_share,
          metrics.all_conversions, metrics.all_conversions_value
        FROM campaign
        WHERE ${dateClause} AND campaign.status != 'REMOVED'
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Daily performance ──
    if (req.method === 'POST' && route === '/google/campaigns-daily') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:campaigns-daily:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT segments.date, campaign.name, campaign.id,
          metrics.impressions, metrics.clicks, metrics.cost_micros,
          metrics.conversions, metrics.conversions_value
        FROM campaign
        WHERE ${dateClause} AND campaign.status != 'REMOVED'
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Ad Groups ──
    if (req.method === 'POST' && route === '/google/adgroups') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:adgroups:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT ad_group.id, ad_group.name, ad_group.status, campaign.name, campaign.id,
          metrics.impressions, metrics.clicks, metrics.ctr, metrics.cost_micros,
          metrics.conversions, metrics.conversions_value, metrics.average_cpc
        FROM ad_group
        WHERE ${dateClause} AND ad_group.status != 'REMOVED'
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Individual Ads ──
    if (req.method === 'POST' && route === '/google/ads') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:ads:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type,
          ad_group_ad.status, ad_group_ad.ad.final_urls,
          campaign.name, campaign.id, ad_group.name, ad_group.id,
          metrics.impressions, metrics.clicks, metrics.ctr,
          metrics.cost_micros, metrics.conversions, metrics.conversions_value
        FROM ad_group_ad
        WHERE ${dateClause} AND ad_group_ad.status != 'REMOVED'
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Keywords with Quality Score ──
    if (req.method === 'POST' && route === '/google/keywords') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:keywords:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type,
          ad_group_criterion.status, ad_group_criterion.quality_info.quality_score,
          campaign.name, campaign.id, ad_group.name, ad_group.id,
          metrics.impressions, metrics.clicks, metrics.ctr,
          metrics.cost_micros, metrics.conversions, metrics.average_cpc
        FROM keyword_view
        WHERE ${dateClause}
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Search Terms ──
    if (req.method === 'POST' && route === '/google/search-terms') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:search-terms:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT search_term_view.search_term, search_term_view.status,
          campaign.name, campaign.id, ad_group.name, ad_group.id,
          metrics.impressions, metrics.clicks, metrics.ctr,
          metrics.cost_micros, metrics.conversions, metrics.conversions_value
        FROM search_term_view
        WHERE ${dateClause}
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: PMax / Shopping performance ──
    if (req.method === 'POST' && route === '/google/pmax') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:pmax:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      let products = [], placements = [];
      try {
        products = await googleAdsQuery(`
          SELECT segments.product_title, segments.product_item_id,
            segments.product_type_l1, segments.product_brand,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.conversions_value
          FROM shopping_performance_view
          WHERE ${dateClause}
        `);
      } catch (e) { log('⚠️', `PMax products: ${e.message}`); }

      try {
        placements = await googleAdsQuery(`
          SELECT group_placement_view.display_name, group_placement_view.placement_type,
            group_placement_view.target_url,
            metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions
          FROM group_placement_view
          WHERE ${dateClause}
        `);
      } catch (e) { log('⚠️', `PMax placements: ${e.message}`); }

      const data = { products, placements };
      cacheSet(cacheKey, data, CACHE_TTL.campaigns);
      json(res, 200, data);
      return;
    }

    // ── Google Ads: Conversion Actions (CRITICAL for audit) ──
    if (req.method === 'POST' && route === '/google/conversions') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:conversions:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT segments.conversion_action, segments.conversion_action_name,
          segments.conversion_action_category,
          metrics.conversions, metrics.all_conversions,
          metrics.conversions_value, metrics.all_conversions_value
        FROM customer
        WHERE ${dateClause}
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Conversions by campaign (breakdown WhatsApp vs Purchase) ──
    if (req.method === 'POST' && route === '/google/conversions-by-campaign') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:conv-by-campaign:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT campaign.id, campaign.name,
          segments.conversion_action_name, segments.conversion_action_category,
          metrics.conversions, metrics.conversions_value, metrics.all_conversions
        FROM campaign
        WHERE ${dateClause} AND campaign.status != 'REMOVED'
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Device performance ──
    if (req.method === 'POST' && route === '/google/devices') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:devices:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT campaign.name, campaign.id, segments.device,
          metrics.impressions, metrics.clicks, metrics.conversions, metrics.cost_micros
        FROM campaign
        WHERE ${dateClause} AND campaign.status != 'REMOVED'
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Auction Insights (competitor visibility) ──
    if (req.method === 'POST' && route === '/google/auction-insights') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:auction:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT campaign.name, campaign.id,
          metrics.auction_insight_search_impression_share,
          metrics.auction_insight_search_overlap_rate,
          metrics.auction_insight_search_outranking_share,
          metrics.auction_insight_search_top_impression_percentage,
          metrics.auction_insight_search_absolute_top_impression_percentage,
          metrics.search_impression_share,
          metrics.search_rank_lost_impression_share,
          metrics.search_budget_lost_impression_share
        FROM campaign
        WHERE ${dateClause} AND campaign.status = 'ENABLED'
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Hourly performance (day-of-week + hour patterns) ──
    if (req.method === 'POST' && route === '/google/hourly') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:hourly:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT segments.day_of_week, segments.hour,
          metrics.impressions, metrics.clicks, metrics.cost_micros,
          metrics.conversions, metrics.conversions_value
        FROM campaign
        WHERE ${dateClause} AND campaign.status != 'REMOVED'
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Shopping product performance ──
    if (req.method === 'POST' && route === '/google/shopping-products') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:shopping-products:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT segments.product_title, segments.product_item_id,
          segments.product_type_l1, segments.product_type_l2,
          metrics.impressions, metrics.clicks, metrics.cost_micros,
          metrics.conversions, metrics.conversions_value
        FROM shopping_performance_view
        WHERE ${dateClause}
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Geographic performance ──
    if (req.method === 'POST' && route === '/google/geo') {
      const body = JSON.parse(await readBody(req));
      const days = body?.days || 30;
      const cacheKey = `google:geo:${days}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const dateClause = gaqlDateClause(days);
      const results = await withRetry(() => googleAdsQuery(`
        SELECT campaign_criterion.location.geo_target_constant,
          campaign.name, campaign.id,
          metrics.impressions, metrics.clicks, metrics.cost_micros,
          metrics.conversions, metrics.conversions_value
        FROM location_view
        WHERE ${dateClause} AND campaign.status != 'REMOVED'
      `));
      cacheSet(cacheKey, results, CACHE_TTL.campaigns);
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Custom GAQL query ──
    if (req.method === 'POST' && route === '/google/query') {
      const body = JSON.parse(await readBody(req));
      const { gaql } = body;
      if (!gaql) { json(res, 400, { error: 'gaql query required' }); return; }
      // Validate GAQL: only allow SELECT queries (prevent mutations via this endpoint)
      if (!gaql.trim().toUpperCase().startsWith('SELECT')) { json(res, 400, { error: 'Only SELECT queries allowed' }); return; }
      log('🔍', `Custom GAQL: ${gaql.substring(0, 80)}...`);
      const results = await withRetry(() => googleAdsQuery(gaql));
      json(res, 200, results);
      return;
    }

    // ── Google Ads: Mutate (create campaigns, ad groups, ads) ──
    if (req.method === 'POST' && route === '/google/mutate') {
      const body = JSON.parse(await readBody(req));
      const { operations } = body;
      if (!operations || !Array.isArray(operations)) {
        json(res, 400, { error: 'operations array required' });
        return;
      }
      const token = await googleRefreshToken();
      const customerId = (KEYS.googleCustomerId || '').replace(/-/g, '');
      if (!customerId) throw new Error('Google Ads Customer ID not configured');
      const url = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${customerId}/googleAds:mutate`;
      const headers = {
        'Authorization': `Bearer ${token}`,
        'developer-token': KEYS.googleDevToken,
        'Content-Type': 'application/json',
      };
      if (KEYS.googleLoginCustomerId) {
        headers['login-customer-id'] = KEYS.googleLoginCustomerId.replace(/-/g, '');
      }
      log('✏️', `Google Ads Mutate: ${operations.length} operations`);
      const result = await httpsRequest(url, {
        method: 'POST',
        headers,
        body: JSON.stringify({ mutateOperations: operations }),
        timeout: 30000,
      });
      if (result.status >= 400) {
        const errMsg = result.data?.error?.message || JSON.stringify(result.data).substring(0, 500);
        log('❌', `Google Ads Mutate error: ${errMsg}`);
        throw new Error(`Google Ads Mutate: ${errMsg}`);
      }
      log('✅', `Google Ads Mutate success`);
      json(res, 200, result.data);
      return;
    }

    // ── Meta: Targeting search (interests, behaviors) ──
    if (req.method === 'POST' && route === '/meta/targeting-search') {
      const body = JSON.parse(await readBody(req));
      const q = body?.q || '';
      const type = body?.type || 'adinterest';
      if (!q) { json(res, 400, { error: 'q parameter required' }); return; }
      if (!KEYS.meta) throw new Error('Meta access token not configured');
      const url = `https://graph.facebook.com/${META_API_VERSION}/search?type=${type}&q=${encodeURIComponent(q)}&access_token=${KEYS.meta}`;
      log('🔍', `Meta targeting search: ${q}`);
      const result = await httpsRequest(url, { timeout: 15000 });
      if (result.status >= 400) {
        throw new Error(result.data?.error?.message || 'Targeting search failed');
      }
      json(res, 200, result.data?.data || []);
      return;
    }

    // ═══════════════════════════════════════════════════════════
    // GA4 ROUTES
    // ═══════════════════════════════════════════════════════════

    // ── GA4: Verify credentials ──
    if (req.method === 'POST' && route === '/ga4/verify') {
      const token = await googleRefreshToken();
      // Simple metadata check
      const url = `https://analyticsdata.googleapis.com/v1beta/properties/${KEYS.ga4PropertyId}/metadata`;
      const result = await httpsRequest(url, {
        headers: { 'Authorization': `Bearer ${token}` },
        timeout: 15000,
      });
      if (result.status >= 400) {
        const msg = result.data?.error?.message || 'Access denied';
        if (msg.includes('insufficient authentication scopes') || msg.includes('PERMISSION_DENIED')) {
          throw new Error(`GA4: Escopo insuficiente. Seu refresh token precisa incluir o scope "analytics.readonly" além de "adwords". Regere o token em: https://developers.google.com/oauthplayground com os scopes: https://www.googleapis.com/auth/adwords e https://www.googleapis.com/auth/analytics.readonly`);
        }
        throw new Error(`GA4: ${msg}`);
      }
      json(res, 200, { ok: true, propertyId: KEYS.ga4PropertyId });
      return;
    }

    // ── GA4: Generic report ──
    if (req.method === 'POST' && route === '/ga4/report') {
      const body = JSON.parse(await readBody(req));
      const { reportBody, cacheKey } = body;
      if (!reportBody) { json(res, 400, { error: { message: 'reportBody required' } }); return; }

      if (cacheKey) {
        const cached = cacheGet(`ga4:${cacheKey}`);
        if (cached) { json(res, 200, cached); return; }
      }

      const result = await withRetry(() => ga4Report(reportBody));
      if (cacheKey) cacheSet(`ga4:${cacheKey}`, result, CACHE_TTL.campaigns);
      json(res, 200, result);
      return;
    }

    // ═══════════════════════════════════════════════════════════
    // GOOGLE SEARCH CONSOLE ROUTES
    // ═══════════════════════════════════════════════════════════

    // ── Search Console: Verify ──
    if (req.method === 'POST' && route === '/gsc/verify') {
      const token = await googleRefreshToken();
      const result = await httpsRequest('https://www.googleapis.com/webmasters/v3/sites', {
        headers: { 'Authorization': `Bearer ${token}` },
        timeout: 15000,
      });
      if (result.status >= 400) {
        const msg = result.data?.error?.message || 'Access denied';
        if (msg.includes('insufficient') || msg.includes('PERMISSION_DENIED')) {
          throw new Error('Search Console: Escopo insuficiente. Re-autorize o Google OAuth com o scope webmasters.readonly.');
        }
        throw new Error(`Search Console: ${msg}`);
      }
      const sites = (result.data?.siteEntry || []).map(s => ({ url: s.siteUrl, permission: s.permissionLevel }));
      json(res, 200, { ok: true, sites });
      return;
    }

    // ── Search Console: Query performance ──
    if (req.method === 'POST' && route === '/gsc/query') {
      const token = await googleRefreshToken();
      const body = JSON.parse(await readBody(req));
      const { siteUrl, startDate, endDate, dimensions, rowLimit, dimensionFilterGroups } = body;
      if (!siteUrl) { json(res, 400, { error: { message: 'siteUrl required' } }); return; }

      const cacheKey = `gsc:${siteUrl}:${startDate}:${endDate}:${(dimensions||[]).join(',')}`;
      const cached = cacheGet(cacheKey);
      if (cached) { json(res, 200, cached); return; }

      const days = 30;
      const end = endDate || new Date().toISOString().substring(0, 10);
      const start = startDate || new Date(Date.now() - days * 86400000).toISOString().substring(0, 10);

      const reqBody = {
        startDate: start,
        endDate: end,
        dimensions: dimensions || ['query'],
        rowLimit: rowLimit || 1000,
      };
      if (dimensionFilterGroups) reqBody.dimensionFilterGroups = dimensionFilterGroups;

      const encodedUrl = encodeURIComponent(siteUrl);
      const apiUrl = `https://www.googleapis.com/webmasters/v3/sites/${encodedUrl}/searchAnalytics/query`;
      log('🔍', `GSC query: ${(dimensions||['query']).join(',')} for ${siteUrl}`);

      const result = await httpsRequest(apiUrl, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(reqBody),
        timeout: 30000,
      });

      if (result.status >= 400) {
        throw new Error(`Search Console: ${result.data?.error?.message || 'Query failed'}`);
      }

      log('✅', `GSC: ${(result.data?.rows || []).length} rows`);
      cacheSet(cacheKey, result.data, CACHE_TTL.campaigns);
      json(res, 200, result.data);
      return;
    }

    // ── Search Console: URL inspection ──
    if (req.method === 'POST' && route === '/gsc/inspect') {
      const token = await googleRefreshToken();
      const body = JSON.parse(await readBody(req));
      const { siteUrl, inspectionUrl } = body;
      if (!siteUrl || !inspectionUrl) { json(res, 400, { error: { message: 'siteUrl and inspectionUrl required' } }); return; }

      const result = await httpsRequest('https://searchconsole.googleapis.com/v1/urlInspection/index:inspect', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ inspectionUrl, siteUrl }),
        timeout: 30000,
      });
      if (result.status >= 400) {
        throw new Error(`URL Inspection: ${result.data?.error?.message || 'Inspection failed'}`);
      }
      json(res, 200, result.data);
      return;
    }

    // ═══════════════════════════════════════════════════════════
    // GOOGLE MERCHANT CENTER ROUTES
    // ═══════════════════════════════════════════════════════════

    // ── Merchant Center: Verify & list accounts ──
    if (req.method === 'POST' && route === '/merchant/verify') {
      const token = await googleRefreshToken();
      // Try to find merchant account linked to the Google Ads account
      const result = await httpsRequest('https://shoppingcontent.googleapis.com/content/v2.1/accounts/authinfo', {
        headers: { 'Authorization': `Bearer ${token}` },
        timeout: 15000,
      });
      if (result.status >= 400) {
        const msg = result.data?.error?.message || 'Access denied';
        if (msg.includes('insufficient') || msg.includes('PERMISSION_DENIED')) {
          throw new Error('Merchant Center: Escopo insuficiente. Re-autorize o Google OAuth com o scope content.');
        }
        throw new Error(`Merchant Center: ${msg}`);
      }
      json(res, 200, result.data);
      return;
    }

    // ── Merchant Center: List products with status ──
    if (req.method === 'POST' && route === '/merchant/products') {
      const token = await googleRefreshToken();
      const body = JSON.parse(await readBody(req));
      const { merchantId, maxResults, pageToken } = body;
      if (!merchantId) { json(res, 400, { error: { message: 'merchantId required' } }); return; }

      const cached = cacheGet(`merchant:products:${merchantId}`);
      if (cached && !pageToken) { json(res, 200, cached); return; }

      let url = `https://shoppingcontent.googleapis.com/content/v2.1/${merchantId}/productstatuses?maxResults=${maxResults || 250}`;
      if (pageToken) url += `&pageToken=${pageToken}`;

      log('📡', `Merchant products: ${merchantId}`);
      const result = await httpsRequest(url, {
        headers: { 'Authorization': `Bearer ${token}` },
        timeout: 30000,
      });
      if (result.status >= 400) {
        throw new Error(`Merchant Center: ${result.data?.error?.message || 'Products query failed'}`);
      }
      log('✅', `Merchant: ${(result.data?.resources || []).length} products`);
      if (!pageToken) cacheSet(`merchant:products:${merchantId}`, result.data, CACHE_TTL.shopify);
      json(res, 200, result.data);
      return;
    }

    // ── Merchant Center: Account-level issues ──
    if (req.method === 'POST' && route === '/merchant/issues') {
      const token = await googleRefreshToken();
      const body = JSON.parse(await readBody(req));
      const { merchantId } = body;
      if (!merchantId) { json(res, 400, { error: { message: 'merchantId required' } }); return; }

      const url = `https://shoppingcontent.googleapis.com/content/v2.1/${merchantId}/accountstatuses/${merchantId}`;
      log('📡', `Merchant issues: ${merchantId}`);
      const result = await httpsRequest(url, {
        headers: { 'Authorization': `Bearer ${token}` },
        timeout: 30000,
      });
      if (result.status >= 400) {
        throw new Error(`Merchant Center: ${result.data?.error?.message || 'Issues query failed'}`);
      }
      json(res, 200, result.data);
      return;
    }

    // ── Merchant Center: Product performance report (via Merchant Reports API) ──
    if (req.method === 'POST' && route === '/merchant/performance') {
      const token = await googleRefreshToken();
      const body = JSON.parse(await readBody(req));
      const { merchantId, query } = body;
      if (!merchantId) { json(res, 400, { error: { message: 'merchantId required' } }); return; }

      const cached = cacheGet(`merchant:perf:${merchantId}`);
      if (cached) { json(res, 200, cached); return; }

      const reportQuery = query || `SELECT product_view.id, product_view.title, product_view.price_micros, product_view.currency_code, product_view.availability, product_view.shipping_label, product_view.condition, product_view.channel FROM product_view WHERE product_view.channel = 'ONLINE' LIMIT 500`;

      const url = `https://shoppingcontent.googleapis.com/content/v2.1/${merchantId}/reports/search`;
      log('📡', `Merchant report: ${reportQuery.substring(0, 80)}...`);
      const result = await httpsRequest(url, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query: reportQuery }),
        timeout: 30000,
      });
      if (result.status >= 400) {
        throw new Error(`Merchant Reports: ${result.data?.error?.message || 'Report failed'}`);
      }
      log('✅', `Merchant report: ${(result.data?.results || []).length} rows`);
      cacheSet(`merchant:perf:${merchantId}`, result.data, CACHE_TTL.shopify);
      json(res, 200, result.data);
      return;
    }

    // ═══════════════════════════════════════════════════════════
    // SHOPIFY ROUTES
    // ═══════════════════════════════════════════════════════════

    // ── Shopify: Verify credentials ──
    // ── Shopify OAuth: Start authorization ──
    if (req.method === 'POST' && route === '/shopify/auth') {
      const store = KEYS.shopifyStore;
      const clientId = KEYS.shopifyClientId;
      if (!store) throw new Error('Shopify store name not configured');
      if (!clientId) throw new Error('Shopify Client ID not configured');
      const scopes = 'read_orders,read_products,read_customers';
      const redirectUri = `http://localhost:${PORT}/shopify/callback`;
      const authUrl = `https://${store}.myshopify.com/admin/oauth/authorize?client_id=${clientId}&scope=${scopes}&redirect_uri=${encodeURIComponent(redirectUri)}`;
      log('🔗', `Shopify OAuth: ${authUrl}`);
      json(res, 200, { ok: true, authUrl });
      return;
    }

    // ── Shopify OAuth: Callback — exchanges code for access token ──
    if (req.method === 'GET' && route === '/shopify/callback') {
      const code = url.searchParams.get('code');
      const store = KEYS.shopifyStore;
      if (!code || !store) {
        res.writeHead(400, { 'Content-Type': 'text/html' });
        res.end('<html><body><h2>Erro: código de autorização não recebido.</h2></body></html>');
        return;
      }
      log('🔑', `Shopify OAuth callback: exchanging code for token...`);
      const tokenResult = await httpsRequest(`https://${store}.myshopify.com/admin/oauth/access_token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          client_id: KEYS.shopifyClientId,
          client_secret: KEYS.shopifyClientSecret,
          code,
        }),
        timeout: 15000,
      });
      if (tokenResult.data?.access_token) {
        KEYS.shopifyToken = tokenResult.data.access_token;
        // Persist the token
        const persist = {};
        const keyFields = [
          'anthropic', 'openai', 'gemini', 'meta', 'adAccountId', 'pixelId',
          'googleDevToken', 'googleClientId', 'googleClientSecret', 'googleRefreshToken',
          'googleCustomerId', 'googleLoginCustomerId', 'ga4PropertyId',
          'shopifyStore', 'shopifyToken', 'shopifyClientId', 'shopifyClientSecret',
        ];
        for (const field of keyFields) { persist[field] = KEYS[field] || ''; }
        saveData('keys', persist);
        log('✅', `Shopify OAuth: access token obtained and saved`);
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(`<html><body style="background:#0a0a0a;color:#fff;font-family:system-ui;display:flex;align-items:center;justify-content:center;height:100vh;margin:0">
          <div style="text-align:center">
            <h1 style="color:#96bf48">✓ Shopify Autorizado!</h1>
            <p>Token salvo. Pode fechar esta aba e voltar ao Toth Intelligence.</p>
            <script>setTimeout(()=>window.close(),3000)</script>
          </div></body></html>`);
        return;
      } else {
        const errMsg = tokenResult.data?.error_description || tokenResult.data?.error || 'Token exchange failed';
        log('❌', `Shopify OAuth error: ${errMsg}`);
        res.writeHead(400, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(`<html><body style="background:#0a0a0a;color:#fff;font-family:system-ui;display:flex;align-items:center;justify-content:center;height:100vh;margin:0">
          <div style="text-align:center">
            <h1 style="color:#ff4444">✕ Erro na autorização Shopify</h1>
            <p>${errMsg}</p>
          </div></body></html>`);
        return;
      }
    }

    // ── Shopify: Verify connection ──
    if (req.method === 'POST' && route === '/shopify/verify') {
      const result = await shopifyGraphQL(`{ shop { name myshopifyDomain plan { displayName } } }`);
      json(res, 200, { ok: true, shop: result.shop });
      return;
    }

    // ── Shopify: Orders (last 90 days, with cursor pagination) ──
    if (req.method === 'POST' && route === '/shopify/orders') {
      const cached = cacheGet('shopify:orders');
      if (cached) { json(res, 200, cached); return; }

      const since = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      let allOrders = [];
      let hasNext = true;
      let cursor = null;

      while (hasNext) {
        const afterClause = cursor ? `, after: "${cursor}"` : '';
        const result = await shopifyGraphQL(`{
          orders(first: 250, sortKey: CREATED_AT, reverse: true,
            query: "created_at:>${since}"${afterClause}) {
            edges {
              node {
                id name createdAt
                displayFinancialStatus displayFulfillmentStatus
                totalPriceSet { shopMoney { amount currencyCode } }
                subtotalPriceSet { shopMoney { amount currencyCode } }
                currentTotalPriceSet { shopMoney { amount currencyCode } }
                totalDiscountsSet { shopMoney { amount currencyCode } }
                email phone tags sourceName
                lineItems(first: 50) {
                  edges { node { title quantity sku
                    originalTotalSet { shopMoney { amount currencyCode } }
                  }}
                }
                customer {
                  id firstName lastName
                  numberOfOrders
                  amountSpent { amount currencyCode }
                  defaultEmailAddress { emailAddress }
                }
                shippingAddress { city provinceCode country }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }`);

        const edges = result.orders?.edges || [];
        for (const edge of edges) allOrders.push(edge.node);
        hasNext = result.orders?.pageInfo?.hasNextPage || false;
        cursor = result.orders?.pageInfo?.endCursor || null;
        if (allOrders.length > 1000) break; // Safety limit
      }

      cacheSet('shopify:orders', allOrders, CACHE_TTL.shopify);
      json(res, 200, allOrders);
      return;
    }

    // ── Shopify: Products (best-selling) ──
    if (req.method === 'POST' && route === '/shopify/products') {
      const cached = cacheGet('shopify:products');
      if (cached) { json(res, 200, cached); return; }

      const result = await shopifyGraphQL(`{
        products(first: 100, sortKey: TITLE) {
          edges {
            node {
              id title handle status totalInventory
              priceRangeV2 {
                minVariantPrice { amount currencyCode }
                maxVariantPrice { amount currencyCode }
              }
              variants(first: 10) {
                edges { node { id title sku price inventoryQuantity } }
              }
            }
          }
        }
      }`);

      const products = (result.products?.edges || []).map(e => e.node);
      cacheSet('shopify:products', products, CACHE_TTL.shopify);
      json(res, 200, products);
      return;
    }

    // ── Shopify: Top customers by spend ──
    if (req.method === 'POST' && route === '/shopify/customers') {
      const cached = cacheGet('shopify:customers');
      if (cached) { json(res, 200, cached); return; }

      const result = await shopifyGraphQL(`{
        customers(first: 100, sortKey: UPDATED_AT, reverse: true) {
          edges {
            node {
              id firstName lastName
              numberOfOrders
              amountSpent { amount currencyCode }
              createdAt tags
              defaultEmailAddress { emailAddress }
            }
          }
        }
      }`);

      const customers = (result.customers?.edges || []).map(e => e.node);
      cacheSet('shopify:customers', customers, CACHE_TTL.shopify);
      json(res, 200, customers);
      return;
    }

    // ── Shopify: Abandoned Checkouts ──
    if (req.method === 'POST' && route === '/shopify/abandoned') {
      const cached = cacheGet('shopify:abandoned');
      if (cached) { json(res, 200, cached); return; }

      const result = await shopifyGraphQL(`{
        abandonedCheckouts(first: 100, sortKey: CREATED_AT, reverse: true) {
          edges {
            node {
              id createdAt
              totalPriceSet { shopMoney { amount currencyCode } }
              lineItems(first: 20) {
                edges { node { title quantity
                  variant { price }
                }}
              }
              customer { id firstName lastName
                defaultEmailAddress { emailAddress }
              }
              shippingAddress { city provinceCode }
            }
          }
        }
      }`);

      const checkouts = (result.abandonedCheckouts?.edges || []).map(e => e.node);
      log('🛒', `Shopify abandoned: ${checkouts.length} checkouts`);
      cacheSet('shopify:abandoned', checkouts, CACHE_TTL.shopify);
      json(res, 200, checkouts);
      return;
    }

    // ── Cache management ──
    if (req.method === 'POST' && route === '/cache/clear') {
      const { prefix } = JSON.parse(await readBody(req));
      cacheClear(prefix || '');
      json(res, 200, { ok: true });
      return;
    }

    // ═══════════════════════════════════════════════════════════
    // SNAPSHOT SYSTEM — Period-to-period analysis
    // ═══════════════════════════════════════════════════════════

    if (req.method === 'POST' && route === '/data/snapshot/save') {
      const body = JSON.parse(await readBody(req));
      const snapDir = path.join(DATA_DIR, 'snapshots');
      if (!fs.existsSync(snapDir)) fs.mkdirSync(snapDir, { recursive: true });
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const label = body.label || timestamp;
      const snapshot = {
        id: timestamp,
        label,
        date: new Date().toISOString(),
        window: body.window || 30,
        data: body.data || {},
      };
      const filePath = path.join(snapDir, `snap-${timestamp}.json`);
      fs.writeFileSync(filePath, JSON.stringify(snapshot, null, 2));
      json(res, 200, { ok: true, id: timestamp, path: filePath });
      return;
    }

    if (req.method === 'POST' && route === '/data/snapshots/list') {
      const snapDir = path.join(DATA_DIR, 'snapshots');
      if (!fs.existsSync(snapDir)) { json(res, 200, []); return; }
      const files = fs.readdirSync(snapDir).filter(f => f.startsWith('snap-') && f.endsWith('.json')).sort().reverse();
      const snapshots = files.map(f => {
        try {
          const raw = JSON.parse(fs.readFileSync(path.join(snapDir, f), 'utf8'));
          return { id: raw.id, label: raw.label, date: raw.date, window: raw.window, file: f };
        } catch { return null; }
      }).filter(Boolean);
      json(res, 200, snapshots);
      return;
    }

    if (req.method === 'POST' && route === '/data/snapshot/load') {
      const { id } = JSON.parse(await readBody(req));
      const snapDir = path.join(DATA_DIR, 'snapshots');
      const files = fs.existsSync(snapDir) ? fs.readdirSync(snapDir) : [];
      const file = files.find(f => f.includes(id));
      if (!file) { json(res, 404, { error: { message: 'Snapshot not found' } }); return; }
      const data = JSON.parse(fs.readFileSync(path.join(snapDir, file), 'utf8'));
      json(res, 200, data);
      return;
    }

    if (req.method === 'POST' && route === '/data/snapshot/delete') {
      const { id } = JSON.parse(await readBody(req));
      const snapDir = path.join(DATA_DIR, 'snapshots');
      const files = fs.existsSync(snapDir) ? fs.readdirSync(snapDir) : [];
      const file = files.find(f => f.includes(id));
      if (file) fs.unlinkSync(path.join(snapDir, file));
      json(res, 200, { ok: true });
      return;
    }

    // ── Goals system ──
    if (req.method === 'POST' && route === '/data/goals/save') {
      const body = JSON.parse(await readBody(req));
      fs.writeFileSync(path.join(DATA_DIR, 'goals.json'), JSON.stringify(body, null, 2));
      json(res, 200, { ok: true });
      return;
    }

    if (req.method === 'POST' && route === '/data/goals/load') {
      const filePath = path.join(DATA_DIR, 'goals.json');
      if (!fs.existsSync(filePath)) { json(res, 200, {}); return; }
      const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      json(res, 200, data);
      return;
    }

    // ── Adjustment log ──
    if (req.method === 'POST' && route === '/data/adjustments/save') {
      const body = JSON.parse(await readBody(req));
      const filePath = path.join(DATA_DIR, 'adjustments.json');
      let existing = [];
      if (fs.existsSync(filePath)) { try { existing = JSON.parse(fs.readFileSync(filePath, 'utf8')); } catch {} }
      existing.push({ ...body, timestamp: new Date().toISOString() });
      fs.writeFileSync(filePath, JSON.stringify(existing, null, 2));
      json(res, 200, { ok: true, total: existing.length });
      return;
    }

    if (req.method === 'POST' && route === '/data/adjustments/list') {
      const filePath = path.join(DATA_DIR, 'adjustments.json');
      if (!fs.existsSync(filePath)) { json(res, 200, []); return; }
      const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      json(res, 200, data);
      return;
    }

    // ═══════════════════════════════════════════════════════════
    // INTELLIGENCE BRAIN ROUTES
    // ═══════════════════════════════════════════════════════════

    if (req.method === 'POST' && route === '/intelligence/analyze') {
      const analysis = await runBrainAnalysis();
      json(res, 200, analysis);
      return;
    }

    if (req.method === 'POST' && route === '/intelligence/execute') {
      const { action } = JSON.parse(await readBody(req));
      if (!action) { json(res, 400, { error: { message: 'action required' } }); return; }
      const result = await executeBrainAction(action);
      json(res, 200, result);
      return;
    }

    if (req.method === 'GET' && route === '/intelligence/history') {
      try {
        const history = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'intelligence-history.json'), 'utf8'));
        json(res, 200, { history: history.slice(-10) });
      } catch { json(res, 200, { history: [] }); }
      return;
    }

    if (req.method === 'GET' && route === '/intelligence/data') {
      const data = await collectAllIntelligenceData();
      json(res, 200, data);
      return;
    }

    // ── Autopilot alerts ──
    if (req.method === 'GET' && route === '/autopilot/alerts') {
      const alerts = loadData('autopilot-alerts') || [];
      json(res, 200, { alerts: alerts.slice(-10) });
      return;
    }

    if (req.method === 'POST' && route === '/autopilot/check') {
      await runAutopilotCheck();
      const alerts = loadData('autopilot-alerts') || [];
      json(res, 200, { lastCheck: alerts[alerts.length - 1] || null });
      return;
    }

    // ═══════════════════════════════════════════════════════════
    // NEXUS INTEGRATION — WhatsApp AI Platform
    // Separate SaaS product that connects via API Key.
    // Intelligence polls Nexus for conversions → sends to CAPI.
    // ═══════════════════════════════════════════════════════════

    // ── Nexus: Connect ──
    if (req.method === 'POST' && route === '/integrations/nexus/connect') {
      const { url: nexusUrl, apiKey } = JSON.parse(await readBody(req));
      if (!nexusUrl || !apiKey) { json(res, 400, { error: { message: 'url and apiKey required' } }); return; }

      const cleanUrl = nexusUrl.replace(/\/+$/, '');
      log('🔗', `Testing Nexus connection: ${cleanUrl}`);

      try {
        // Test connection — supports both http and https
        const testUrl = cleanUrl + '/api/intelligence/nexus/conversions/pending?platform=meta';
        const testParsed = new URL(testUrl);
        const testModule = testParsed.protocol === 'https:' ? https : http;
        const test = await new Promise((resolve, reject) => {
          const req = testModule.request({ hostname: testParsed.hostname, port: testParsed.port || (testParsed.protocol === 'https:' ? 443 : 80), path: testParsed.pathname + testParsed.search, method: 'GET', headers: { 'x-nexus-api-key': apiKey }, timeout: 15000 }, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => { try { resolve({ status: res.statusCode, data: JSON.parse(Buffer.concat(chunks).toString()) }); } catch { resolve({ status: res.statusCode, data: {} }); } });
          });
          req.on('error', reject);
          req.setTimeout(15000, () => { req.destroy(); reject(new Error('Timeout ao conectar ao Nexus')); });
          req.end();
        });
        if (test.status === 401) throw new Error('API Key inválida. Verifique a chave no Nexus.');
        if (test.status >= 400) throw new Error(`Nexus retornou erro ${test.status}: ${test.data?.error || 'Unknown'}`);

        KEYS.nexusUrl = cleanUrl;
        KEYS.nexusApiKey = apiKey;
        const persist = {};
        const keyFields = Object.keys(KEYS);
        for (const f of keyFields) persist[f] = KEYS[f] || '';
        saveData('keys', persist);

        _startNexusAutoSync();
        log('✅', `Nexus connected: ${cleanUrl}`);
        json(res, 200, { ok: true, message: 'Nexus conectado com sucesso', url: cleanUrl });
      } catch (e) {
        log('❌', `Nexus connection failed: ${e.message}`);
        json(res, 400, { error: { message: `Falha ao conectar: ${e.message}` } });
      }
      return;
    }

    // ── Nexus: Disconnect ──
    if (req.method === 'POST' && route === '/integrations/nexus/disconnect') {
      KEYS.nexusUrl = '';
      KEYS.nexusApiKey = '';
      const persist = {};
      const keyFields = Object.keys(KEYS);
      for (const f of keyFields) persist[f] = KEYS[f] || '';
      saveData('keys', persist);
      if (_nexusSyncInterval) { clearInterval(_nexusSyncInterval); _nexusSyncInterval = null; }
      log('🔌', 'Nexus disconnected');
      json(res, 200, { ok: true, message: 'Nexus desconectado' });
      return;
    }

    // ── Nexus: Status ──
    if (req.method === 'GET' && route === '/integrations/nexus/status') {
      const history = loadData('nexus-sync-history') || [];
      const lastSync = history.length ? history[history.length - 1] : null;
      json(res, 200, {
        connected: !!(KEYS.nexusUrl && KEYS.nexusApiKey),
        url: KEYS.nexusUrl || null,
        autoSyncActive: !!_nexusSyncInterval,
        lastSync,
        totalSyncs: history.length,
      });
      return;
    }

    // ── Nexus: Sync (manual or called by scheduler) ──
    if (req.method === 'POST' && route === '/integrations/nexus/sync') {
      if (!KEYS.nexusUrl || !KEYS.nexusApiKey) {
        json(res, 400, { error: { message: 'Nexus não conectado. Configure em Conexões.' } }); return;
      }
      const result = await _nexusSyncConversions();
      json(res, 200, result);
      return;
    }

    // ── Nexus: Sync History ──
    if (req.method === 'GET' && route === '/integrations/nexus/sync-history') {
      const history = loadData('nexus-sync-history') || [];
      json(res, 200, { history: history.slice(-20) });
      return;
    }

    // ── 404 ──
    json(res, 404, { error: { message: `Route not found: ${route}` } });

  } catch (e) {
    log('❌', `${route}: ${e.message}`);
    json(res, 500, { error: { message: e.message } });
  }

}).listen(PORT, 'localhost', () => {
  const g = v => v ? '\x1b[32m✓\x1b[0m' : '\x1b[31m✕\x1b[0m';
  console.log('');
  console.log('  ┌─────────────────────────────────────────────────┐');
  console.log('  │                                                 │');
  console.log('  │   ⬡  Toth Intelligence v4 — Cross-Channel      │');
  console.log('  │                                                 │');
  console.log(`  │   🚀  http://localhost:${PORT}                      │`);
  console.log('  │   🔒  100% local — zero cloud storage          │');
  console.log('  │                                                 │');
  console.log('  │   Integrations:                                 │');
  console.log(`  │     ${g(KEYS.meta)} Meta Ads     ${g(KEYS.googleDevToken)} Google Ads          │`);
  console.log(`  │     ${g(KEYS.ga4PropertyId)} GA4          ${g(KEYS.shopifyStore && KEYS.shopifyToken)} Shopify             │`);
  console.log(`  │     ${g(KEYS.googleRefreshToken)} Search Console  ${g(KEYS.googleRefreshToken)} Merchant Center  │`);
  console.log('  │   AI:                                           │');
  console.log(`  │     ${g(KEYS.anthropic)} Claude       ${g(KEYS.openai)} OpenAI              │`);
  console.log(`  │     ${g(KEYS.gemini)} Gemini                                   │`);
  console.log('  │   Connected Platforms:                           │');
  console.log(`  │     ${g(KEYS.nexusUrl && KEYS.nexusApiKey)} Nexus WhatsApp                           │`);
  console.log('  │                                                 │');
  console.log('  │   ⛔  Ctrl+C to stop                           │');
  console.log('  │                                                 │');
  console.log('  └─────────────────────────────────────────────────┘');
  console.log('');

  // Start Nexus auto-sync if connected
  if (KEYS.nexusUrl && KEYS.nexusApiKey) {
    _startNexusAutoSync();
  }

  // Start Brain auto-analysis if configured
  if (_brainConfig && _brainConfig.enabled) {
    _startBrainAutoAnalysis();
  }

  // Start Autopilot (campaign protection)
  if (KEYS.meta) {
    _startAutopilot();
  }
});
