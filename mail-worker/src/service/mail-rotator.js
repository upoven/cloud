// mail-rotator.js — 四家免费邮件服务商轮换发信（Brevo / SendPulse / Mailjet / Mailtrap）
//
// ★★ 为什么需要这个模块（2026-11-03 接入）★★
//
//   cloud-mail 原本只有两条站外发信通道：
//     ① `send_email` 绑定（c.env.email）—— 只发给**账号内已验证**的地址，不能发任意站外
//     ② Resend（每域名一个 token）—— 要付费 / 免费额度很小
//   本站部署时 wrangler.toml 里的 `send_email` 绑定名是 `cloud`（不是 `email`），
//   所以 `c.env.email` 是 undefined，`useCloudflareEmail=false` → 全部走 Resend。
//   而 Resend 没配 token → **站外一封都发不出去**。
//
//   用户已有四家免费额度（≈1050 封/天），原本是 Python 脚本（mail-rotator/rotator.py）。
//   Worker 里跑不了 Python，所以把调度逻辑移植成 JS，直接接在发信链路上。
//
// ★ 与 Python 版保持一致的语义：
//   · 按剩余额度最多优先，同额按 priority
//   · 一家失败立刻换下一家（failover）
//   · 每日额度持久化（这里用 KV，键 rot:sent:<provider>:<YYYY-MM-DD>）
//   · warm-up 冷启动限速
//
// ★ 与 Python 版的**必要差异**：
//   · KV 是最终一致的（写入后立刻读可能读不到），所以计数采用「先读后写 + 允许少量超发」，
//     不用它做严格配额。真正超发时服务商会返回 4xx，failover 会兜住。
//   · 不做 SQLite（Worker 无文件系统）。
import dayjs from 'dayjs';

const PROVIDERS = ['brevo', 'sendpulse', 'mailjet', 'mailtrap'];
const TIMEOUT_MS = 20000;

// SendPulse 的 OAuth token 有效期 1h。Worker 的 isolate 会被复用，
// 用模块级变量缓存能省掉大部分 token 请求；失效后自动重取。
let spTokenCache = { token: null, exp: 0 };

function envKey(env, name) {
  const v = env?.[name];
  return typeof v === 'string' && v.trim() ? v.trim() : null;
}

/** 今天已发多少（按 provider）。KV 读失败按 0 处理，不让统计问题阻断发信。 */
async function sentToday(env, provider) {
  try {
    const k = `rot:sent:${provider}:${dayjs().format('YYYY-MM-DD')}`;
    const v = await env.kv.get(k);
    return v ? Number(v) || 0 : 0;
  } catch { return 0; }
}

async function incrSent(env, provider, n = 1) {
  try {
    const k = `rot:sent:${provider}:${dayjs().format('YYYY-MM-DD')}`;
    const cur = Number(await env.kv.get(k)) || 0;
    // TTL 2 天：跨天后自然过期，不需要清理任务
    await env.kv.put(k, String(cur + n), { expirationTtl: 60 * 60 * 48 });
  } catch { /* 计数失败不影响发信 */ }
}

/** warm-up：从 start_date 起每 ramp_interval_days 天 +ramp_step，封顶 max_daily_cap */
export function warmupCap(env) {
  const enabled = String(env.rot_warmup_enabled || 'true') !== 'false';
  if (!enabled) return Infinity;
  const startStr = env.rot_warmup_start || '';
  if (!startStr) return Infinity;
  const start = dayjs(startStr);
  if (!start.isValid()) return Infinity;
  const days = dayjs().startOf('day').diff(start.startOf('day'), 'day');
  if (days < 0) return 0;
  const stepDays = Math.max(1, Number(env.rot_warmup_step_days) || 2);
  const steps = Math.floor(days / stepDays);
  const cap = (Number(env.rot_warmup_start_cap) || 30) + steps * (Number(env.rot_warmup_step) || 15);
  return Math.min(cap, Number(env.rot_warmup_max) || Infinity);
}

/** provider 配置（凭据全部从 env 读，不写进仓库） */
function providerCfg(env) {
  return {
    brevo:     { enabled: !!envKey(env, 'BREVO_API_KEY'),      quota: Number(env.rot_brevo_quota) || 300,    priority: 1 },
    sendpulse: { enabled: !!(envKey(env, 'SENDPULSE_CLIENT_ID') && envKey(env, 'SENDPULSE_CLIENT_SECRET')),
                 quota: Number(env.rot_sendpulse_quota) || 400, priority: 2 },
    mailjet:   { enabled: !!(envKey(env, 'MAILJET_API_KEY') && envKey(env, 'MAILJET_SECRET_KEY')),
                 quota: Number(env.rot_mailjet_quota) || 200,   priority: 3 },
    mailtrap:  { enabled: !!envKey(env, 'MAILTRAP_API_TOKEN'),  quota: Number(env.rot_mailtrap_quota) || 150,  priority: 4 },
  };
}

/** 选出候选（剩余额度 desc, priority asc），并受 warm-up 全局上限约束 */
export async function pickProviders(env) {
  const cfg = providerCfg(env);
  const cap = warmupCap(env);
  const counts = {};
  for (const p of PROVIDERS) counts[p] = await sentToday(env, p);
  const totalSent = Object.values(counts).reduce((a, b) => a + b, 0);
  const globalRoom = cap - totalSent;
  if (globalRoom <= 0) return [];
  const out = [];
  for (const p of PROVIDERS) {
    const c = cfg[p];
    if (!c.enabled) continue;
    const remain = c.quota - counts[p];
    if (remain <= 0) continue;
    out.push({ name: p, cfg: c, remain });
  }
  out.sort((a, b) => (b.remain - a.remain) || (a.cfg.priority - b.cfg.priority));
  return out.slice(0, Math.max(1, Math.min(out.length, globalRoom)));
}

// ---------------------------------------------------------------
// 四家适配器：统一签名 (env, from, to, subject, html, text) -> { id }
// 失败抛异常 → 由调用方 failover
// ---------------------------------------------------------------

async function httpJson(url, opts) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), TIMEOUT_MS);
  try {
    const r = await fetch(url, { ...opts, signal: ctl.signal });
    const text = await r.text();
    let body = null;
    try { body = JSON.parse(text); } catch { body = text.slice(0, 300); }
    return { ok: r.ok, status: r.status, body };
  } finally { clearTimeout(timer); }
}

async function sendBrevo(env, from, to, subject, html, text) {
  const r = await httpJson('https://api.brevo.com/v3/smtp/email', {
    method: 'POST',
    headers: { 'api-key': envKey(env, 'BREVO_API_KEY'), 'Content-Type': 'application/json' },
    body: JSON.stringify({
      sender: { name: from.name, email: from.email },
      to: to.map((e) => ({ email: e })),
      subject,
      htmlContent: html,
      ...(text ? { textContent: text } : {}),
    }),
  });
  if (!r.ok) throw new Error(`Brevo ${r.status}: ${JSON.stringify(r.body).slice(0, 160)}`);
  return { id: r.body?.messageId || null };
}

async function sendpulseToken(env) {
  const now = Date.now();
  if (spTokenCache.token && spTokenCache.exp > now + 60000) return spTokenCache.token;
  const r = await httpJson('https://api.sendpulse.com/oauth/access_token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      grant_type: 'client_credentials',
      client_id: envKey(env, 'SENDPULSE_CLIENT_ID'),
      client_secret: envKey(env, 'SENDPULSE_CLIENT_SECRET'),
    }),
  });
  if (!r.ok || !r.body?.access_token) throw new Error(`SendPulse OAuth ${r.status}`);
  spTokenCache = { token: r.body.access_token, exp: now + (Number(r.body.expires_in) || 3600) * 1000 };
  return spTokenCache.token;
}

async function sendSendpulse(env, from, to, subject, html, text) {
  const token = await sendpulseToken(env);
  const r = await httpJson('https://api.sendpulse.com/smtp/emails', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: {
        html, subject,
        ...(text ? { text } : {}),
        from: { name: from.name, email: from.email },
        to: to.map((e) => ({ email: e })),
      },
    }),
  });
  if (!r.ok) throw new Error(`SendPulse ${r.status}: ${JSON.stringify(r.body).slice(0, 160)}`);
  // 成功返回 {"result": true}
  if (r.body?.result !== true) throw new Error(`SendPulse 拒绝: ${JSON.stringify(r.body).slice(0, 160)}`);
  return { id: null };
}

async function sendMailjet(env, from, to, subject, html, text) {
  const basic = btoa(envKey(env, 'MAILJET_API_KEY') + ':' + envKey(env, 'MAILJET_SECRET_KEY'));
  const r = await httpJson('https://api.mailjet.com/v3.1/send', {
    method: 'POST',
    headers: { Authorization: 'Basic ' + basic, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      Messages: [{
        From: { Email: from.email, Name: from.name },
        To: to.map((e) => ({ Email: e })),
        Subject: subject,
        HTMLPart: html,
        ...(text ? { TextPart: text } : {}),
      }],
    }),
  });
  if (!r.ok) throw new Error(`Mailjet ${r.status}: ${JSON.stringify(r.body).slice(0, 160)}`);
  const st = r.body?.Messages?.[0]?.Status;
  if (st !== 'success' && st !== 'ok') throw new Error(`Mailjet 拒绝: ${JSON.stringify(r.body).slice(0, 160)}`);
  return { id: r.body?.Messages?.[0]?.To?.[0]?.MessageID || null };
}

async function sendMailtrap(env, from, to, subject, html, text) {
  // ★ 端点必须是 send.api.mailtrap.io/api/send（不是 /api/v2/send）
  const r = await httpJson('https://send.api.mailtrap.io/api/send', {
    method: 'POST',
    headers: { 'Api-Token': envKey(env, 'MAILTRAP_API_TOKEN'), 'Content-Type': 'application/json' },
    body: JSON.stringify({
      from: { email: from.email, name: from.name },
      to: to.map((e) => ({ email: e })),
      subject,
      html,
      ...(text ? { text } : {}),
      category: 'transactional',
    }),
  });
  if (!r.ok) throw new Error(`Mailtrap ${r.status}: ${JSON.stringify(r.body).slice(0, 160)}`);
  if (r.body?.success === false) throw new Error(`Mailtrap 拒绝: ${JSON.stringify(r.body).slice(0, 160)}`);
  return { id: null };
}

const ADAPTERS = {
  brevo: sendBrevo,
  sendpulse: sendSendpulse,
  mailjet: sendMailjet,
  mailtrap: sendMailtrap,
};

/**
 * 对外统一入口：发一封站外邮件，自动选服务商 + 失败降级。
 * 返回 { provider, id }；全部失败抛 Error（消息里带每家的失败原因）。
 */
export async function sendViaRotator(env, { from, to, subject, html, text }) {
  const candidates = await pickProviders(env);
  if (!candidates.length) {
    throw new Error('今日无可用发信额度（四家额度已满或 warm-up 上限已到）');
  }
  const errors = [];
  for (const c of candidates) {
    try {
      const out = await ADAPTERS[c.name](env, from, to, subject, html, text);
      await incrSent(env, c.name, to.length);
      return { provider: c.name, id: out?.id || null };
    } catch (e) {
      errors.push(c.name + ': ' + e.message);
    }
  }
  throw new Error('全部服务商发送失败 → ' + errors.join(' | '));
}

/** 是否有任何一家可用（给设置页/健康检查用，不实际发信） */
export function rotatorConfigured(env) {
  const cfg = providerCfg(env);
  return PROVIDERS.some((p) => cfg[p].enabled);
}

/** 今日额度快照（给后台显示用） */
export async function rotatorStatus(env) {
  const cfg = providerCfg(env);
  const out = [];
  for (const p of PROVIDERS) {
    const sent = await sentToday(env, p);
    out.push({
      provider: p, enabled: cfg[p].enabled, quota: cfg[p].quota,
      sent, remain: Math.max(0, cfg[p].quota - sent),
    });
  }
  return { cap: warmupCap(env), providers: out };
}
