/**
 * NexusBot — Node.js API Server
 * Replaces Laravel completely.
 *
 * Changes in this version:
 *   + GET /api/market/analysis/:symbol  — real signal data from bot's signals table
 *   + is_paper_trading always true in /api/bot/config (testnet only)
 *   + Exchange test URL points to testnet endpoint
 */

import express from 'express';
import cors from 'cors';
import crypto from 'crypto';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app  = express();
const PORT = process.env.PORT || 3001;

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ── In-process balance cache (30s TTL) ───────────────────────────────────
// Prevents hammering testnet.binancefuture.com on every Dashboard load.
// The balance endpoint is non-critical — stale by 30s is acceptable.
const balanceCache = new Map(); // key = user_id → { data, expiresAt }
const BALANCE_TTL_MS = 30_000;

function getCachedBalance(userId) {
  const entry = balanceCache.get(userId);
  if (entry && entry.expiresAt > Date.now()) return entry.data;
  return null;
}
function setCachedBalance(userId, data) {
  balanceCache.set(userId, { data, expiresAt: Date.now() + BALANCE_TTL_MS });
}

// ── Suppress repeated fetch-timeout console noise ────────────────────────
let _lastFetchErrMsg = '';
let _fetchErrCount   = 0;
function logFetchError(endpoint, err) {
  const msg = `[${endpoint}] ${err?.cause?.code || err?.message || err}`;
  if (msg === _lastFetchErrMsg) {
    _fetchErrCount++;
    // Only log every 10th repeat to avoid log spam
    if (_fetchErrCount % 10 !== 0) return;
    console.warn(`[EXCHANGE] ${endpoint}: ${err?.cause?.code || 'fetch failed'} (×${_fetchErrCount}, suppressed repeats)`);
  } else {
    _lastFetchErrMsg = msg;
    _fetchErrCount   = 1;
    console.warn(`[EXCHANGE] ${endpoint}: ${err?.cause?.code || 'fetch failed'} — is testnet.binancefuture.com reachable?`);
  }
}

app.use(cors({ origin: process.env.FRONTEND_URL || '*' }));
app.use(express.json({ limit: '1mb' }));

// ── Auth middleware ────────────────────────────────────────────────────────

async function requireBotToken(req, res, next) {
  const auth  = req.headers['authorization'] || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : req.body?.bot_token;
  if (!token) return res.status(401).json({ error: 'Missing bot token' });
  const { data: bot, error } = await supabase.from('bots').select('*').eq('bot_token', token).maybeSingle();
  if (error || !bot) return res.status(401).json({ error: 'Invalid bot token' });
  req.bot = bot;
  next();
}

async function requireUser(req, res, next) {
  const auth = req.headers['authorization'] || '';
  const jwt  = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  if (!jwt) return res.status(401).json({ error: 'Not authenticated' });
  const { data: { user }, error } = await supabase.auth.getUser(jwt);
  if (error || !user) return res.status(401).json({ error: 'Invalid session' });
  req.user = user;
  next();
}

async function requireAdmin(req, res, next) {
  const { data } = await supabase.from('admin_roles').select('role').eq('user_id', req.user.id).maybeSingle();
  if (!data) return res.status(403).json({ error: 'Admin access required' });
  req.adminRole = data.role;
  next();
}

// ══════════════════════════════════════════════════════════════════════════════
// PYTHON BOT API
// ══════════════════════════════════════════════════════════════════════════════

app.post('/api/bot/heartbeat', requireBotToken, async (req, res) => {
  const commands = [];
  if (!req.bot.is_running) commands.push({ command: 'stop', close_open_trades: true });
  res.json({ success: true, commands });
});

app.post('/api/bot/trade/open', requireBotToken, async (req, res) => {
  const bot = req.bot;
  const { symbol, side, entry_price, quantity, leverage, tp_price, sl_price,
          confidence, signal_type, regime, order_id, opened_at } = req.body;

  const { data: trade, error } = await supabase.from('trades').insert({
    bot_id: bot.id, user_id: bot.user_id, exchange_name: 'binance',
    trading_pair: symbol, trade_type: side === 'long' ? 'long' : 'short',
    entry_price, quantity, leverage: leverage || 5, tp_price, sl_price,
    confidence, signal_type, regime, order_id, status: 'open',
    is_paper: (bot.trading_mode === 'paper'),  // correctly flags paper trades
    opened_at: opened_at || new Date().toISOString(),
  }).select('id').single();

  if (error) { console.error('[trade/open]', error); return res.status(500).json({ success: false, error: error.message }); }
  await supabase.rpc('decrement_remaining_trades', { p_user_id: bot.user_id });
  res.json({ success: true, trade_id: trade.id });
});

app.post('/api/bot/trade/close', requireBotToken, async (req, res) => {
  const bot = req.bot;
  const { trade_id, exit_price, pnl_usdt, pnl_r, exit_reason, bars_held, fee_usdt, net_pnl, closed_at } = req.body;
  if (!trade_id) return res.status(400).json({ success: false, error: 'trade_id required' });

  const { error: tradeErr } = await supabase.from('trades').update({
    exit_price, profit_loss: pnl_usdt, fee_usdt, net_pnl, pnl_r,
    exit_reason, bars_held, status: 'closed',
    closed_at: closed_at || new Date().toISOString(),
  }).eq('id', trade_id).eq('bot_id', bot.id);

  if (tradeErr) return res.status(500).json({ success: false, error: tradeErr.message });
  // Use net_pnl (after fees) for stats — gross PnL is misleading since
  // a trade can be gross-positive but net-negative after fees.
  const effectivePnl = (net_pnl !== undefined && net_pnl !== null) ? net_pnl : (pnl_usdt || 0);
  const isWin = effectivePnl > 0;
  await supabase.rpc('update_bot_stats_on_close', { p_bot_id: bot.id, p_pnl: effectivePnl, p_is_win: isWin });
  res.json({ success: true });
});

app.post('/api/bot/signal', requireBotToken, async (req, res) => {
  const { symbol, signal, confidence, signal_type, regime, adx, atr_ratio,
          ema_long, ema_short, action_taken, price_at_signal, signaled_at } = req.body;
  await supabase.from('signals').insert({
    bot_id: req.bot.id, symbol, signal, confidence, signal_type, regime,
    adx, atr_ratio, ema_long, ema_short, action_taken, price_at_signal,
    signaled_at: signaled_at || new Date().toISOString(),
  });
  res.json({ success: true });
});

app.post('/api/bot/log/batch', requireBotToken, async (req, res) => {
  const entries = req.body?.entries;
  if (!Array.isArray(entries) || entries.length === 0) return res.json({ success: true });
  await supabase.from('bot_logs').insert(entries.map(e => ({
    bot_id: req.bot.id, level: e.level || 'info', channel: e.channel || 'bot',
    message: (e.message || '').slice(0, 2000), context: e.context || null,
    logged_at: e.logged_at || new Date().toISOString(),
  })));
  res.json({ success: true });
});

app.post('/api/bot/status', requireBotToken, async (req, res) => {
  const { status, message } = req.body;
  // 'running' = true; stopped/error/anything else = false.
  // This fires on every bot lifecycle event including graceful exit and crashes.
  await supabase.from('bots')
    .update({ is_running: (status === 'running'), updated_at: new Date().toISOString() })
    .eq('id', req.bot.id);
  await supabase.from('bot_logs').insert({
    bot_id: req.bot.id, level: status === 'error' ? 'error' : 'info',
    channel: 'bot', message: `[STATUS] ${status}${message ? ': ' + message : ''}`,
    logged_at: new Date().toISOString(),
  });
  res.json({ success: true });
});

app.get('/api/bot/config/:bot_id', requireBotToken, async (req, res) => {
  const bot = req.bot;
  if (bot.id !== req.params.bot_id) return res.status(403).json({ error: 'Token mismatch' });

  let apiKey = '', apiSecret = '';
  if (bot.exchange_id) {
    const { data: ex } = await supabase.from('exchange_connections').select('api_key, api_secret').eq('id', bot.exchange_id).single();
    apiKey = ex?.api_key || ''; apiSecret = ex?.api_secret || '';
  }

  res.json({
    bot_id: bot.id, bot_token: bot.bot_token, symbol: bot.trading_pair, timeframe: bot.timeframe,
    leverage: bot.leverage || 5, min_confidence: bot.min_confidence || 68,
    stop_loss_percent: bot.stop_loss_percent, take_profit_percent: bot.take_profit_percent,
    max_trades_per_day: bot.max_trades_per_day, trade_amount: bot.trade_amount,
    trade_amount_type: bot.trade_amount_type, daily_max_loss: bot.daily_max_loss,
    max_open_trades: bot.max_open_trades,
    is_paper_trading: true,   // always testnet in current setup
    exchange: { name: 'binance', api_key: apiKey, api_secret: apiSecret, testnet: true },
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// MARKET ANALYSIS — reads live signal data written by the Python bot
// ══════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/market/analysis/:symbol
 *
 * Returns the most recent signal the Python bot wrote for this symbol.
 * The bot posts to /api/bot/signal on every evaluation cycle (every ~60s).
 *
 * 404 → bot hasn't signalled this symbol yet (frontend shows "waiting for bot")
 * 200 → response shape used by AITradingAssistant component
 */
app.get('/api/market/analysis/:symbol', requireUser, async (req, res) => {
  // Accept both BTCUSDT and BTC/USDT
  const symbol = req.params.symbol.toUpperCase().replace('/', '');

  // First try: bots owned by this user trading that symbol
  const { data: userBots } = await supabase
    .from('bots').select('id, stop_loss_percent, take_profit_percent')
    .eq('user_id', req.user.id)
    .eq('trading_pair', symbol);

  let latestSignal = null;
  let matchedBot   = null;

  if (userBots && userBots.length > 0) {
    const botIds = userBots.map(b => b.id);
    const { data: sig } = await supabase
      .from('signals').select('*')
      .in('bot_id', botIds).eq('symbol', symbol)
      .order('signaled_at', { ascending: false }).limit(1).maybeSingle();
    if (sig) { latestSignal = sig; matchedBot = userBots.find(b => b.id === sig.bot_id) || userBots[0]; }
  }

  // Fallback: any signal for this symbol from any bot (platform-wide)
  if (!latestSignal) {
    const { data: sig } = await supabase
      .from('signals').select('*').eq('symbol', symbol)
      .order('signaled_at', { ascending: false }).limit(1).maybeSingle();
    latestSignal = sig;
  }

  if (!latestSignal) {
    return res.status(404).json({
      error: 'No signal data yet for ' + symbol + '. Start a bot trading this pair first.',
    });
  }

  const conf       = Number(latestSignal.confidence) || 0;
  const atrRatio   = Number(latestSignal.atr_ratio)  || 1;
  const emaL       = Math.round((Number(latestSignal.ema_long)  || 0) * 100);
  const emaS       = Math.round((Number(latestSignal.ema_short) || 0) * 100);
  const sig        = latestSignal.signal;

  res.json({
    // Raw fields
    symbol,
    signal:          sig,
    confidence:      conf,
    confidence_pct:  Math.round(conf * 100),
    regime:          latestSignal.regime || 'UNKNOWN',
    adx:             Number(latestSignal.adx) || null,
    atr_ratio:       atrRatio,
    ema_long:        emaL,
    ema_short:       emaS,
    price_at_signal: Number(latestSignal.price_at_signal) || null,
    signaled_at:     latestSignal.signaled_at,
    action_taken:    latestSignal.action_taken,
    // UI-friendly aliases
    sentiment:       sig === 'long' ? 'bullish' : sig === 'short' ? 'bearish' : 'neutral',
    sentiment_score: Math.round(conf * 100),
    risk_level:      conf >= 0.70 ? 'low' : conf >= 0.55 ? 'medium' : 'high',
    trend:           sig === 'long' ? 'upward' : sig === 'short' ? 'downward' : 'sideways',
    volatility_score:  Math.min(100, Math.round((atrRatio - 0.5) * 50)),
    volume_strength:   Math.min(100, Math.max(0, Math.round(Math.max(emaL, emaS) * 1.5))),
    // Config-driven suggestions (from user's bot, or sane defaults)
    suggested_stop_loss:   Number(matchedBot?.stop_loss_percent)   || 2.0,
    suggested_take_profit: Number(matchedBot?.take_profit_percent) || 3.0,
  });
});

// ══════════════════════════════════════════════════════════════════════════════
// BOT CONTROL
// ══════════════════════════════════════════════════════════════════════════════

app.post('/api/bots/:id/start', requireUser, async (req, res) => {
  const { id: botId } = req.params;
  const { data: bot, error } = await supabase.from('bots').select('*').eq('id', botId).eq('user_id', req.user.id).single();
  if (error || !bot) return res.status(404).json({ error: 'Bot not found' });
  if (bot.is_running) return res.json({ success: true, message: 'Already running' });

  const { data: sub } = await supabase.from('subscriptions')
    .select('remaining_trades, is_active, expires_at')
    .eq('user_id', req.user.id)
    .eq('is_active', true)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (!sub) return res.status(402).json({ error: 'No active subscription. Please subscribe to start bots.' });
  if (new Date(sub.expires_at) < new Date()) return res.status(402).json({ error: 'Subscription expired.' });
  if (sub.remaining_trades <= 0) return res.status(402).json({ error: 'No remaining trades in your plan.' });

  const botToken = crypto.randomBytes(32).toString('hex');
  await supabase.from('bots').update({ bot_token: botToken, is_running: true, updated_at: new Date().toISOString() }).eq('id', botId);

  const configDir  = process.env.BOT_CONFIG_DIR || path.join(__dirname, '../AITradingBot/bot_configs');
  fs.mkdirSync(configDir, { recursive: true });
  const configPath = path.join(configDir, `bot_${botId}.json`);
  // Fetch exchange API keys if a connection is linked
  let apiKey = '', apiSecret = '';
  if (bot.exchange_id) {
    const { data: ex } = await supabase
      .from('exchange_connections')
      .select('api_key, api_secret')
      .eq('id', bot.exchange_id)
      .single();
    apiKey    = (ex?.api_key    || '').trim();
    apiSecret = (ex?.api_secret || '').trim();
  }

  // Config keys must match EXACTLY what run_bot_managed.py reads:
  //   config["api_key"]                  → exchange key
  //   config["api_secret"]               → exchange secret
  //   config["is_testnet"]               → always true (testnet mode)
  //   config["risk_per_trade"]           → % of balance per trade
  //   config["daily_loss_limit"]         → USDT hard stop per day
  //   config["base_confidence_threshold"]→ ML min confidence (0-100 scale)
  //   config["laravel_api_url"]          → reporter API base (bot appends /heartbeat etc.)
  fs.writeFileSync(configPath, JSON.stringify({
    // Identity
    bot_id:     botId,
    bot_token:  botToken,
    // Reporter URL — bot reads config["laravel_api_url"] to build endpoint URLs
    laravel_api_url: `${process.env.API_BASE_URL || 'http://127.0.0.1:3001'}/api/bot`,
    // Trading params
    symbol:     bot.trading_pair,
    timeframe:  bot.timeframe,
    leverage:   bot.leverage    || 5,
    // Exchange credentials
    api_key:      apiKey,
    api_secret:   apiSecret,
    is_testnet:   false,           // only real keys accepted
    trading_mode: bot.trading_mode || 'paper',
    paper_balance: bot.paper_balance || 10000,
    // Risk settings (map from bot DB columns to config keys bot expects)
    // risk_per_trade_pct: Python bot divides by 100 internally.
    // Send as a true percentage: $500 trade on $10000 balance = 5.0%
    // So: risk_amount = $10000 × (5.0/100) = $500 → position = $500 × leverage ✅
    // BUG WAS: sending 0.01 → bot computed $1000 × (0.01/100) = $0.10 → $100 notional
    risk_per_trade:            (bot.trade_amount_type === 'percent')
                                 ? Number(bot.trade_amount)   // already a %
                                 : (Number(bot.trade_amount) / Number(bot.paper_balance || 10000)) * 100,
    daily_loss_limit:          Number(bot.daily_max_loss)     || 50,
    take_profit_pct:           Number(bot.take_profit_percent)|| 3.0,
    stop_loss_pct:             Number(bot.stop_loss_percent)  || 2.0,
    base_confidence_threshold: Number(bot.min_confidence)     || 68,
    max_open_trades:           Number(bot.max_open_trades)    || 1,
  }, null, 2));

  const pythonExe = process.env.PYTHON_EXE  || 'python';
  const botScript = process.env.BOT_SCRIPT   ||
    path.join(__dirname, '../AITradingBot/scripts/run_bot_managed.py');

  const child = spawn(pythonExe, [botScript, '--config', configPath], {
    detached:    true,
    stdio:       'ignore',
    cwd:         path.dirname(botScript),
    // windowsHide suppresses the CMD console window on Windows.
    // Python.exe is a console-subsystem app so spawn() shows a window by default.
    // This flag sets STARTF_USESHOWWINDOW + SW_HIDE in the Win32 STARTUPINFO struct.
    // Silently ignored on macOS/Linux.
    windowsHide: true,
  });
  child.unref();  // fully decouple — bot keeps running if Node restarts

  console.log(`[BOT] Started ${botId} (PID ${child.pid})`);
  res.json({ success: true, pid: child.pid, message: `Bot started (PID ${child.pid})` });
});

app.post('/api/bots/:id/stop', requireUser, async (req, res) => {
  const { error } = await supabase.from('bots')
    .update({ is_running: false, updated_at: new Date().toISOString() })
    .eq('id', req.params.id).eq('user_id', req.user.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, message: 'Stop command sent. Bot will halt within 30s.' });
});

// ══════════════════════════════════════════════════════════════════════════════
// SUBSCRIPTIONS
// ══════════════════════════════════════════════════════════════════════════════

const PLAN_CONFIG = {
  starter: { trades: 15,  bots: 3,  days: 45,  price: 29 },
  pro:     { trades: 50,  bots: 10, days: 90,  price: 79 },
  elite:   { trades: 200, bots: 25, days: 180, price: 149 },
};

app.post('/api/subscriptions/purchase', requireUser, async (req, res) => {
  const { plan_type } = req.body;
  const plan = PLAN_CONFIG[plan_type];
  if (!plan) return res.status(400).json({ error: 'Invalid plan type' });
  const expiresAt = new Date(); expiresAt.setDate(expiresAt.getDate() + plan.days);
  await supabase.from('subscriptions').update({ is_active: false }).eq('user_id', req.user.id).eq('is_active', true);
  const { data: sub, error } = await supabase.from('subscriptions').insert({
    user_id: req.user.id, plan_type, total_trades: plan.trades, remaining_trades: plan.trades,
    total_bot_creations: plan.bots, remaining_bot_creations: plan.bots,
    expires_at: expiresAt.toISOString(), is_active: true,
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  await supabase.from('revenue_records').insert({ user_id: req.user.id, amount: plan.price, plan_type, description: `${plan_type} plan purchase` });
  res.json({ success: true, subscription: sub });
});

app.post('/api/subscriptions/cancel', requireUser, async (req, res) => {
  const { error } = await supabase.from('subscriptions').update({ is_active: false }).eq('user_id', req.user.id).eq('is_active', true);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ══════════════════════════════════════════════════════════════════════════════
// EXCHANGE
// ══════════════════════════════════════════════════════════════════════════════

app.post('/api/exchange/test', requireUser, async (req, res) => {
  const { exchange_connection_id } = req.body;
  const { data: conn } = await supabase.from('exchange_connections').select('*')
    .eq('id', exchange_connection_id).eq('user_id', req.user.id).single();
  if (!conn) return res.status(404).json({ error: 'Connection not found' });
  try {
    // Trim keys defensively — guards against whitespace from copy-paste or DB storage
    const apiKey    = (conn.api_key    || '').trim();
    const apiSecret = (conn.api_secret || '').trim();
    if (!apiKey || !apiSecret) {
      return res.json({ connected: false, message: 'API key or secret is empty — please re-enter your credentials.' });
    }

    const ts    = Date.now();
    // recvWindow=10000 gives a 10s tolerance for clock skew between server and Binance
    const query = `timestamp=${ts}&recvWindow=10000`;
    const sig   = crypto.createHmac('sha256', apiSecret).update(query).digest('hex');
    const url   = `https://fapi.binance.com/fapi/v2/balance?${query}&signature=${sig}`;
    const response = await fetch(url, {
      headers: { 'X-MBX-APIKEY': apiKey },
      signal: AbortSignal.timeout(10_000),
    });
    const data = await response.json();

    // Binance returns -2015 for IP restriction and -2014 for bad key format
    const connected = response.ok && Array.isArray(data);
    let message = connected ? 'Connected to Binance Futures' : (data?.msg || 'Connection failed');

    // Translate opaque Binance errors into actionable messages
    const serverIp = await getServerPublicIp();
    if (!connected) {
      if (data?.code === -2015 || data?.code === -2014) {
        message = serverIp
          ? `IP not whitelisted. In Binance API Management → Edit Restrictions → add this IP to the whitelist: ${serverIp}`
          : "IP restriction error. Open Binance API Management → Edit Restrictions and add this server's public IP to the whitelist.";
      } else if (data?.code === -1021) {
        message = 'Timestamp error — server clock may be out of sync. Try again.';
      } else if (data?.code === -2011) {
        message = 'Enable Futures trading permission on this API key in Binance settings.';
      }
    }

    await supabase.from('exchange_connections')
      .update({ is_connected: connected, last_tested_at: new Date().toISOString() })
      .eq('id', conn.id);

    res.json({ connected, message, binance_code: data?.code || null, server_ip: serverIp });
  } catch (e) {
    const msg = e?.cause?.code === 'ECONNREFUSED'
      ? 'Cannot reach Binance — check your internet connection.'
      : (e.message || 'Connection error');
    res.json({ connected: false, message: msg });
  }
});

// ══════════════════════════════════════════════════════════════════════════════
// ADMIN
// ══════════════════════════════════════════════════════════════════════════════

app.get('/api/admin/users', requireUser, requireAdmin, async (req, res) => {
  const { data: profiles } = await supabase.from('user_profiles')
    .select('user_id, full_name, created_at, kyc_status, is_banned').order('created_at', { ascending: false });
  const { data: subs } = await supabase.from('subscriptions').select('user_id, plan_type').eq('is_active', true);
  const subMap = new Map((subs || []).map(s => [s.user_id, s.plan_type]));
  const { data: { users: authUsers } } = await supabase.auth.admin.listUsers();
  const emailMap = new Map((authUsers || []).map(u => [u.id, u.email]));
  const users = (profiles || []).map(p => ({
    id: p.user_id, email: emailMap.get(p.user_id) || '',
    full_name: p.full_name, created_at: p.created_at, kyc_status: p.kyc_status,
    is_banned: p.is_banned, subscription_tier: subMap.get(p.user_id) || 'free',
  }));
  res.json({ users });
});

app.post('/api/admin/kyc/approve', requireUser, requireAdmin, async (req, res) => {
  const { user_id, notes } = req.body;
  await supabase.from('user_profiles').update({ kyc_status: 'approved', kyc_completed_at: new Date().toISOString(), updated_at: new Date().toISOString() }).eq('user_id', user_id);
  await supabase.from('kyc_logs').insert({ user_id, event_type: 'approved', status: 'approved', notes: notes || null });
  await supabase.from('audit_logs').insert({ admin_id: req.user.id, user_id, action: 'kyc_approved', resource_type: 'user_profile' });
  res.json({ success: true });
});

app.post('/api/admin/kyc/reject', requireUser, requireAdmin, async (req, res) => {
  const { user_id, notes } = req.body;
  await supabase.from('user_profiles').update({ kyc_status: 'rejected', updated_at: new Date().toISOString() }).eq('user_id', user_id);
  await supabase.from('kyc_logs').insert({ user_id, event_type: 'rejected', status: 'rejected', notes: notes || null });
  await supabase.from('audit_logs').insert({ admin_id: req.user.id, user_id, action: 'kyc_rejected', resource_type: 'user_profile' });
  res.json({ success: true });
});

app.post('/api/admin/users/ban', requireUser, requireAdmin, async (req, res) => {
  const { user_id, reason, expires_at } = req.body;
  await supabase.from('user_profiles').update({ is_banned: true, ban_reason: reason }).eq('user_id', user_id);
  await supabase.from('user_bans').insert({ user_id, reason, banned_by: req.user.id, expires_at: expires_at || null });
  await supabase.from('audit_logs').insert({ admin_id: req.user.id, user_id, action: 'user_banned', resource_type: 'user', changes: { reason } });
  res.json({ success: true });
});

app.post('/api/admin/users/unban', requireUser, requireAdmin, async (req, res) => {
  const { user_id } = req.body;
  await supabase.from('user_profiles').update({ is_banned: false, ban_reason: null }).eq('user_id', user_id);
  await supabase.from('audit_logs').insert({ admin_id: req.user.id, user_id, action: 'user_unbanned', resource_type: 'user' });
  res.json({ success: true });
});

app.get('/api/admin/metrics', requireUser, requireAdmin, async (req, res) => {
  const [{ count: totalUsers }, { data: subs }, { count: totalBots }, { count: activeBots }, { count: totalTrades }, { data: revenue }] = await Promise.all([
    supabase.from('user_profiles').select('*', { count: 'exact', head: true }),
    supabase.from('subscriptions').select('plan_type').eq('is_active', true),
    supabase.from('bots').select('*', { count: 'exact', head: true }),
    supabase.from('bots').select('*', { count: 'exact', head: true }).eq('is_running', true),
    supabase.from('trades').select('*', { count: 'exact', head: true }),
    supabase.from('revenue_records').select('amount'),
  ]);
  const planPrices = { starter: 29, pro: 79, elite: 149 };
  res.json({
    totalUsers: totalUsers || 0, activeSubscriptions: subs?.length || 0,
    totalRevenue: (revenue || []).reduce((s, r) => s + Number(r.amount), 0),
    mrr: (subs || []).reduce((s, sub) => s + (planPrices[sub.plan_type] || 0), 0),
    totalBots: totalBots || 0, activeBots: activeBots || 0, totalTrades: totalTrades || 0,
  });
});

app.post('/api/admin/subscriptions/grant', requireUser, requireAdmin, async (req, res) => {
  const { user_id, plan_type } = req.body;
  const plan = PLAN_CONFIG[plan_type];
  if (!plan) return res.status(400).json({ error: 'Invalid plan' });
  const expiresAt = new Date(); expiresAt.setDate(expiresAt.getDate() + plan.days);
  await supabase.from('subscriptions').update({ is_active: false }).eq('user_id', user_id).eq('is_active', true);
  const { data: sub, error } = await supabase.from('subscriptions').insert({
    user_id, plan_type, total_trades: plan.trades, remaining_trades: plan.trades,
    total_bot_creations: plan.bots, remaining_bot_creations: plan.bots,
    expires_at: expiresAt.toISOString(), is_active: true,
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  await supabase.from('audit_logs').insert({ admin_id: req.user.id, user_id, action: 'subscription_granted', resource_type: 'subscription', changes: { plan_type } });
  res.json({ success: true, subscription: sub });
});


// ══════════════════════════════════════════════════════════════════════════════
// OPEN TRADE UNREALISED PNL
// Reads the most recent [WATCHING] log for each open trade to extract live PnL.
// The Python bot logs "[WATCHING] SYMBOL ... PnL=+0.1234 ..." every cycle.
// ══════════════════════════════════════════════════════════════════════════════

app.get('/api/user/open-pnl', requireUser, async (req, res) => {
  // 1. Get all open trades for this user
  const { data: openTrades } = await supabase
    .from('trades')
    .select('id, bot_id, trading_pair, trade_type, entry_price, quantity, leverage, tp_price, sl_price, opened_at')
    .eq('user_id', req.user.id)
    .eq('status', 'open');

  if (!openTrades || openTrades.length === 0) {
    return res.json({ trades: [] });
  }

  // 2. For each open trade, find most recent WATCHING log from that bot
  const enriched = await Promise.all(openTrades.map(async (trade) => {
    const { data: logs } = await supabase
      .from('bot_logs')
      .select('message, logged_at')
      .eq('bot_id', trade.bot_id)
      .ilike('message', `%[WATCHING] ${trade.trading_pair}%`)
      .order('logged_at', { ascending: false })
      .limit(1);

    let unrealized_pnl = 0;
    let current_price  = null;
    let pnl_r          = null;

    if (logs && logs.length > 0) {
      const msg = logs[0].message;
      // Parse: PnL=+0.1234
      const pnlMatch = msg.match(/PnL=([+-]?\d+\.\d+)/);
      if (pnlMatch) unrealized_pnl = parseFloat(pnlMatch[1]);
      // Parse: now=84.1100
      const nowMatch = msg.match(/now=([\d.]+)/);
      if (nowMatch) current_price = parseFloat(nowMatch[1]);
      // Parse: R=+0.54
      const rMatch = msg.match(/R=([+-]?[\d.]+)/);
      if (rMatch) pnl_r = parseFloat(rMatch[1]);
    }

    return {
      ...trade,
      unrealized_pnl,
      current_price,
      pnl_r,
    };
  }));

  res.json({ trades: enriched });
});

// GET /api/bots/:id/detail — full bot detail for the detail page
app.get('/api/bots/:id/detail', requireUser, async (req, res) => {
  const { id: botId } = req.params;

  const [botRes, tradesRes, signalsRes, closedCountRes, openCountRes] = await Promise.all([
    supabase.from('bots').select('*').eq('id', botId).eq('user_id', req.user.id).single(),
    // No limit — fetch ALL trades so counts are exact and pagination issues are ruled out
    supabase.from('trades').select('*').eq('bot_id', botId).eq('user_id', req.user.id)
      .order('opened_at', { ascending: false }),
    // More signals for a richer chart baseline (up to 200 = ~3h of 5m bars)
    supabase.from('signals').select('price_at_signal, signaled_at, signal, confidence, regime, action_taken')
      .eq('bot_id', botId).order('signaled_at', { ascending: false }).limit(200),
    // Authoritative closed trade count direct from trades table
    supabase.from('trades').select('id', { count: 'exact', head: true })
      .eq('bot_id', botId).eq('user_id', req.user.id).eq('status', 'closed'),
    // Open trade count
    supabase.from('trades').select('id', { count: 'exact', head: true })
      .eq('bot_id', botId).eq('user_id', req.user.id).eq('status', 'open'),
  ]);

  if (!botRes.data) return res.status(404).json({ error: 'Bot not found' });

  const closedCount = closedCountRes.count ?? 0;
  const openCount   = openCountRes.count   ?? 0;

  res.json({
    bot:          botRes.data,
    trades:       tradesRes.data || [],
    signals:      (signalsRes.data || []).reverse(), // chronological for chart
    // Ground-truth counts from trades table (bot.total_trades can lag if
    // a trade was inserted without going through update_bot_stats_on_close)
    closed_count: closedCount,
    open_count:   openCount,
    total_count:  closedCount + openCount,
  });
});


// ══════════════════════════════════════════════════════════════════════════════
// BOT EDIT (PUT /api/bots/:id)
// Updates editable bot fields. Cannot change trading_pair while running.
// ══════════════════════════════════════════════════════════════════════════════

app.put('/api/bots/:id', requireUser, async (req, res) => {
  const { id: botId } = req.params;
  const { data: bot, error } = await supabase.from('bots').select('*')
    .eq('id', botId).eq('user_id', req.user.id).single();
  if (error || !bot) return res.status(404).json({ error: 'Bot not found' });

  const {
    bot_name, leverage, min_confidence,
    stop_loss_percent, take_profit_percent,
    trade_amount, trade_amount_type,
    daily_max_loss, max_open_trades, max_trades_per_day,
  } = req.body;

  // Build update payload — only include fields that were sent
  const patch = {};
  if (bot_name            !== undefined) patch.bot_name            = bot_name;
  if (leverage            !== undefined) patch.leverage            = Number(leverage);
  if (min_confidence      !== undefined) patch.min_confidence      = Number(min_confidence);
  if (stop_loss_percent   !== undefined) patch.stop_loss_percent   = Number(stop_loss_percent);
  if (take_profit_percent !== undefined) patch.take_profit_percent = Number(take_profit_percent);
  if (trade_amount        !== undefined) patch.trade_amount        = Number(trade_amount);
  if (trade_amount_type   !== undefined) patch.trade_amount_type   = trade_amount_type;
  if (daily_max_loss      !== undefined) patch.daily_max_loss      = Number(daily_max_loss);
  if (max_open_trades     !== undefined) patch.max_open_trades     = Number(max_open_trades);
  if (max_trades_per_day  !== undefined) patch.max_trades_per_day  = Number(max_trades_per_day);
  patch.updated_at = new Date().toISOString();

  const { data: updated, error: upErr } = await supabase.from('bots')
    .update(patch).eq('id', botId).select().single();
  if (upErr) return res.status(500).json({ error: upErr.message });

  res.json({ success: true, bot: updated });
});

// ══════════════════════════════════════════════════════════════════════════════
// WALLET BALANCE (GET /api/user/balance)
// Fetches USDT balance directly from Binance testnet using the user's
// first connected exchange. No bot needs to be running.
// ══════════════════════════════════════════════════════════════════════════════

app.get('/api/user/balance', requireUser, async (req, res) => {
  // Get the first exchange for this user — do NOT filter by is_connected.
  // is_connected can be false due to IP restriction during the test, even
  // though keys are saved and Binance accepts them. Always try to fetch.
  const { data: conn } = await supabase
    .from('exchange_connections')
    .select('api_key, api_secret')
    .eq('user_id', req.user.id)
    .order('created_at', { ascending: true })
    .limit(1)
    .maybeSingle();

  if (!conn || !conn.api_key) {
    // No exchange connected — return zeros gracefully
    return res.json({ total_balance: 0, available_balance: 0, unrealized_pnl: 0, connected: false });
  }

  // Check cache first — avoids hammering exchange on every Dashboard render
  const cached = getCachedBalance(req.user.id);
  if (cached) return res.json(cached);

  try {
    const ts    = Date.now();
    const query = `timestamp=${ts}&recvWindow=10000`;
    const sig   = crypto.createHmac('sha256', (conn.api_secret || '').trim()).update(query).digest('hex');
    const url   = `https://fapi.binance.com/fapi/v2/balance?${query}&signature=${sig}`;

    const response = await fetch(url, {
      headers: { 'X-MBX-APIKEY': conn.api_key },
      signal: AbortSignal.timeout(8000),
    });

    if (!response.ok) {
      const err = await response.json().catch(() => ({}));
      // Keys exist but Binance rejected the call (IP restriction, clock skew, etc.).
      // Return connected:true so the UI shows the exchange as linked — just without balance.
      const result = { total_balance: 0, available_balance: 0, unrealized_pnl: 0,
                       connected: true, balance_error: err.msg || 'Could not fetch balance' };
      return res.json(result);
    }

    const data = await response.json();
    const usdt = Array.isArray(data) ? data.find(b => b.asset === 'USDT') : null;

    const result = usdt ? {
      total_balance:     parseFloat(usdt.balance          || 0),
      available_balance: parseFloat(usdt.availableBalance || 0),
      unrealized_pnl:    parseFloat(usdt.crossUnPnl       || 0),
      connected:         true,
    } : { total_balance: 0, available_balance: 0, unrealized_pnl: 0, connected: true };

    setCachedBalance(req.user.id, result);
    res.json(result);
  } catch (e) {
    logFetchError('/api/user/balance', e);   // suppresses repeated spam
    res.json({ total_balance: 0, available_balance: 0, unrealized_pnl: 0,
               connected: false, error: 'Exchange unreachable' });
  }
});

// ══════════════════════════════════════════════════════════════════════════════
// EXCHANGE SYNC (POST /api/user/sync-trades)
//
// Reconciles DB open trades against live exchange positions.
// Called by the frontend "Sync" button on Bots page, and automatically
// by the bot on startup via the heartbeat response.
//
// Algorithm:
//   1. Load all DB trades with status='open' for this user's bots
//   2. For each unique trading_pair, fetch open positions from Binance
//   3. If a DB trade has no matching exchange position → close it as PHANTOM
//      (the exchange closed it via TP/SL/liquidation without the bot recording it)
//   4. Return a summary of what was fixed
//
// Design notes:
//   - Uses the user's first connected exchange (same as /api/user/balance)
//   - Bot-authenticated version is POST /api/bot/sync — called on bot startup
//   - Does NOT open new positions — only closes ghost ones
//   - Safe to call multiple times (idempotent)
// ══════════════════════════════════════════════════════════════════════════════

async function syncTradesForUser(userId, apiKey, apiSecret) {
  // 1. Get all open trades for this user
  const { data: openTrades } = await supabase
    .from('trades')
    .select('id, bot_id, trading_pair, trade_type, entry_price, tp_price, sl_price, opened_at')
    .eq('user_id', userId)
    .eq('status', 'open');

  if (!openTrades || openTrades.length === 0) {
    return { synced: 0, closed_phantom: 0, message: 'No open trades to sync' };
  }

  // 2. Fetch all open positions from exchange (one call, all symbols)
  const ts    = Date.now();
  const query = `timestamp=${ts}&recvWindow=10000`;
  const sig   = crypto.createHmac('sha256', (apiSecret || '').trim()).update(query).digest('hex');
  const url   = `https://fapi.binance.com/fapi/v2/positionRisk?${query}&signature=${sig}`;

  let exchangePositions = [];
  try {
    const resp = await fetch(url, {
      headers: { 'X-MBX-APIKEY': apiKey },
      signal: AbortSignal.timeout(10000),
    });
    if (resp.ok) {
      const all = await resp.json();
      // Only positions with non-zero size
      exchangePositions = Array.isArray(all)
        ? all.filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0)
        : [];
    }
  } catch (e) {
    console.error('[sync] Exchange fetch failed:', e.message);
    return { synced: 0, closed_phantom: 0, error: e.message };
  }

  // Build lookup: symbol → positionAmt (positive=long, negative=short)
  const exchangeMap = {};
  for (const p of exchangePositions) {
    const sym = p.symbol.toUpperCase();
    exchangeMap[sym] = parseFloat(p.positionAmt || 0);
    if (!exchangeMap[sym + '_price']) {
      exchangeMap[sym + '_price']    = parseFloat(p.markPrice || 0);
      exchangeMap[sym + '_unrealised'] = parseFloat(p.unRealizedProfit || 0);
    }
  }

  // 3. Close phantom trades
  let closed_phantom = 0;
  const now = new Date().toISOString();

  for (const trade of openTrades) {
    const sym     = trade.trading_pair.toUpperCase();
    const botSide = trade.trade_type;          // 'long' or 'short'
    const exchAmt = exchangeMap[sym] ?? null;  // null = symbol not in exchange response at all

    // A trade is phantom if:
    //   a) The exchange has NO position at all for this symbol, OR
    //   b) The exchange position direction is opposite (can't happen normally but guards edge cases)
    const exchangeHasNoPosition = exchAmt === null || exchAmt === 0;
    const directionMismatch = exchAmt !== null && exchAmt !== 0 &&
      ((botSide === 'long' && exchAmt < 0) || (botSide === 'short' && exchAmt > 0));

    if (exchangeHasNoPosition || directionMismatch) {
      // Infer exit reason from price vs SL/TP
      const markPrice  = exchangeMap[sym + '_price'] || 0;
      const unrealised = exchangeMap[sym + '_unrealised'] || 0;

      let exit_reason = 'SYNC_CLOSED';
      if (trade.sl_price && trade.tp_price && markPrice > 0) {
        if (botSide === 'long') {
          if (markPrice <= parseFloat(trade.sl_price))       exit_reason = 'SL_EXTERNAL';
          else if (markPrice >= parseFloat(trade.tp_price))  exit_reason = 'TP_EXTERNAL';
        } else {
          if (markPrice >= parseFloat(trade.sl_price))       exit_reason = 'SL_EXTERNAL';
          else if (markPrice <= parseFloat(trade.tp_price))  exit_reason = 'TP_EXTERNAL';
        }
      }

      const { error: closeErr } = await supabase.from('trades').update({
        status:      'closed',
        exit_price:  markPrice || null,
        profit_loss: unrealised,
        net_pnl:     unrealised,
        exit_reason,
        closed_at:   now,
        bars_held:   null,
      }).eq('id', trade.id);

      if (!closeErr) {
        // Update bot stats for this phantom close
        const isWin = unrealised > 0;
        await supabase.rpc('update_bot_stats_on_close', {
          p_bot_id: trade.bot_id,
          p_pnl:    unrealised,
          p_is_win: isWin,
        });
        closed_phantom++;
        console.log(`[SYNC] Closed phantom trade ${trade.id} (${sym} ${botSide}) reason=${exit_reason}`);
      }
    }
  }

  return {
    synced: openTrades.length,
    closed_phantom,
    message: closed_phantom > 0
      ? `Closed ${closed_phantom} phantom trade(s) that were no longer on the exchange`
      : 'All open trades verified on exchange — no phantoms found',
  };
}

// User-facing sync (called from Bots page "Sync" button)
app.post('/api/user/sync-trades', requireUser, async (req, res) => {
  // Don't gate on is_connected — IP restriction can mark it false even with valid keys
  const { data: conn } = await supabase
    .from('exchange_connections')
    .select('api_key, api_secret')
    .eq('user_id', req.user.id)
    .order('created_at', { ascending: true })
    .limit(1)
    .maybeSingle();

  if (!conn?.api_key) {
    return res.status(400).json({ error: 'No exchange connection found. Add your Binance API keys in Settings.' });
  }

  try {
    const result = await syncTradesForUser(req.user.id, conn.api_key, conn.api_secret);
    res.json({ success: true, ...result });
  } catch (e) {
    console.error('[sync]', e);
    res.status(500).json({ error: e.message });
  }
});

// Bot-facing sync (called by bot via bot token on startup)
app.post('/api/bot/sync', requireBotToken, async (req, res) => {
  const bot = req.bot;
  let apiKey = '', apiSecret = '';
  if (bot.exchange_id) {
    const { data: ex } = await supabase.from('exchange_connections')
      .select('api_key, api_secret').eq('id', bot.exchange_id).single();
    apiKey = ex?.api_key || ''; apiSecret = ex?.api_secret || '';
  }
  if (!apiKey) return res.json({ success: true, message: 'No exchange credentials — skipping sync' });

  try {
    const result = await syncTradesForUser(bot.user_id, apiKey, apiSecret);
    res.json({ success: true, ...result });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════════════════════
// PAPER / LIVE MODE SYSTEM
// Add these routes to server.js
// ══════════════════════════════════════════════════════════════════════════════

// ── Validate exchange keys (real keys only) ───────────────────────────────
// Called by frontend when user saves a new exchange connection.
// Returns { valid, is_testnet, balance, message }
app.post('/api/exchange/validate', requireUser, async (req, res) => {
  const { api_key, api_secret } = req.body;
  if (!api_key || !api_secret) {
    return res.status(400).json({ valid: false, message: 'api_key and api_secret are required' });
  }

  // Heuristic: testnet keys are <40 chars
  if (api_key.trim().length < 40) {
    return res.json({
      valid:      false,
      is_testnet: true,
      balance:    0,
      message:    'Please connect real Binance API keys. Testnet keys are not supported — create keys at binance.com > Profile > API Management, then enable Futures trading.',
    });
  }

  // Full validation against real Binance endpoint
  try {
    const ts    = Date.now();
    const query = `timestamp=${ts}&recvWindow=10000`;
    const sig   = crypto.createHmac('sha256', api_secret.trim()).update(query).digest('hex');
    // Real Binance endpoint (NOT testnet)
    const url   = `https://fapi.binance.com/fapi/v2/balance?${query}&signature=${sig}`;

    const response = await fetch(url, {
      headers: { 'X-MBX-APIKEY': api_key.trim() },
      signal: AbortSignal.timeout(10_000),
    });

    const data = await response.json();

    if (!response.ok) {
      // Code -2014 or -2015 = invalid keys
      const isAuthError = data?.code === -2014 || data?.code === -2015;
      return res.json({
        valid:      false,
        is_testnet: false,
        balance:    0,
        message:    isAuthError
          ? 'Invalid API key or secret. Please check your credentials and ensure Futures trading is enabled.'
          : (data?.msg || 'Connection failed'),
      });
    }

    if (!Array.isArray(data)) {
      return res.json({ valid: false, balance: 0, message: 'Unexpected response from Binance' });
    }

    const usdt = data.find(b => b.asset === 'USDT');
    const balance = parseFloat(usdt?.availableBalance || 0);

    // Mark the exchange_connection as verified real
    if (req.body.exchange_connection_id) {
      await supabase.from('exchange_connections')
        .update({ is_connected: true, is_verified_real: true, last_tested_at: new Date().toISOString() })
        .eq('id', req.body.exchange_connection_id)
        .eq('user_id', req.user.id);
    }

    return res.json({ valid: true, is_testnet: false, balance, message: 'Connected' });

  } catch (e) {
    return res.json({ valid: false, balance: 0, message: `Connection error: ${e.message}` });
  }
});

// ── Switch bot trading mode ──────────────────────────────────────────────
// PUT /api/bots/:id/trading-mode  { mode: "paper" | "live" }
app.put('/api/bots/:id/trading-mode', requireUser, async (req, res) => {
  const { id: botId } = req.params;
  const { mode }      = req.body;

  if (!['paper', 'live'].includes(mode)) {
    return res.status(400).json({ error: 'mode must be "paper" or "live"' });
  }

  const { data: bot, error } = await supabase.from('bots')
    .select('*').eq('id', botId).eq('user_id', req.user.id).single();
  if (error || !bot) return res.status(404).json({ error: 'Bot not found' });

  if (bot.is_running) {
    return res.status(409).json({
      error: 'Stop the bot before switching trading mode.',
    });
  }

  // For live mode: verify exchange connection has real (non-testnet) keys
  if (mode === 'live' && bot.exchange_id) {
    const { data: conn } = await supabase.from('exchange_connections')
      .select('is_verified_real').eq('id', bot.exchange_id).single();
    if (!conn?.is_verified_real) {
      return res.status(422).json({
        error: 'Please connect and validate real Binance API keys before switching to live mode.',
      });
    }
  }

  const { data: updated } = await supabase.from('bots')
    .update({ trading_mode: mode, updated_at: new Date().toISOString() })
    .eq('id', botId).select().single();

  res.json({ success: true, bot: updated });
});



// ── Server public IP cache ─────────────────────────────────────────────────
let _cachedServerIp = null;

async function getServerPublicIp() {
  if (_cachedServerIp) return _cachedServerIp;

  // Try HTTP sources in parallel — first winner wins, others are ignored
  const sources = [
    'https://api.ipify.org',
    'https://checkip.amazonaws.com',
    'https://icanhazip.com',
    'https://ipecho.net/plain',
  ];

  const fetchIp = async (url) => {
    const res = await fetch(url, { signal: AbortSignal.timeout(3000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const ip = (await res.text()).trim();
    if (!/^\d{1,3}(\.\d{1,3}){3}$/.test(ip)) throw new Error('not an IP');
    return ip;
  };

  // Race all sources — first one to resolve wins
  try {
    const ip = await Promise.any(sources.map(fetchIp));
    _cachedServerIp = ip;
    return ip;
  } catch (_) {
    // All sources failed — last resort: use the local machine address
    // This won't be the public IP but is better than nothing
    return null;
  }
}

// GET /api/server-ip — returns this server's outbound public IP
// Used by the frontend to tell users which IP to add to Binance whitelist
app.get('/api/server-ip', async (_req, res) => {
  const ip = await getServerPublicIp();
  res.json({
    ip,
    available: !!ip,
    message: ip
      ? `Add this IP to your Binance API key whitelist: ${ip}`
      : 'Could not detect server IP.',
  });
});

// ── Reliable subscription status ────────────────────────────────────────────
// Returns the most recent active subscription for the authenticated user.
// Always orders by created_at DESC and filters is_active=true so stale rows
// from previous cancelled subscriptions are never returned.
app.get('/api/user/subscription', requireUser, async (req, res) => {
  const { data: sub, error } = await supabase
    .from('subscriptions')
    .select('*')
    .eq('user_id', req.user.id)
    .eq('is_active', true)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) return res.status(500).json({ error: error.message });
  if (!sub)  return res.json({ active: false, subscription: null });

  // Check expiry
  const expired = sub.expires_at && new Date(sub.expires_at) < new Date();
  if (expired) {
    // Mark it inactive so future queries are clean
    await supabase.from('subscriptions').update({ is_active: false }).eq('id', sub.id);
    return res.json({ active: false, subscription: null, reason: 'expired' });
  }

  res.json({ active: true, subscription: sub });
});

app.get('/health', (_req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

app.listen(PORT, () => console.log(`NexusBot API server on http://127.0.0.1:${PORT}`));