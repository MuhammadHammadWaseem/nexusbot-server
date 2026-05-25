/**
 * NexusBot — Node.js API Server
 * Replaces Laravel completely.
 *
 * Changes in this version:
 *   + GET /api/market/analysis/:symbol  — real signal data from bot's signals table
 *   + /api/bot/config reflects paper/live trading mode
 *   + Exchange tests use Binance Futures live endpoints
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
import Stripe from 'stripe';
import jwt from 'jsonwebtoken';

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app  = express();
const PORT = process.env.PORT || 3001;
app.set('etag', false);

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const stripe = process.env.STRIPE_SECRET_KEY
  ? new Stripe(process.env.STRIPE_SECRET_KEY, { apiVersion: '2024-12-18.acacia' })
  : null;

const PLAN_CONFIG = {
  starter: { trades: 15,  bots: 3,  days: 45,  price: 29,  name: 'Starter' },
  pro:     { trades: 50,  bots: 10, days: 90,  price: 79,  name: 'Pro' },
  elite:   { trades: 200, bots: 25, days: 180, price: 149, name: 'Elite' },
};

const PAID_PAYMENT_STATUSES = ['paid'];

async function getActivePaidSubscription(userId) {
  const { data: sub, error } = await supabase
    .from('subscriptions')
    .select('*')
    .eq('user_id', userId)
    .eq('is_active', true)
    .in('payment_status', PAID_PAYMENT_STATUSES)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  if (!sub) return null;

  const expired = sub.expires_at && new Date(sub.expires_at) < new Date();
  if (expired) {
    await supabase.from('subscriptions').update({ is_active: false }).eq('id', sub.id);
    return null;
  }
  return sub;
}

async function activatePaidSubscriptionFromCheckout(session) {
  if (!session || session.payment_status !== 'paid') return { activated: false };

  const userId   = session.metadata?.user_id;
  const planType = session.metadata?.plan_type;
  const plan     = PLAN_CONFIG[planType];
  if (!userId || !plan) throw new Error('Stripe session is missing valid metadata');

  const { data: existingPayment } = await supabase
    .from('subscription_payments')
    .select('id, subscription_id, status')
    .eq('stripe_checkout_session_id', session.id)
    .maybeSingle();
  if (existingPayment?.status === 'paid' && existingPayment.subscription_id) {
    return { activated: false, subscription_id: existingPayment.subscription_id };
  }

  const expiresAt = new Date();
  expiresAt.setDate(expiresAt.getDate() + plan.days);

  await supabase.from('subscriptions')
    .update({ is_active: false })
    .eq('user_id', userId)
    .eq('is_active', true);

  const { data: sub, error: subErr } = await supabase.from('subscriptions').insert({
    user_id: userId,
    plan_type: planType,
    total_trades: plan.trades,
    remaining_trades: plan.trades,
    total_bot_creations: plan.bots,
    remaining_bot_creations: plan.bots,
    expires_at: expiresAt.toISOString(),
    is_active: true,
    payment_status: 'paid',
    payment_provider: 'stripe',
    stripe_checkout_session_id: session.id,
    stripe_payment_intent_id: typeof session.payment_intent === 'string'
      ? session.payment_intent
      : session.payment_intent?.id || null,
    stripe_customer_id: typeof session.customer === 'string'
      ? session.customer
      : session.customer?.id || null,
    paid_at: new Date().toISOString(),
  }).select().single();
  if (subErr) throw subErr;

  const amount = (session.amount_total || plan.price * 100) / 100;
  const currency = (session.currency || 'usd').toUpperCase();

  const paymentPayload = {
    user_id: userId,
    subscription_id: sub.id,
    plan_type: planType,
    amount,
    currency,
    status: 'paid',
    provider: 'stripe',
    stripe_checkout_session_id: session.id,
    stripe_payment_intent_id: typeof session.payment_intent === 'string'
      ? session.payment_intent
      : session.payment_intent?.id || null,
    stripe_customer_id: typeof session.customer === 'string'
      ? session.customer
      : session.customer?.id || null,
    raw_event: session,
    paid_at: new Date().toISOString(),
  };

  if (existingPayment) {
    await supabase.from('subscription_payments').update(paymentPayload).eq('id', existingPayment.id);
  } else {
    await supabase.from('subscription_payments').insert(paymentPayload);
  }

  await supabase.from('revenue_records').insert({
    user_id: userId,
    amount,
    plan_type: planType,
    description: `${planType} plan purchase via Stripe Checkout (${currency})`,
  });

  return { activated: true, subscription_id: sub.id };
}

// ── In-process balance cache (30s TTL) ───────────────────────────────────
// Prevents hammering Binance Futures on every Dashboard load.
// The balance endpoint is non-critical — stale by 30s is acceptable.
const balanceCache = new Map(); // key = user_id:exchange_connection_id:fingerprint → { data, expiresAt }
const BALANCE_TTL_MS = 30_000;

function balanceCacheKey(userId, conn) {
  return `${userId}:${conn?.id || 'none'}:${conn?.account_fingerprint || 'unknown'}`;
}
function getCachedBalance(userId, conn) {
  const entry = balanceCache.get(balanceCacheKey(userId, conn));
  if (entry && entry.expiresAt > Date.now()) return entry.data;
  return null;
}
function setCachedBalance(userId, conn, data) {
  balanceCache.set(balanceCacheKey(userId, conn), { data, expiresAt: Date.now() + BALANCE_TTL_MS });
}
function clearUserBalanceCache(userId) {
  for (const key of balanceCache.keys()) {
    if (key.startsWith(`${userId}:`)) balanceCache.delete(key);
  }
}

function normalizeExchangeName(name) {
  const value = String(name || 'binance').toLowerCase().trim();
  return value === 'coinbase' ? 'coinbase' : 'binance';
}

function buildExchangeAccountFingerprint(exchangeName, apiKey) {
  const exchange = normalizeExchangeName(exchangeName);
  const identity = String(apiKey || '').trim();
  if (!identity) return null;
  return crypto.createHash('sha256').update(`${exchange}:${identity}`).digest('hex');
}

function getConnectionFingerprint(conn) {
  if (!conn) return null;
  return conn.account_fingerprint || buildExchangeAccountFingerprint(conn.exchange_name, conn.api_key);
}

async function recordExchangeAudit(userId, eventType, details = {}, exchangeConnectionId = null) {
  try {
    await supabase.from('exchange_audit_events').insert({
      user_id: userId,
      exchange_connection_id: exchangeConnectionId,
      event_type: eventType,
      details,
    });
  } catch (e) {
    console.warn('[exchange_audit]', e?.message || e);
  }
}

async function upsertActiveExchangeContext(userId, conn, reason = 'manual') {
  const fingerprint = getConnectionFingerprint(conn);
  const exchangeName = normalizeExchangeName(conn.exchange_name);

  await supabase.from('exchange_connections')
    .update({ is_active: false })
    .eq('user_id', userId);

  await supabase.from('exchange_connections')
    .update({
      is_active: true,
      account_fingerprint: fingerprint,
      lifecycle_status: conn.is_connected === false ? 'disconnected' : 'connected',
    })
    .eq('id', conn.id)
    .eq('user_id', userId);

  await supabase.from('user_exchange_context').upsert({
    user_id: userId,
    active_exchange_connection_id: conn.id,
    active_exchange_name: exchangeName,
    active_account_fingerprint: fingerprint,
    updated_at: new Date().toISOString(),
  }, { onConflict: 'user_id' });

  const { data: bots } = await supabase.from('bots')
    .select('id, exchange_id, exchange_account_fingerprint, is_running')
    .eq('user_id', userId);

  const { data: userConnections } = await supabase.from('exchange_connections')
    .select('id')
    .eq('user_id', userId);
  const knownConnectionIds = new Set((userConnections || []).map(row => row.id));

  const compatibleIds = (bots || [])
    .filter(bot => {
      const sameConnection = bot.exchange_id === conn.id;
      const sameFingerprint = fingerprint && bot.exchange_account_fingerprint === fingerprint;
      const unversionedCurrentConnection = sameConnection && !bot.exchange_account_fingerprint;
      const legacyOrphanBinanceBot =
        exchangeName === 'binance' &&
        bot.exchange_id &&
        !knownConnectionIds.has(bot.exchange_id) &&
        !bot.exchange_account_fingerprint;

      return sameConnection || sameFingerprint || unversionedCurrentConnection || legacyOrphanBinanceBot;
    })
    .map(bot => bot.id);

  const incompatibleIds = (bots || [])
    .filter(bot => bot.exchange_id && !compatibleIds.includes(bot.id))
    .map(bot => bot.id);

  if (compatibleIds.length > 0) {
    await supabase.from('bots').update({
      exchange_id: conn.id,
      exchange_account_fingerprint: fingerprint,
      lifecycle_status: 'active',
      requires_reconfiguration: false,
      disabled_reason: null,
      updated_at: new Date().toISOString(),
    }).in('id', compatibleIds);
  }

  if (incompatibleIds.length > 0) {
    await supabase.from('bots').update({
      is_running: false,
      lifecycle_status: 'requires_reconfiguration',
      requires_reconfiguration: true,
      disabled_reason: reason === 'credentials_changed'
        ? 'API keys changed for this exchange account.'
        : 'Bot belongs to a different exchange/account context.',
      updated_at: new Date().toISOString(),
    }).in('id', incompatibleIds);
  }

  clearUserBalanceCache(userId);
  await recordExchangeAudit(userId, 'active_exchange_changed', {
    reason,
    exchange_name: exchangeName,
    account_fingerprint: fingerprint,
    compatible_bot_count: compatibleIds.length,
    incompatible_bot_count: incompatibleIds.length,
  }, conn.id);

  return {
    connection: { ...conn, exchange_name: exchangeName, account_fingerprint: fingerprint, is_active: true },
    recovered_bot_count: compatibleIds.length,
    disabled_bot_count: incompatibleIds.length,
  };
}

async function getActiveExchangeContext(userId, { allowFallback = true } = {}) {
  const { data: context } = await supabase.from('user_exchange_context')
    .select('active_exchange_connection_id, active_exchange_name, active_account_fingerprint, updated_at')
    .eq('user_id', userId)
    .maybeSingle();

  if (context?.active_exchange_connection_id) {
    const { data: conn } = await supabase.from('exchange_connections')
      .select('*')
      .eq('id', context.active_exchange_connection_id)
      .eq('user_id', userId)
      .maybeSingle();
    if (conn) {
      const fingerprint = getConnectionFingerprint(conn);
      if (!conn.account_fingerprint && fingerprint) {
        await supabase.from('exchange_connections')
          .update({ account_fingerprint: fingerprint })
          .eq('id', conn.id);
      }
      return {
        context,
        connection: { ...conn, account_fingerprint: fingerprint },
      };
    }
  }

  if (!allowFallback) return { context: null, connection: null };

  const { data: conns } = await supabase.from('exchange_connections')
    .select('*')
    .eq('user_id', userId)
    .order('is_active', { ascending: false })
    .order('is_verified_real', { ascending: false })
    .order('is_connected', { ascending: false })
    .order('created_at', { ascending: false });

  const fallback = (conns || []).find(c => c.api_key && c.api_secret) || null;
  if (!fallback) return { context: null, connection: null };
  const result = await upsertActiveExchangeContext(userId, fallback, 'fallback');
  return {
    context: {
      active_exchange_connection_id: result.connection.id,
      active_exchange_name: result.connection.exchange_name,
      active_account_fingerprint: result.connection.account_fingerprint,
    },
    connection: result.connection,
  };
}

async function assertBotMatchesActiveExchange(userId, bot) {
  const { connection } = await getActiveExchangeContext(userId);
  if (!connection) {
    return { ok: false, status: 422, error: 'No active exchange selected. Connect and activate an exchange first.' };
  }
  if (!bot.exchange_id) {
    return { ok: false, status: 422, error: 'This bot is not assigned to the active exchange. Reconfigure it before starting.' };
  }
  const activeFingerprint = getConnectionFingerprint(connection);
  const botFingerprint = bot.exchange_account_fingerprint || null;
  const sameAccount = activeFingerprint && botFingerprint === activeFingerprint;
  if (bot.exchange_id !== connection.id && sameAccount) {
    await supabase.from('bots').update({
      exchange_id: connection.id,
      lifecycle_status: 'active',
      requires_reconfiguration: false,
      disabled_reason: null,
      updated_at: new Date().toISOString(),
    }).eq('id', bot.id);
    bot.exchange_id = connection.id;
  }
  if (bot.exchange_id !== connection.id || (botFingerprint && botFingerprint !== activeFingerprint)) {
    await supabase.from('bots').update({
      is_running: false,
      lifecycle_status: 'requires_reconfiguration',
      requires_reconfiguration: true,
      disabled_reason: 'Bot exchange/account does not match the active exchange.',
      updated_at: new Date().toISOString(),
    }).eq('id', bot.id);
    return { ok: false, status: 409, error: 'This bot belongs to a different exchange/account. Reconfigure it for the active exchange before starting.' };
  }
  if (bot.lifecycle_status && !['active', 'stopped'].includes(bot.lifecycle_status) && bot.requires_reconfiguration) {
    return { ok: false, status: 409, error: `This bot status is "${bot.lifecycle_status}" and requires reconfiguration.` };
  }
  if (!botFingerprint) {
    await supabase.from('bots').update({
      exchange_account_fingerprint: activeFingerprint,
      lifecycle_status: 'active',
      requires_reconfiguration: false,
      disabled_reason: null,
    }).eq('id', bot.id);
  }
  return { ok: true, connection: { ...connection, account_fingerprint: activeFingerprint } };
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
    console.warn(`[EXCHANGE] ${endpoint}: ${err?.cause?.code || 'fetch failed'} - is Binance Futures reachable?`);
  }
}

// ── CORS: allow production domain + local dev ─────────────────────────────
const ALLOWED_ORIGINS = [
  process.env.FRONTEND_URL,          // set in .env → https://ecom-accelerate.com
  'https://ecom-accelerate.com',      // production domain
  'https://www.ecom-accelerate.com',  // www variant
  'http://localhost:5173',            // local Vite dev
  'http://127.0.0.1:5173',
].filter(Boolean);

app.use(cors({
  origin: (origin, cb) => {
    // Allow requests with no origin (curl, Postman, same-origin server calls)
    if (!origin || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
    cb(new Error(`CORS blocked: ${origin}`));
  },
  credentials: true,
  methods: ['GET','POST','PUT','DELETE','OPTIONS'],
  allowedHeaders: ['Content-Type','Authorization'],
}));

// Handle OPTIONS preflight for all routes
app.options('*', cors());

app.post('/api/stripe/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  if (!stripe || !process.env.STRIPE_WEBHOOK_SECRET) {
    return res.status(500).json({ error: 'Stripe webhook is not configured' });
  }

  let event;
  try {
    event = stripe.webhooks.constructEvent(
      req.body,
      req.headers['stripe-signature'],
      process.env.STRIPE_WEBHOOK_SECRET
    );
  } catch (err) {
    console.warn('[stripe/webhook] Signature verification failed:', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  try {
    if (event.type === 'checkout.session.completed') {
      await activatePaidSubscriptionFromCheckout(event.data.object);
    } else if (event.type === 'checkout.session.expired') {
      const session = event.data.object;
      await supabase.from('subscription_payments').upsert({
        user_id: session.metadata?.user_id || null,
        plan_type: session.metadata?.plan_type || null,
        amount: (session.amount_total || 0) / 100,
        currency: (session.currency || 'usd').toUpperCase(),
        status: 'expired',
        provider: 'stripe',
        stripe_checkout_session_id: session.id,
        raw_event: session,
      }, { onConflict: 'stripe_checkout_session_id' });
    }
    res.json({ received: true });
  } catch (err) {
    console.error('[stripe/webhook]', err);
    res.status(500).json({ error: 'Webhook processing failed' });
  }
});

app.use(express.json({ limit: '1mb' }));

// ── Strip /api-server prefix injected by Nginx proxy ──────────────────────
// Nginx config:  location /api-server/ { proxy_pass http://127.0.0.1:3001/; }
// Browser sends: POST https://ecom-accelerate.com/api-server/api/bots/:id/start
// Nginx forwards: POST http://127.0.0.1:3001/api/bots/:id/start  (trailing slash strips it)
// BUT if Nginx does NOT strip it, Node receives /api-server/api/... and no route matches.
// This middleware handles BOTH cases safely.
app.use((req, _res, next) => {
  if (req.path.startsWith('/api-server')) {
    req.url = req.url.slice('/api-server'.length) || '/';
  }
  next();
});

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
  if (req.bot.exchange_id) {
    const active = await getActiveExchangeContext(req.bot.user_id, { allowFallback: false });
    const activeConn = active.connection;
    const activeFingerprint = activeConn ? getConnectionFingerprint(activeConn) : null;
    if (!activeConn || req.bot.exchange_id !== activeConn.id ||
        (req.bot.exchange_account_fingerprint && req.bot.exchange_account_fingerprint !== activeFingerprint) ||
        req.bot.requires_reconfiguration) {
      commands.push({ command: 'stop', close_open_trades: true, reason: 'exchange_context_changed' });
      await supabase.from('bots').update({
        is_running: false,
        lifecycle_status: 'requires_reconfiguration',
        requires_reconfiguration: true,
        disabled_reason: 'Active exchange/account changed while bot was running.',
        updated_at: new Date().toISOString(),
      }).eq('id', req.bot.id);
    }
  }
  res.json({ success: true, commands });
});

app.post('/api/bot/trade/open', requireBotToken, async (req, res) => {
  const bot = req.bot;
  const { symbol, side, entry_price, quantity, leverage, tp_price, sl_price,
          confidence, signal_type, regime, order_id, opened_at } = req.body;
  let exchangeName = 'binance';
  let exchangeConnectionId = bot.exchange_id || null;
  let exchangeFingerprint = bot.exchange_account_fingerprint || null;
  if (bot.exchange_id) {
    const { data: ex } = await supabase.from('exchange_connections')
      .select('exchange_name, account_fingerprint').eq('id', bot.exchange_id).maybeSingle();
    exchangeName = ex?.exchange_name || 'binance';
    exchangeFingerprint = exchangeFingerprint || ex?.account_fingerprint || null;
  }

  const { data: trade, error } = await supabase.from('trades').insert({
    bot_id: bot.id, user_id: bot.user_id, exchange_name: exchangeName,
    exchange_connection_id: exchangeConnectionId,
    exchange_account_fingerprint: exchangeFingerprint,
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

  if (bot.exchange_id) {
    const validation = await assertBotMatchesActiveExchange(bot.user_id, bot);
    if (!validation.ok) return res.status(validation.status).json({ error: validation.error });
  }

  let apiKey = '', apiSecret = '', exchangeName = 'binance';
  if (bot.exchange_id) {
    const { data: ex } = await supabase.from('exchange_connections').select('api_key, api_secret, exchange_name').eq('id', bot.exchange_id).single();
    apiKey = ex?.api_key || ''; apiSecret = ex?.api_secret || ''; exchangeName = ex?.exchange_name || 'binance';
  }

  res.json({
    bot_id: bot.id, bot_token: bot.bot_token, symbol: bot.trading_pair, timeframe: bot.timeframe,
    leverage: bot.leverage || 5, min_confidence: bot.min_confidence || 65,
    stop_loss_percent: bot.stop_loss_percent, take_profit_percent: bot.take_profit_percent,
    max_trades_per_day: bot.max_trades_per_day, trade_amount: bot.trade_amount,
    trade_amount_type: bot.trade_amount_type, daily_max_loss: bot.daily_max_loss,
    max_open_trades: bot.max_open_trades,
    is_paper_trading: (bot.trading_mode || 'paper') !== 'live',
    exchange: { name: exchangeName, api_key: apiKey, api_secret: apiSecret, testnet: false },
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
app.get('/api/bots/:id/logs', requireUser, async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 100, 300);
  const since = typeof req.query.since === 'string' ? req.query.since : null;

  const { data: bot, error: botErr } = await supabase
    .from('bots')
    .select('id')
    .eq('id', req.params.id)
    .eq('user_id', req.user.id)
    .maybeSingle();

  if (botErr) return res.status(500).json({ error: botErr.message });
  if (!bot) return res.status(404).json({ error: 'Bot not found' });

  let query = supabase
    .from('bot_logs')
    .select('id, bot_id, level, channel, message, logged_at')
    .eq('bot_id', req.params.id)
    .order('logged_at', { ascending: false })
    .limit(limit);

  if (since) query = query.gte('logged_at', since);

  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });

  res.json({ logs: (data || []).reverse() });
});

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
  try {
  const { data: bot, error } = await supabase.from('bots').select('*').eq('id', botId).eq('user_id', req.user.id).single();
  if (error || !bot) return res.status(404).json({ error: 'Bot not found' });
  if (bot.is_running) return res.json({ success: true, message: 'Already running' });

  const sub = await getActivePaidSubscription(req.user.id);
  if (!sub) return res.status(402).json({ error: 'No paid active subscription. Complete payment before starting bots.' });
  if (sub.remaining_trades <= 0) return res.status(402).json({ error: 'No remaining trades in your plan.' });

  const validation = await assertBotMatchesActiveExchange(req.user.id, bot);
  if (!validation.ok) return res.status(validation.status).json({ error: validation.error });

  // Fetch exchange API keys if a connection is linked
  let apiKey = '', apiSecret = '', exchangeName = 'binance', exchangeFingerprint = null;
  if (bot.exchange_id) {
    const { data: ex } = await supabase
      .from('exchange_connections')
      .select('api_key, api_secret, exchange_name, account_fingerprint')
      .eq('id', bot.exchange_id)
      .single();
    apiKey    = (ex?.api_key    || '').trim();
    apiSecret = ex?.exchange_name === 'coinbase'
      ? String(ex?.api_secret || '').trim()
      : (ex?.api_secret || '').trim();
    exchangeName = ex?.exchange_name || 'binance';
    exchangeFingerprint = ex?.account_fingerprint || buildExchangeAccountFingerprint(exchangeName, apiKey);
  }

  if ((bot.trading_mode || 'paper') === 'live') {
    if (!bot.exchange_id) {
      return res.status(422).json({ error: 'Live trading requires a connected exchange.' });
    }
    if (exchangeName === 'coinbase') {
      return res.status(422).json({
        error: 'Coinbase balance sync is supported, but live Coinbase order execution is not enabled yet. Use Binance for live bots or keep Coinbase bots in paper mode.',
      });
    }
    if (apiKey.length < 40 || apiSecret.length < 40) {
      return res.status(422).json({
        error: 'Live trading requires real Binance API keys. Testnet, missing, or short keys are only valid for paper mode.',
      });
    }
  }

  const botToken = crypto.randomBytes(32).toString('hex');
  await supabase.from('bots').update({
    bot_token: botToken,
    is_running: true,
    lifecycle_status: 'active',
    requires_reconfiguration: false,
    disabled_reason: null,
    exchange_account_fingerprint: exchangeFingerprint || bot.exchange_account_fingerprint || null,
    updated_at: new Date().toISOString(),
  }).eq('id', botId);

  const configDir  = process.env.BOT_CONFIG_DIR || path.join(__dirname, '../AITradingBot/bot_configs');
  fs.mkdirSync(configDir, { recursive: true });
  const configPath = path.join(configDir, `bot_${botId}.json`);

  // Config keys must match EXACTLY what run_bot_managed.py reads:
  //   config["api_key"]                  → exchange key
  //   config["api_secret"]               → exchange secret
  //   config["is_testnet"]               -> false; production uses real Binance keys
  //   config["risk_per_trade"]           → % of balance per trade
  //   config["daily_loss_limit"]         → USDT hard stop per day
  //   config["base_confidence_threshold"]→ ML min confidence (0-100 scale)
  //   config["laravel_api_url"]          → reporter API base (bot appends /heartbeat etc.)
  fs.writeFileSync(configPath, JSON.stringify({
    // Identity
    bot_id:     botId,
    bot_token:  botToken,
    // Reporter URL — bot reads config["laravel_api_url"] to build endpoint URLs
    // BOT_API_URL: the URL the Python bot uses to call this Node server.
    // The bot runs on the SAME VPS — always use 127.0.0.1, never the public domain.
    // Routing through the public domain adds unnecessary Nginx overhead.
    // In .env: BOT_API_URL=http://127.0.0.1:3001  (loopback, fastest path)
    laravel_api_url: `${process.env.BOT_API_URL || 'http://127.0.0.1:3001'}/api/bot`,
    // Trading params
    symbol:     bot.trading_pair,
    timeframe:  bot.timeframe,
    leverage:   bot.leverage    || 5,
    // Exchange credentials
    api_key:      apiKey,
    api_secret:   apiSecret,
    exchange:     exchangeName,
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
    base_confidence_threshold: Number(bot.min_confidence)     || 65,
    max_open_trades:           Number(bot.max_open_trades)    || 1,
    max_trades_per_day:        Number(bot.max_trades_per_day) || 5,
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

  await supabase.from('bot_logs').insert({
    bot_id: botId,
    level: 'info',
    channel: 'bot',
    message: `[START] Bot process launch requested via ${pythonExe}`,
    logged_at: new Date().toISOString(),
  });

  child.on('error', async (err) => {
    console.error(`[BOT] Failed to start ${botId}:`, err);
    await supabase.from('bots').update({
      is_running: false,
      lifecycle_status: 'requires_reconfiguration',
      disabled_reason: `Bot process failed to start: ${err.message}`,
      updated_at: new Date().toISOString(),
    }).eq('id', botId);
    await supabase.from('bot_logs').insert({
      bot_id: botId,
      level: 'error',
      channel: 'bot',
      message: `[START_FAILED] ${err.message}`,
      logged_at: new Date().toISOString(),
    });
  });

  console.log(`[BOT] Started ${botId} (PID ${child.pid})`);
  res.json({ success: true, pid: child.pid, message: `Bot started (PID ${child.pid})` });
  } catch (err) {
    const message = err?.message || String(err);
    console.error(`[BOT] Start route failed for ${botId}:`, err);
    await supabase.from('bots').update({
      is_running: false,
      lifecycle_status: 'requires_reconfiguration',
      requires_reconfiguration: true,
      disabled_reason: `Bot start failed: ${message}`,
      updated_at: new Date().toISOString(),
    }).eq('id', botId).eq('user_id', req.user.id);
    await supabase.from('bot_logs').insert({
      bot_id: botId,
      level: 'error',
      channel: 'bot',
      message: `[START_ROUTE_FAILED] ${message}`,
      logged_at: new Date().toISOString(),
    });
    res.status(500).json({ error: `Bot start failed: ${message}` });
  }
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

app.post('/api/subscriptions/purchase', requireUser, async (req, res) => {
  const { plan_type } = req.body;
  const plan = PLAN_CONFIG[plan_type];
  if (!plan) return res.status(400).json({ error: 'Invalid plan type' });
  if (!stripe) return res.status(500).json({ error: 'Stripe is not configured on the server.' });

  const frontendUrl = (process.env.FRONTEND_URL || 'http://localhost:5173').replace(/\/$/, '');
  const session = await stripe.checkout.sessions.create({
    mode: 'payment',
    client_reference_id: req.user.id,
    customer_email: req.user.email || undefined,
    line_items: [{
      quantity: 1,
      price_data: {
        currency: process.env.STRIPE_CURRENCY || 'usd',
        unit_amount: plan.price * 100,
        product_data: {
          name: `NexusBot ${plan.name} Plan`,
          description: `${plan.trades} trades, ${plan.bots} bot creations, ${plan.days} days access`,
        },
      },
    }],
    metadata: {
      user_id: req.user.id,
      plan_type,
    },
    success_url: `${frontendUrl}/subscription?payment=success&session_id={CHECKOUT_SESSION_ID}`,
    cancel_url: `${frontendUrl}/subscription?payment=cancelled`,
  });

  await supabase.from('subscription_payments').insert({
    user_id: req.user.id,
    plan_type,
    amount: plan.price,
    currency: (process.env.STRIPE_CURRENCY || 'usd').toUpperCase(),
    status: 'checkout_created',
    provider: 'stripe',
    stripe_checkout_session_id: session.id,
    stripe_customer_id: typeof session.customer === 'string' ? session.customer : null,
    raw_event: session,
  });

  res.json({
    success: true,
    checkout_url: session.url,
    url: session.url,
    session_id: session.id,
  });
});

app.post('/api/subscriptions/verify-session', requireUser, async (req, res) => {
  const { session_id } = req.body;
  if (!session_id) return res.status(400).json({ error: 'session_id is required' });
  if (!stripe) return res.status(500).json({ error: 'Stripe is not configured on the server.' });

  const session = await stripe.checkout.sessions.retrieve(session_id);
  if (session.client_reference_id !== req.user.id && session.metadata?.user_id !== req.user.id) {
    return res.status(403).json({ error: 'Checkout session does not belong to this user' });
  }

  if (session.payment_status !== 'paid') {
    return res.status(402).json({ success: false, status: session.payment_status });
  }

  await activatePaidSubscriptionFromCheckout(session);
  const subscription = await getActivePaidSubscription(req.user.id);
  res.json({ success: true, subscription });
});

app.post('/api/subscriptions/cancel', requireUser, async (req, res) => {
  const { error } = await supabase.from('subscriptions')
    .update({ is_active: false })
    .eq('user_id', req.user.id)
    .eq('is_active', true)
    .in('payment_status', PAID_PAYMENT_STATUSES);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ══════════════════════════════════════════════════════════════════════════════
// EXCHANGE
// ══════════════════════════════════════════════════════════════════════════════

function normalizeCoinbasePrivateKey(secret) {
  const raw = String(secret || '').trim();
  try {
    const parsed = JSON.parse(raw);
    const key = parsed.privateKey || parsed.private_key || parsed.key || parsed.secret;
    if (key) return String(key).trim().replace(/\\n/g, '\n');
  } catch (_) {
    // Not JSON; treat it as the PEM/private key itself.
  }
  return raw.replace(/\\n/g, '\n');
}

function parseJsonObject(value) {
  try {
    const parsed = JSON.parse(String(value || '').trim());
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch (_) {
    return null;
  }
}

function getCoinbaseCredentials(conn) {
  const apiKeyRaw = String(conn.api_key || '').trim();
  const apiSecretRaw = String(conn.api_secret || '').trim();
  const keyJson = parseJsonObject(apiKeyRaw);
  const secretJson = parseJsonObject(apiSecretRaw);
  const json = keyJson || secretJson || {};

  const keyName = String(
    json.name ||
    json.keyName ||
    json.key_name ||
    json.apiKeyName ||
    json.api_key_name ||
    apiKeyRaw
  ).trim();

  const privateKey = normalizeCoinbasePrivateKey(
    json.privateKey ||
    json.private_key ||
    json.key ||
    json.secret ||
    apiSecretRaw
  );

  return { keyName, privateKey };
}

function getCoinbaseKeyType(privateKey) {
  if (privateKey.includes('BEGIN EC PRIVATE KEY')) return 'EC PRIVATE KEY';
  if (privateKey.includes('BEGIN PRIVATE KEY')) return 'PRIVATE KEY';
  return 'UNKNOWN';
}

function getCoinbaseAuthDebug(keyName, privateKey, method, requestPath, issuer = 'cdp') {
  const keyParts = String(keyName || '').split('/');
  return {
    issuer,
    uri: `${method.toUpperCase()} api.coinbase.com${requestPath}`,
    key_name_starts_with_organizations: String(keyName || '').startsWith('organizations/'),
    key_name_segments: keyParts.length,
    key_name_preview: keyName ? `${keyName.slice(0, 24)}...${keyName.slice(-12)}` : '',
    private_key_type: getCoinbaseKeyType(privateKey),
    private_key_has_begin: privateKey.includes('-----BEGIN'),
    private_key_has_end: privateKey.includes('-----END'),
    server_time_utc: new Date().toISOString(),
  };
}

function buildCoinbaseJwt(apiKeyName, apiSecret, method, requestPath, issuer = 'cdp') {
  const keyName = String(apiKeyName || '').trim();
  const privateKey = normalizeCoinbasePrivateKey(apiSecret);

  if (!keyName.startsWith('organizations/')) {
    throw new Error('Coinbase API key must be the full key name: organizations/{org_id}/apiKeys/{key_id}');
  }
  if (!privateKey.includes('BEGIN EC PRIVATE KEY') && !privateKey.includes('BEGIN PRIVATE KEY')) {
    throw new Error('Coinbase private key must be the full EC private key PEM.');
  }

  const now = Math.floor(Date.now() / 1000);
  const uri = `${method.toUpperCase()} api.coinbase.com${requestPath}`;

  return jwt.sign(
    {
      iss: issuer,
      sub: keyName,
      nbf: now - 5,
      exp: now + 120,
      uri,
    },
    privateKey,
    {
      algorithm: 'ES256',
      noTimestamp: true,
      header: {
        kid: keyName,
        nonce: crypto.randomBytes(16).toString('hex'),
      },
    }
  );
}

async function coinbaseRequest(conn, method, requestPath, body = null) {
  const { keyName, privateKey } = getCoinbaseCredentials(conn);
  const issuers = ['cdp'];
  let lastErr = null;

  for (const issuer of issuers) {
    const token = buildCoinbaseJwt(keyName, privateKey, method, requestPath, issuer);
    const response = await fetch(`https://api.coinbase.com${requestPath}`, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache',
      },
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(10_000),
    });
    const text = await response.text().catch(() => '');
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch (_) {
      data = { raw: text };
    }
    if (response.ok) return data;

    const msg = data?.error_details || data?.message || data?.error || text || `HTTP ${response.status}`;
    const err = new Error(msg);
    err.status = response.status;
    err.data = data;
    err.auth_debug = getCoinbaseAuthDebug(keyName, privateKey, method, requestPath, issuer);
    lastErr = err;

    if (response.status !== 401) break;
  }

  throw lastErr || new Error('Coinbase request failed');
}

async function getCoinbaseUsdPrice(asset) {
  const symbol = String(asset || '').toUpperCase();
  if (['USD', 'USDC', 'USDT'].includes(symbol)) return 1;
  const productId = `${symbol}-USD`;
  const response = await fetch(`https://api.exchange.coinbase.com/products/${encodeURIComponent(productId)}/ticker`, {
    headers: { 'Cache-Control': 'no-cache' },
    signal: AbortSignal.timeout(8_000),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data?.message || `No ${productId} price`);
  const price = parseFloat(data?.price || 0);
  if (!Number.isFinite(price) || price <= 0) throw new Error(`Invalid ${productId} price`);
  return price;
}

async function getCoinbaseBalance(conn) {
  const data = await coinbaseRequest(conn, 'GET', '/api/v3/brokerage/accounts');
  const accounts = Array.isArray(data?.accounts) ? data.accounts : [];
  let total = 0;
  let available = 0;
  const assets = [];
  const errors = [];

  for (const account of accounts) {
    const currency = String(account?.currency || '').toUpperCase();
    const free = parseFloat(account?.available_balance?.value || 0);
    const hold = parseFloat(account?.hold?.value || 0);
    const amount = free + hold;
    if (!currency || amount <= 0) continue;

    try {
      const price = await getCoinbaseUsdPrice(currency);
      const usdValue = amount * price;
      const usdAvailable = free * price;
      total += usdValue;
      available += usdAvailable;
      assets.push({
        asset: currency,
        balance: amount,
        available_balance: free,
        usd_price: price,
        usd_value: usdValue,
        usd_available: usdAvailable,
      });
    } catch (e) {
      errors.push({ source: 'coinbase_price', code: currency, message: e?.message || `Could not price ${currency}` });
    }
  }

  return {
    total_balance: total,
    available_balance: available,
    unrealized_pnl: 0,
    coinbase_balance: total,
    coinbase_available_balance: available,
    coinbase_assets: assets,
    connected: true,
    exchange: 'coinbase',
    balance_sources_checked: ['coinbase_accounts'],
    balance_source_errors: errors,
    balance_error: total === 0 && errors.length === 0 ? 'No Coinbase balances found for this API key.' : '',
  };
}

app.get('/api/exchange/active', requireUser, async (req, res) => {
  let { connection, context } = await getActiveExchangeContext(req.user.id);
  let repair = null;
  if (connection) {
    repair = await upsertActiveExchangeContext(req.user.id, connection, 'context_refresh');
    ({ connection, context } = await getActiveExchangeContext(req.user.id));
  }
  const { data: exchanges } = await supabase.from('exchange_connections')
    .select('id, exchange_name, is_connected, is_verified_real, is_active, lifecycle_status, account_fingerprint, account_label, last_tested_at, created_at')
    .eq('user_id', req.user.id)
    .order('created_at', { ascending: false });

  res.json({
    active: connection ? {
      id: connection.id,
      exchange_name: normalizeExchangeName(connection.exchange_name),
      is_connected: !!connection.is_connected,
      is_verified_real: !!connection.is_verified_real,
      lifecycle_status: connection.lifecycle_status || 'connected',
      account_fingerprint: connection.account_fingerprint,
      account_label: connection.account_label || null,
      last_tested_at: connection.last_tested_at || null,
    } : null,
    context,
    repair: repair ? {
      recovered_bot_count: repair.recovered_bot_count || 0,
      disabled_bot_count: repair.disabled_bot_count || 0,
    } : null,
    exchanges: exchanges || [],
  });
});

app.put('/api/exchange/active', requireUser, async (req, res) => {
  const { exchange_connection_id } = req.body || {};
  if (!exchange_connection_id) return res.status(400).json({ error: 'exchange_connection_id is required' });

  const { data: conn, error } = await supabase.from('exchange_connections')
    .select('*')
    .eq('id', exchange_connection_id)
    .eq('user_id', req.user.id)
    .maybeSingle();
  if (error || !conn) return res.status(404).json({ error: 'Exchange connection not found' });
  if (!conn.api_key || !conn.api_secret) return res.status(422).json({ error: 'Exchange credentials are missing.' });

  const result = await upsertActiveExchangeContext(req.user.id, conn, 'manual');
  res.json({
    success: true,
    active: {
      id: result.connection.id,
      exchange_name: result.connection.exchange_name,
      account_fingerprint: result.connection.account_fingerprint,
    },
    disabled_bot_count: result.disabled_bot_count,
    message: result.disabled_bot_count > 0
      ? `${result.disabled_bot_count} bot(s) require reconfiguration because they belong to another exchange/account.`
      : 'Active exchange updated.',
  });
});

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

    if (conn.exchange_name === 'coinbase') {
      const nextFingerprint = buildExchangeAccountFingerprint('coinbase', apiKey);
      const previousFingerprint = conn.account_fingerprint || null;
      const coinbaseCreds = getCoinbaseCredentials(conn);
      if (!coinbaseCreds.keyName.startsWith('organizations/')) {
        return res.json({
          connected: false,
          exchange: 'coinbase',
          message: 'Coinbase API Key must be the full key name, like organizations/{org_id}/apiKeys/{key_id}. Do not use a Binance key or a short nickname.',
        });
      }
      if (!coinbaseCreds.privateKey.includes('BEGIN EC PRIVATE KEY') && !coinbaseCreds.privateKey.includes('BEGIN PRIVATE KEY')) {
        return res.json({
          connected: false,
          exchange: 'coinbase',
          message: 'Coinbase Private Key must be the full ECDSA private key PEM, including BEGIN/END lines. Ed25519 keys are not supported for this API.',
        });
      }
      const balance = await getCoinbaseBalance(conn);
      await supabase.from('exchange_connections')
        .update({
          is_connected: true,
          is_verified_real: true,
          lifecycle_status: 'connected',
          account_fingerprint: nextFingerprint,
          credentials_version: previousFingerprint && previousFingerprint !== nextFingerprint
            ? Number(conn.credentials_version || 1) + 1
            : Number(conn.credentials_version || 1),
          credentials_changed_at: previousFingerprint && previousFingerprint !== nextFingerprint
            ? new Date().toISOString()
            : conn.credentials_changed_at,
          last_tested_at: new Date().toISOString(),
        })
        .eq('id', conn.id);
      if (previousFingerprint && previousFingerprint !== nextFingerprint) {
        await upsertActiveExchangeContext(req.user.id, { ...conn, account_fingerprint: nextFingerprint, is_connected: true, is_verified_real: true }, 'credentials_changed');
      } else {
        const { connection: active } = await getActiveExchangeContext(req.user.id, { allowFallback: false });
        if (!active) await upsertActiveExchangeContext(req.user.id, { ...conn, account_fingerprint: nextFingerprint, is_connected: true, is_verified_real: true }, 'first_connection');
      }
      return res.json({
        connected: true,
        message: 'Connected to Coinbase Advanced Trade. Balance sync is active.',
        balance: balance.total_balance,
      });
    }

    const nextFingerprint = buildExchangeAccountFingerprint('binance', apiKey);
    const previousFingerprint = conn.account_fingerprint || null;
    const ts    = Date.now();
    // recvWindow=10000 gives a 10s tolerance for clock skew between server and Binance
    const query = `timestamp=${ts}&recvWindow=10000`;
    const sig   = crypto.createHmac('sha256', apiSecret).update(query).digest('hex');
    const url   = `https://fapi.binance.com/fapi/v2/balance?${query}&signature=${sig}`;
    const response = await fetch(url, {
      headers: { 'X-MBX-APIKEY': apiKey },
      signal: AbortSignal.timeout(10_000),
    });
    const data = await response.json().catch(() => ({}));

    // Binance returns -2015 for IP restriction and -2014 for bad key format
    const connected = response.ok && Array.isArray(data);
    let message = connected ? 'Connected to Binance Futures' : (data?.msg || `Binance returned HTTP ${response.status}`);

    // Translate opaque Binance errors into actionable messages
    const serverIp = await getServerPublicIp();
    if (!connected) {
      if (data?.code === -2015 || data?.code === -2014) {
        message = serverIp
          ? `IP not whitelisted. In Binance API Management → Edit Restrictions → add this IP to the whitelist: ${serverIp}`
          : "IP restriction error. Open Binance API Management → Edit Restrictions and add this server's public IP to the whitelist.";
      } else if (response.status === 401) {
        message = 'Binance rejected this API key request with HTTP 401. Check that the key/secret pair is correct, Futures permission is enabled, and any IP whitelist includes this server.';
      } else if (data?.code === -1021) {
        message = 'Timestamp error — server clock may be out of sync. Try again.';
      } else if (data?.code === -2011) {
        message = 'Enable Futures trading permission on this API key in Binance settings.';
      }
    }

    await supabase.from('exchange_connections')
      .update({
        is_connected: connected,
        is_verified_real: connected,
        lifecycle_status: connected ? 'connected' : 'disconnected',
        account_fingerprint: nextFingerprint,
        credentials_version: previousFingerprint && previousFingerprint !== nextFingerprint
          ? Number(conn.credentials_version || 1) + 1
          : Number(conn.credentials_version || 1),
        credentials_changed_at: previousFingerprint && previousFingerprint !== nextFingerprint
          ? new Date().toISOString()
          : conn.credentials_changed_at,
        last_tested_at: new Date().toISOString(),
      })
      .eq('id', conn.id);
    if (connected) {
      if (previousFingerprint && previousFingerprint !== nextFingerprint) {
        await upsertActiveExchangeContext(req.user.id, { ...conn, account_fingerprint: nextFingerprint, is_connected: true, is_verified_real: true }, 'credentials_changed');
      } else {
        const { connection: active } = await getActiveExchangeContext(req.user.id, { allowFallback: false });
        if (!active) await upsertActiveExchangeContext(req.user.id, { ...conn, account_fingerprint: nextFingerprint, is_connected: true, is_verified_real: true }, 'first_connection');
      }
    }

    res.json({ connected, message, binance_code: data?.code || null, http_status: response.status, server_ip: serverIp });
  } catch (e) {
    let msg = e?.cause?.code === 'ECONNREFUSED'
      ? 'Cannot reach Binance — check your internet connection.'
      : (e.message || 'Connection error');
    if (conn.exchange_name === 'coinbase' && e?.status === 401) {
      const serverIp = await getServerPublicIp();
      msg = serverIp
        ? `Coinbase rejected the API credentials with HTTP 401. Confirm this public IP is allowed in the Coinbase key allowlist: ${serverIp}. Also confirm the key is a Coinbase App API key with ECDSA/ES256 and portfolio View permission.`
        : 'Coinbase rejected the API credentials with HTTP 401. Confirm the key is a Coinbase App API key with ECDSA/ES256, portfolio View permission, and correct IP allowlist.';
    }
    res.json({
      connected: false,
      message: msg,
      http_status: e?.status || null,
      exchange: conn.exchange_name,
      coinbase_error: conn.exchange_name === 'coinbase' ? (e?.data || null) : undefined,
      coinbase_auth_debug: conn.exchange_name === 'coinbase' ? (e?.auth_debug || null) : undefined,
    });
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
  if (process.env.ALLOW_ADMIN_SUBSCRIPTION_GRANTS !== 'true') {
    return res.status(403).json({
      error: 'Manual subscription grants are disabled. Paid Stripe Checkout is required.',
    });
  }
  const { user_id, plan_type } = req.body;
  const plan = PLAN_CONFIG[plan_type];
  if (!plan) return res.status(400).json({ error: 'Invalid plan' });
  const expiresAt = new Date(); expiresAt.setDate(expiresAt.getDate() + plan.days);
  await supabase.from('subscriptions').update({ is_active: false }).eq('user_id', user_id).eq('is_active', true);
  const { data: sub, error } = await supabase.from('subscriptions').insert({
    user_id, plan_type, total_trades: plan.trades, remaining_trades: plan.trades,
    total_bot_creations: plan.bots, remaining_bot_creations: plan.bots,
    expires_at: expiresAt.toISOString(), is_active: true,
    payment_status: 'admin_granted',
    payment_provider: 'admin',
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
  const { connection: activeConn } = await getActiveExchangeContext(req.user.id);
  // 1. Get all open trades for this user
  let openQuery = supabase
    .from('trades')
    .select('id, bot_id, trading_pair, trade_type, entry_price, quantity, leverage, tp_price, sl_price, opened_at')
    .eq('user_id', req.user.id)
    .eq('status', 'open');
  if (activeConn?.id) openQuery = openQuery.eq('exchange_connection_id', activeConn.id);
  const { data: openTrades } = await openQuery;

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
// Fetches USDT balance directly from Binance Futures using the user's
// best verified Binance connection. No bot needs to be running.
// ══════════════════════════════════════════════════════════════════════════════

app.get('/api/user/balance', requireUser, async (req, res) => {
  res.set({
    'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
    Pragma: 'no-cache',
    Expires: '0',
    'Surrogate-Control': 'no-store',
  });

  // Get the first exchange for this user — do NOT filter by is_connected.
  // is_connected can be false due to IP restriction during the test, even
  // though keys are saved and Binance accepts them. Always try to fetch.
  const { connection: conn } = await getActiveExchangeContext(req.user.id);

  if (!conn || !conn.api_key || !conn.api_secret) {
    // No exchange connected — return zeros gracefully
    return res.json({ total_balance: 0, available_balance: 0, unrealized_pnl: 0, connected: false });
  }

  // Check cache first — avoids hammering exchange on every Dashboard render
  if (conn.exchange_name === 'coinbase') {
    try {
      const result = await getCoinbaseBalance(conn);
      return res.json({
        ...result,
        exchange_connection_id: conn.id,
        api_key_preview: `${String(conn.api_key || '').slice(0, 12)}...`,
      });
    } catch (e) {
      return res.json({
        total_balance: 0,
        available_balance: 0,
        unrealized_pnl: 0,
        connected: true,
        exchange: 'coinbase',
        balance_error: e?.message || 'Could not fetch Coinbase balance',
        exchange_connection_id: conn.id,
      });
    }
  }

  try {
    const apiKey = (conn.api_key || '').trim();
    const apiSecret = (conn.api_secret || '').trim();
    const keyPreview = apiKey ? `${apiKey.slice(0, 6)}...${apiKey.slice(-4)}` : null;
    const sourceErrors = [];
    const sourcesChecked = [];
    const sign = (query) => crypto.createHmac('sha256', apiSecret).update(query).digest('hex');
    const recordSourceError = async (source, response, fallback = 'Could not fetch balance') => {
      const err = await response.json().catch(() => ({}));
      sourceErrors.push({
        source,
        code: err?.code || response.status,
        message: err?.msg || fallback,
      });
      return err;
    };
    const stableMarginAssets = new Set(['USDT', 'USDC', 'FDUSD', 'USD1', 'BFUSD', 'RWUSD']);
    const priceCache = new Map();
    const getFuturesUsdtPrice = async (asset) => {
      if (stableMarginAssets.has(asset)) return 1;
      if (priceCache.has(asset)) return priceCache.get(asset);
      const priceResponse = await fetch(`https://fapi.binance.com/fapi/v1/ticker/price?symbol=${encodeURIComponent(`${asset}USDT`)}`, {
        signal: AbortSignal.timeout(8000),
      });
      if (!priceResponse.ok) throw new Error(`No ${asset}USDT futures ticker`);
      const priceData = await priceResponse.json();
      const price = parseFloat(priceData?.price || 0);
      if (!Number.isFinite(price) || price <= 0) throw new Error(`Invalid ${asset}USDT futures price`);
      priceCache.set(asset, price);
      return price;
    };

    const ts    = Date.now();
    const query = `timestamp=${ts}&recvWindow=10000`;
    const sig   = sign(query);
    const url   = `https://fapi.binance.com/fapi/v2/balance?${query}&signature=${sig}`;
    sourcesChecked.push('futures');

    const response = await fetch(url, {
      headers: { 'X-MBX-APIKEY': apiKey },
      signal: AbortSignal.timeout(8000),
    });

    if (!response.ok) {
      const err = await recordSourceError('futures', response, 'Could not fetch futures balance');
      const serverIp = await getServerPublicIp();
      let message = err.msg || 'Could not fetch balance';
      if (err?.code === -2015 || err?.code === -2014) {
        message = serverIp
          ? `Binance rejected the balance request. Check API permissions and whitelist this server IP: ${serverIp}`
          : 'Binance rejected the balance request. Check API permissions and IP whitelist settings.';
      } else if (err?.code === -1021) {
        message = 'Binance timestamp error. The server clock may be out of sync.';
      }
      // Keys exist but Binance rejected the call (IP restriction, clock skew, etc.).
      // Return connected:true so the UI shows the exchange as linked — just without balance.
      const result = { total_balance: 0, available_balance: 0, unrealized_pnl: 0,
                       connected: true, balance_error: message,
                       binance_code: err?.code || null, server_ip: serverIp || null,
                       balance_sources_checked: sourcesChecked,
                       balance_source_errors: sourceErrors,
                       exchange_connection_id: conn.id,
                       api_key_preview: keyPreview };
      // Continue checking Spot/Funding balances; a Futures permission issue should not hide wallet funds.
    }

    const data = response.ok ? await response.json() : [];
    let futuresTotal = 0;
    let futuresAvailable = 0;
    let futuresPnl = 0;
    const futuresAssets = [];

    if (Array.isArray(data)) {
      for (const row of data) {
        const asset = String(row?.asset || '').toUpperCase();
        const balance = parseFloat(row?.balance || 0);
        const available = parseFloat(row?.availableBalance || 0);
        const pnl = parseFloat(row?.crossUnPnl || 0);
        if (!asset || (balance === 0 && available === 0 && pnl === 0)) continue;

        try {
          const price = await getFuturesUsdtPrice(asset);
          const usdtValue = balance * price;
          const usdtAvailable = available * price;
          const usdtPnl = pnl * price;
          futuresTotal += usdtValue;
          futuresAvailable += usdtAvailable;
          futuresPnl += usdtPnl;
          futuresAssets.push({
            asset,
            balance,
            available_balance: available,
            unrealized_pnl: pnl,
            usdt_price: price,
            usdt_value: usdtValue,
            usdt_available: usdtAvailable,
          });
        } catch (e) {
          sourceErrors.push({ source: 'futures_price', code: asset, message: e?.message || `Could not price ${asset}` });
        }
      }
    }

    let spotTotal = 0;
    let spotAvailable = 0;
    sourcesChecked.push('spot');
    try {
      const spotTs = Date.now();
      const spotQuery = `timestamp=${spotTs}&recvWindow=10000`;
      const spotSig = sign(spotQuery);
      const spotResponse = await fetch(`https://api.binance.com/api/v3/account?${spotQuery}&signature=${spotSig}`, {
        headers: { 'X-MBX-APIKEY': apiKey },
        signal: AbortSignal.timeout(8000),
      });
      if (spotResponse.ok) {
        const spotData = await spotResponse.json();
        const spotUsdt = Array.isArray(spotData?.balances)
          ? spotData.balances.find(b => b.asset === 'USDT')
          : null;
        spotAvailable = parseFloat(spotUsdt?.free || 0);
        spotTotal = spotAvailable + parseFloat(spotUsdt?.locked || 0);
      } else {
        await recordSourceError('spot', spotResponse, 'Could not fetch spot balance');
      }
    } catch (e) {
      sourceErrors.push({ source: 'spot', code: 'FETCH_FAILED', message: e?.message || 'Could not fetch spot balance' });
    }

    let fundingTotal = 0;
    let fundingAvailable = 0;
    sourcesChecked.push('funding');
    try {
      const fundingTs = Date.now();
      const fundingParams = new URLSearchParams({
        asset: 'USDT',
        needBtcValuation: 'false',
        timestamp: String(fundingTs),
        recvWindow: '10000',
      });
      const fundingQuery = fundingParams.toString();
      fundingParams.set('signature', sign(fundingQuery));
      const fundingResponse = await fetch('https://api.binance.com/sapi/v1/asset/get-funding-asset', {
        method: 'POST',
        headers: {
          'X-MBX-APIKEY': apiKey,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: fundingParams.toString(),
        signal: AbortSignal.timeout(8000),
      });

      if (fundingResponse.ok) {
        const fundingData = await fundingResponse.json();
        const fundingUsdt = Array.isArray(fundingData)
          ? fundingData.find(b => b.asset === 'USDT')
          : null;
        fundingAvailable = parseFloat(fundingUsdt?.free || 0);
        fundingTotal = fundingAvailable
          + parseFloat(fundingUsdt?.locked || 0)
          + parseFloat(fundingUsdt?.freeze || 0)
          + parseFloat(fundingUsdt?.withdrawing || 0);
      } else {
        await recordSourceError('funding', fundingResponse, 'Could not fetch funding balance');
      }
    } catch (e) {
      sourceErrors.push({ source: 'funding', code: 'FETCH_FAILED', message: e?.message || 'Could not fetch funding balance' });
    }

    let balanceMessage = '';
    if (futuresTotal + spotTotal + fundingTotal === 0 && sourceErrors.length === 0) {
      balanceMessage = keyPreview
        ? `No USDT found in Futures, Spot, or Funding wallets for API key ${keyPreview}. Confirm this is the same Binance account.`
        : 'No USDT found in Futures, Spot, or Funding wallets.';
    }

    const result = {
      total_balance:             futuresTotal + spotTotal + fundingTotal,
      available_balance:         futuresAvailable + spotAvailable + fundingAvailable,
      unrealized_pnl:            futuresPnl,
      futures_balance:           futuresTotal,
      futures_available_balance: futuresAvailable,
      futures_assets:            futuresAssets,
      spot_balance:              spotTotal,
      spot_available_balance:    spotAvailable,
      funding_balance:           fundingTotal,
      funding_available_balance: fundingAvailable,
      connected:                 true,
      balance_error:             balanceMessage || '',
      balance_sources_checked:   sourcesChecked,
      balance_source_errors:     sourceErrors,
      exchange_connection_id:    conn.id,
      api_key_preview:           keyPreview,
    };

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
//   - Uses the user's Binance exchange connection
//   - Bot-authenticated version is POST /api/bot/sync — called on bot startup
//   - Does NOT open new positions — only closes ghost ones
//   - Safe to call multiple times (idempotent)
// ══════════════════════════════════════════════════════════════════════════════

async function syncTradesForUser(userId, apiKey, apiSecret, exchangeConnectionId = null) {
  // 1. Get all open trades for this user
  let openQuery = supabase
    .from('trades')
    .select('id, bot_id, trading_pair, trade_type, entry_price, tp_price, sl_price, opened_at')
    .eq('user_id', userId)
    .eq('status', 'open');
  if (exchangeConnectionId) openQuery = openQuery.eq('exchange_connection_id', exchangeConnectionId);
  const { data: openTrades } = await openQuery;

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
  const { connection: conn } = await getActiveExchangeContext(req.user.id);

  if (!conn?.api_key) {
    return res.status(400).json({ error: 'No exchange connection found. Add exchange API keys in Settings.' });
  }
  if (conn.exchange_name === 'coinbase') {
    return res.json({
      success: true,
      closed_phantom: 0,
      message: 'Coinbase balance sync is active. Position sync is not enabled for Coinbase until live Coinbase futures execution is supported.',
    });
  }

  try {
    const result = await syncTradesForUser(req.user.id, conn.api_key, conn.api_secret, conn.id);
    res.json({ success: true, ...result });
  } catch (e) {
    console.error('[sync]', e);
    res.status(500).json({ error: e.message });
  }
});

// Bot-facing sync (called by bot via bot token on startup)
app.post('/api/bot/sync', requireBotToken, async (req, res) => {
  const bot = req.bot;
  let apiKey = '', apiSecret = '', exchangeName = 'binance';
  if (bot.exchange_id) {
    const { data: ex } = await supabase.from('exchange_connections')
      .select('api_key, api_secret, exchange_name').eq('id', bot.exchange_id).single();
    apiKey = ex?.api_key || ''; apiSecret = ex?.api_secret || ''; exchangeName = ex?.exchange_name || 'binance';
  }
  if (!apiKey) return res.json({ success: true, message: 'No exchange credentials — skipping sync' });

  try {
    if (exchangeName === 'coinbase') {
      return res.json({
        success: true,
        closed_phantom: 0,
        message: 'Coinbase position sync skipped; live Coinbase futures execution is not enabled.',
      });
    }
    const result = await syncTradesForUser(bot.user_id, apiKey, apiSecret, bot.exchange_id || null);
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
  const { api_key, api_secret, exchange_connection_id } = req.body;
  if (!api_key || !api_secret) {
    return res.status(400).json({ valid: false, message: 'api_key and api_secret are required' });
  }

  const key    = api_key.trim();
  const secret = api_secret.trim();

  // ── Format checks (no network call — avoids geo-blocking on US VPS) ────────
  // Binance real API keys are 64 hex chars; secrets are also 64 chars.
  // Testnet keys are shorter (<40 chars).
  if (key.length < 40) {
    return res.json({
      valid: false, is_testnet: true, balance: 0,
      message: 'These look like testnet keys (too short). Please use real Binance API keys from binance.com → Profile → API Management.',
    });
  }
  if (key.length < 60 || secret.length < 60) {
    return res.json({
      valid: false, is_testnet: false, balance: 0,
      message: 'API key or secret looks too short. Please double-check you copied the full key.',
    });
  }

  // ── Attempt live Binance validation ─────────────────────────────────────────
  // This may fail on US-hosted servers due to geo-restrictions.
  // If it fails with a geo error, we skip validation and save the keys anyway —
  // the bot will discover bad keys immediately when it starts.
  try {
    const ts    = Date.now();
    const query = `timestamp=${ts}&recvWindow=10000`;
    const sig   = crypto.createHmac('sha256', secret).update(query).digest('hex');
    const url   = `https://fapi.binance.com/fapi/v2/balance?${query}&signature=${sig}`;

    const response = await fetch(url, {
      headers: { 'X-MBX-APIKEY': key },
      signal: AbortSignal.timeout(8_000),
    });

    const data = await response.json();

    // Geo-restriction: Binance returns 451 or specific msg
    const isGeoBlocked = response.status === 451 ||
      (data?.msg && (
        data.msg.includes('restricted location') ||
        data.msg.includes('Eligibility') ||
        data.msg.includes('unavailable')
      ));

    if (isGeoBlocked) {
      // Can't validate from this server location — save keys and trust format check
      if (exchange_connection_id) {
        await supabase.from('exchange_connections')
          .update({ is_connected: true, last_tested_at: new Date().toISOString() })
          .eq('id', exchange_connection_id).eq('user_id', req.user.id);
      }
      return res.json({
        valid: true, is_testnet: false, balance: 0,
        message: 'Keys saved. Live balance check skipped (server location restriction). The bot will verify keys on start.',
      });
    }

    if (!response.ok) {
      const isAuthError = data?.code === -2014 || data?.code === -2015;
      return res.json({
        valid: false, is_testnet: false, balance: 0,
        message: isAuthError
          ? 'Invalid API key or secret. Check credentials and ensure Futures trading is enabled.'
          : (data?.msg || 'Connection failed'),
      });
    }

    if (!Array.isArray(data)) {
      return res.json({ valid: false, balance: 0, message: 'Unexpected response from Binance' });
    }

    const usdt    = data.find(b => b.asset === 'USDT');
    const balance = parseFloat(usdt?.availableBalance || 0);

    if (exchange_connection_id) {
      await supabase.from('exchange_connections')
        .update({ is_connected: true, is_verified_real: true, last_tested_at: new Date().toISOString() })
        .eq('id', exchange_connection_id).eq('user_id', req.user.id);
    }
    return res.json({ valid: true, is_testnet: false, balance, message: 'Connected' });

  } catch (e) {
    // Network error / timeout — save keys anyway, format check passed
    if (exchange_connection_id) {
      await supabase.from('exchange_connections')
        .update({ is_connected: true, last_tested_at: new Date().toISOString() })
        .eq('id', exchange_connection_id).eq('user_id', req.user.id);
    }
    return res.json({
      valid: true, is_testnet: false, balance: 0,
      message: 'Keys saved. Could not reach Binance from this server to verify balance. The bot will confirm keys on start.',
    });
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

  // For live mode: require a verified real-key exchange connection.
  if (mode === 'live') {
    if (!bot.exchange_id) {
      return res.status(422).json({
        error: 'Connect and select a verified Binance exchange before switching to live mode.',
      });
    }
    const { data: conn } = await supabase.from('exchange_connections')
      .select('is_verified_real, exchange_name').eq('id', bot.exchange_id).single();
    if (conn?.exchange_name === 'coinbase') {
      return res.status(422).json({
        error: 'Coinbase balance sync is supported, but live Coinbase order execution is not enabled yet. Use Binance for live mode.',
      });
    }
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
  // SERVER_IP env var bypasses all HTTP fetches (set in .env when ipify is blocked)
  if (process.env.SERVER_IP) {
    _cachedServerIp = process.env.SERVER_IP;
    return _cachedServerIp;
  }

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
  let sub;
  try {
    sub = await getActivePaidSubscription(req.user.id);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
  if (!sub)  return res.json({ active: false, subscription: null });
  res.json({ active: true, subscription: sub });
});

app.get('/health', (_req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

app.listen(PORT, '0.0.0.0', () => console.log(`NexusBot API server on port ${PORT} (all interfaces)`));
