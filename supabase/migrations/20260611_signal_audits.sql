-- Signal audit system for accepted and rejected trading signals.
-- Run this in Supabase SQL editor before enabling production signal-audit reporting.

create extension if not exists pgcrypto;

create table if not exists public.signal_audits (
  id uuid primary key default gen_random_uuid(),
  audit_id uuid unique,
  bot_id uuid references public.bots(id) on delete cascade,
  user_id uuid not null,
  exchange_name text default 'binance',
  exchange_connection_id uuid null,
  exchange_account_fingerprint text null,
  symbol text not null,
  market_type text not null default 'futures',
  side text null check (side in ('long', 'short') or side is null),
  entry_price numeric(24, 10) null,
  confidence numeric(10, 6) null,
  regime text null,
  adx numeric(12, 6) null,
  atr_ratio numeric(12, 6) null,
  tech_signal text null,
  ml_signal text null,
  hybrid_signal text null,
  blocked_reason text not null default 'UNKNOWN',
  evaluated_at timestamptz not null default now(),

  horizon_5_mfe numeric(14, 6) null,
  horizon_5_mae numeric(14, 6) null,
  horizon_5_virtual_pnl_r numeric(14, 6) null,
  horizon_5_virtual_win boolean null,
  horizon_5_virtual_loss boolean null,

  horizon_10_mfe numeric(14, 6) null,
  horizon_10_mae numeric(14, 6) null,
  horizon_10_virtual_pnl_r numeric(14, 6) null,
  horizon_10_virtual_win boolean null,
  horizon_10_virtual_loss boolean null,

  horizon_20_mfe numeric(14, 6) null,
  horizon_20_mae numeric(14, 6) null,
  horizon_20_virtual_pnl_r numeric(14, 6) null,
  horizon_20_virtual_win boolean null,
  horizon_20_virtual_loss boolean null,

  horizon_50_mfe numeric(14, 6) null,
  horizon_50_mae numeric(14, 6) null,
  horizon_50_virtual_pnl_r numeric(14, 6) null,
  horizon_50_virtual_win boolean null,
  horizon_50_virtual_loss boolean null,

  scored_at timestamptz null,
  created_at timestamptz not null default now()
);

create index if not exists idx_signal_audits_bot_time
  on public.signal_audits (bot_id, evaluated_at desc);

create index if not exists idx_signal_audits_user_time
  on public.signal_audits (user_id, evaluated_at desc);

create index if not exists idx_signal_audits_filter
  on public.signal_audits (blocked_reason, market_type, symbol);

create index if not exists idx_signal_audits_confidence
  on public.signal_audits (confidence, blocked_reason);

create index if not exists idx_signal_audits_scoring
  on public.signal_audits (symbol, market_type, evaluated_at)
  where scored_at is null and side is not null and entry_price is not null;

alter table public.signal_audits enable row level security;

drop policy if exists "Users can read own signal audits" on public.signal_audits;
create policy "Users can read own signal audits"
  on public.signal_audits for select
  using (auth.uid() = user_id);

-- Inserts are made by the Node server using the service-role key.

