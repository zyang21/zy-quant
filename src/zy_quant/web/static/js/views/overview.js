import {
  esc,
  fmtPct,
  fmtUsd,
  humanizeSetup,
  infoTip,
  metricTile,
  scoreBar,
  statusBadge,
} from '../atoms.js';
import { EquityChart } from '../charts.js';

export class OverviewView {
  constructor(container) {
    this.container = container;
    this._mounted = false;
  }

  mount() {
    if (this._mounted) return;
    this.container.innerHTML = `
      <div class="grid-hero">
        <!-- Live Profit & Portfolio Value -->
        <section class="panel" style="margin-bottom:0;">
          <div class="panel-header">
            <div>
              <div style="display:flex;align-items:center;gap:8px;">
                <span class="panel-subtitle" id="ovNavTitle">Simulated Portfolio Net Asset Value</span>
                <span id="ovSimAccountBadge"></span>
              </div>
              <div style="display:flex;align-items:baseline;gap:14px;margin-top:4px;">
                <span class="hero-equity-val" id="ovEquity">$0.00</span>
                <span id="ovReturnBadge"></span>
              </div>
            </div>
            <div style="text-align:right;">
              <div class="panel-subtitle" id="ovProfileLabel">Capital Mandate</div>
              <div style="font-family:var(--font-mono);font-size:12.5px;margin-top:4px;" id="ovCashExposure">
                Cash $0.00 · Exposure 0.0%
              </div>
            </div>
          </div>

          <svg id="ovEquityChart" class="chart" style="width:100%;height:175px;background:#0d1117;border:1px solid var(--border-subtle);border-radius:8px;"></svg>

          <div class="kpi-grid" id="ovKpiGrid"></div>
        </section>

        <!-- Market Traffic Light & Regime Diagnosis -->
        <section class="panel" style="margin-bottom:0;display:flex;flex-direction:column;justify-content:space-between;">
          <div>
            <div class="panel-header">
              <div>
                <div class="panel-title">Market Regime & Traffic Light</div>
                <div class="panel-subtitle">Macro filter prevents buying individual stocks during broad market sell-offs</div>
              </div>
              <div id="ovRegimeBadge"></div>
            </div>

            <div style="padding:13px 14px;background:var(--bg-elevated);border:1px solid var(--border-subtle);border-radius:10px;margin-bottom:14px;">
              <div style="font-size:12.5px;font-weight:600;margin-bottom:4px;" id="ovRegimeHeadline">—</div>
              <div style="font-size:12px;color:var(--text-secondary);" id="ovRegimeGuide">—</div>
            </div>

            <div class="rule-list" id="ovMacroBenchmarks"></div>
          </div>

          <div style="margin-top:14px;padding-top:12px;border-top:1px solid var(--border-subtle);display:flex;justify-content:space-between;font-size:12px;color:var(--text-secondary);">
            <span id="ovSessionSummary">—</span>
            <span id="ovLinkedAcct">—</span>
          </div>
        </section>
      </div>

      <!-- Conservative Institutional & ETF Watchlist Ranking -->
      <section class="panel">
        <div class="panel-header">
          <div>
            <div class="panel-title">Conservative Institutional & ETF Watchlist Ranking</div>
            <div class="panel-subtitle">
              65% anchored on core S&P/Nasdaq ETF weights, institutional ownership & drawdown stability; 35% real-time block flow & insider filings
            </div>
          </div>
          <span class="txt-muted" style="font-size:12px;">Entry Threshold: Score ≥ 62.0 & Passed Veto Filters</span>
        </div>
        <div class="table-wrap">
          <table class="pro-table">
            <thead>
              <tr>
                <th>Asset</th>
                <th>Sector</th>
                <th class="num">Last Price</th>
                <th>Composite Score ${infoTip('Weighted 0–100 score. Only top-ranked stocks ≥ 62 passing liquidity & price filters are eligible for trade triggers.')}</th>
                <th class="num">ETF & Inst Core ${infoTip('Presence in flagship index ETFs (SPY/QQQ/SMH) + 13F institutional ownership stability (max 50 pts).')}</th>
                <th class="num">RS vs QQQ ${infoTip('Relative Strength: Stock daily return minus QQQ return. Positive values during market dips highlight institutional support.')}</th>
                <th class="num">Block Flow ${infoTip('Institutional Super-Large + Large order net inflow score (10 = neutral, >10 = net accumulation, max 20 pts).')}</th>
                <th>Eligibility</th>
              </tr>
            </thead>
            <tbody id="ovWatchlistBody"></tbody>
          </table>
        </div>
      </section>

      <!-- Open Positions -->
      <section class="panel" style="margin-bottom:0;">
        <div class="panel-header">
          <div>
            <div class="panel-title">Positions</div>
            <div class="panel-subtitle">Active holdings protected by volatility-scaled stops (ATR), breakeven ratchets, and time barriers</div>
          </div>
          <span class="txt-muted" id="ovPosCount" style="font-size:12px;">0 Open</span>
        </div>
        <div class="table-wrap">
          <table class="pro-table">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Entry Signal</th>
                <th class="num">Qty</th>
                <th class="num">Entry → Mark</th>
                <th class="num">Market Value</th>
                <th class="num">Unrealized PnL</th>
                <th class="num">R-Multiple ${infoTip('Profit/Loss measured in units of initial risk (1R = Entry minus Initial Stop). At +1.5R, stop moves above breakeven.')}</th>
                <th class="num">Protective Stop ${infoTip('Ratchets strictly upward as price makes new highs (Chandelier Exit); never moves down.')}</th>
                <th class="num">Hold Time</th>
                <th>Protection State</th>
              </tr>
            </thead>
            <tbody id="ovPositionsBody"></tbody>
          </table>
        </div>
      </section>
    `;
    this._mounted = true;
  }

  render(data) {
    this.mount();
    const p = data.portfolio || {};
    const reg = data.regime || {};
    const ms = data.market_session || {};
    const conn = data.connection || {};

    const isSim = conn.is_simulated !== false;
    document.getElementById('ovNavTitle').textContent = isSim
      ? 'Simulated Portfolio Value (Virtual Paper Funds)'
      : 'Live Brokerage Portfolio Net Asset Value';
    document.getElementById('ovSimAccountBadge').innerHTML = isSim
      ? statusBadge('VIRTUAL PAPER ACCOUNT', 'warn', false)
      : statusBadge('REAL CAPITAL', 'up', false);

    // Portfolio Value & Equity Chart
    document.getElementById('ovEquity').textContent = fmtUsd(p.equity_usd);
    const netRet = Number(p.net_return_pct || 0);
    document.getElementById('ovReturnBadge').innerHTML = statusBadge(
      `${fmtPct(netRet)} (${fmtUsd(p.net_pnl_usd, true)})`,
      netRet >= 0 ? 'up' : 'down',
      false
    );
    document.getElementById('ovProfileLabel').textContent = p.profile_label || 'Active Mandate';
    document.getElementById('ovCashExposure').textContent =
      `Cash ${fmtUsd(p.cash_usd)} · Invested ${Number(p.exposure_pct || 0).toFixed(1)}%`;

    EquityChart.render(
      document.getElementById('ovEquityChart'),
      p.equity_curve || [],
      p.starting_equity_usd || 10000,
      175
    );

    const gross = Number(p.gross_pnl_usd || 0);
    document.getElementById('ovKpiGrid').innerHTML = [
      metricTile({
        label: 'Net Profit / Loss',
        value: fmtUsd(p.net_pnl_usd, true),
        valueClass: p.net_pnl_usd >= 0 ? 'txt-up' : 'txt-down',
        hint: `Start Capital ${fmtUsd(p.starting_equity_usd)}`,
        tooltip: 'Total realized + unrealized profit after deducting all Futu commissions, platform fees, settlement fees, and spread slippage.',
      }),
      metricTile({
        label: 'Gross Profit',
        value: fmtUsd(gross, true),
        valueClass: gross >= 0 ? 'txt-up' : 'txt-down',
        hint: `Unrealized ${fmtUsd(p.unrealized_pnl_usd, true)}`,
        tooltip: 'Raw price-movement profit before broker transaction costs.',
      }),
      metricTile({
        label: 'Fees & Slippage',
        value: `-${fmtUsd(p.total_fees_usd)}`,
        valueClass: 'txt-down',
        hint: '2026 Futu US Schedule',
        tooltip: 'Cumulative broker commissions + platform fees + settlement/SEC fees + estimated bid-ask spread impact.',
      }),
      metricTile({
        label: 'Win Rate',
        value: `${Number(p.win_rate_pct || 0).toFixed(1)}%`,
        hint: `${p.closed_trades_count || 0} Closed Trades`,
        tooltip: 'Percentage of closed trades with net profit > $0 after all fees.',
      }),
      metricTile({
        label: 'Max Drawdown',
        value: `-${Number(p.max_drawdown_pct || 0).toFixed(2)}%`,
        valueClass: p.max_drawdown_pct > 2 ? 'txt-down' : '',
        hint: 'Peak-to-trough drop',
        tooltip: 'Largest percentage drop from a portfolio peak. Measures worst-case historical pain.',
      }),
      metricTile({
        label: 'Portfolio Heat',
        value: `${Number(p.principal_heat_pct || 0).toFixed(2)}% / ${Number(p.max_portfolio_heat_pct || 6).toFixed(1)}%`,
        valueClass: Number(p.principal_heat_pct || 0) >= Number(p.max_portfolio_heat_pct || 6) * 0.85 ? 'txt-warn' : 'txt-up',
        hint: `${fmtUsd(p.principal_heat_usd || 0)} Principal · ${p.pipeline_phase || 'IDLE'}`,
        tooltip: 'Total open principal at risk to current stops (drops to 0% per position once breakeven stop is armed). Includes 60d pairwise correlation & StoplossGuard gates.',
      }),
    ].join('');

    // Market Regime Traffic Light
    const light = reg.light || 'YELLOW';
    const lightMap = {
      GREEN: { variant: 'up', label: 'GREEN · BULLISH TREND', guide: 'Broad Nasdaq (QQQ) is holding above both today’s volume-weighted average price (VWAP) and its 20-day trend with calm volatility. Intraday buy triggers are unlocked.' },
      YELLOW: { variant: 'warn', label: 'YELLOW · CONSOLIDATION', guide: 'Market signals are mixed (QQQ split between intraday VWAP and 20-day trend, or moderate volatility). New buys are paused; existing positions remain protected.' },
      RED: { variant: 'down', label: 'RED · DEFENSIVE LOCK', guide: 'Elevated market volatility or index downtrend detected. All new buy orders are locked ($0 fee churn) while the engine tracks relative-strength leaders.' },
    };
    const lm = lightMap[light] || lightMap.YELLOW;
    document.getElementById('ovRegimeBadge').innerHTML = statusBadge(lm.label, lm.variant);
    document.getElementById('ovRegimeHeadline').textContent = reg.reason || 'Evaluating index conditions...';
    document.getElementById('ovRegimeGuide').textContent = lm.guide;

    const qqqVsVwap = reg.qqq_price && reg.qqq_vwap ? (((reg.qqq_price - reg.qqq_vwap) / reg.qqq_vwap) * 100) : 0;
    const qqqVsSma = reg.qqq_price && reg.qqq_sma20 ? (((reg.qqq_price - reg.qqq_sma20) / reg.qqq_sma20) * 100) : 0;

    document.getElementById('ovMacroBenchmarks').innerHTML = `
      <div class="rule-item">
        <div class="rk">QQQ vs Intraday VWAP ${infoTip('VWAP is the institutional average price paid today weighted by volume. Trading above VWAP indicates buyers are in control intraday.')}</div>
        <div class="rv ${qqqVsVwap >= 0 ? 'txt-up' : 'txt-down'}">$${reg.qqq_price ?? '—'} vs $${reg.qqq_vwap ?? '—'} (${fmtPct(qqqVsVwap)})</div>
      </div>
      <div class="rule-item">
        <div class="rk">QQQ vs 20-Day Trend ${infoTip('20-Day Simple Moving Average represents the ~1-month institutional trend baseline.')}</div>
        <div class="rv ${qqqVsSma >= 0 ? 'txt-up' : 'txt-down'}">SMA20 $${reg.qqq_sma20 ?? '—'} (${fmtPct(qqqVsSma)})</div>
      </div>
      <div class="rule-item">
        <div class="rk">Market Volatility Index ${infoTip('Annualized volatility gauge. Above 24 signals caution (Yellow); above 30 signals panic whipsaw risk (Red).')}</div>
        <div class="rv ${reg.vix_level >= 30 ? 'txt-down' : (reg.vix_level >= 24 ? 'txt-warn' : 'txt-up')}">${reg.vix_level ?? '—'} ${reg.vix_level >= 30 ? '(Elevated Panic)' : '(Normal)'}</div>
      </div>
      <div class="rule-item">
        <div class="rk">S&P 500 & Breadth</div>
        <div class="rv">$${reg.spy_price ?? '—'} · Breadth >SMA50: ${reg.breadth_above_sma50_pct != null ? `${reg.breadth_above_sma50_pct}%` : '—'}</div>
      </div>
      <div class="rule-item">
        <div class="rk">Pipeline & Risk Guards</div>
        <div class="rv">${esc(p.pipeline_phase || 'IDLE')} · Corr ≤ ${p.max_pair_corr_limit ?? 0.75} · ${(p.stoploss_guard && p.stoploss_guard.active) ? `Cooldown → ${esc(p.stoploss_guard.cooldown_until_date)}` : 'StoplossGuard OK'}</div>
      </div>
    `;

    document.getElementById('ovSessionSummary').textContent = ms.message || '';
    document.getElementById('ovLinkedAcct').textContent = isSim
      ? 'Execution Mode: Paper Simulation (Real Brokerage Funds Isolated)'
      : `Live Brokerage Account NAV: ${fmtUsd(conn.linked_broker_equity_usd)}`;

    // Watchlist Table
    const wl = data.watchlist || [];
    document.getElementById('ovWatchlistBody').innerHTML = wl
      .map((r) => {
        const rs = Number(r.rs_vs_qqq || 0);
        const corePts = Number(r.etf_score || 0) + Number(r.inst_score || 0);
        const eligible = Boolean(r.passed_hard_filters) && Number(r.total_score) >= 62;
        return `
          <tr>
            <td><strong>${esc(r.symbol)}</strong></td>
            <td class="txt-muted">${esc(r.sector)}</td>
            <td class="num">${fmtUsd(r.price)}</td>
            <td>${scoreBar(r.total_score)}</td>
            <td class="num">${corePts.toFixed(1)} <span class="txt-muted">/ 50</span></td>
            <td class="num ${rs >= 0 ? 'txt-up' : 'txt-down'}">${fmtPct(rs)}</td>
            <td class="num">${Number(r.big_flow_score || 0).toFixed(1)} <span class="txt-muted">/ 20</span></td>
            <td>${
              eligible
                ? statusBadge('Eligible Candidate', 'up', false)
                : statusBadge(r.filter_reason === 'PASS' ? 'Below Score 62' : r.filter_reason, 'neutral', false)
            }</td>
          </tr>
        `;
      })
      .join('');

    // Open Positions Table
    const positions = data.positions || [];
    document.getElementById('ovPosCount').textContent = `${positions.length} Open`;
    const posBody = document.getElementById('ovPositionsBody');
    if (!positions.length) {
      posBody.innerHTML = `
        <tr>
          <td colspan="10" style="text-align:center;padding:24px;color:var(--text-secondary);">
            No open positions. Capital is held in cash until Market Regime is GREEN and an eligible leader triggers a 15m entry setup.
          </td>
        </tr>
      `;
    } else {
      posBody.innerHTML = positions
        .map((pos) => {
          const upnl = Number(pos.unrealized_pnl_usd || 0);
          const cls = upnl >= 0 ? 'txt-up' : 'txt-down';
          const stopTag = pos.stop_order_type || 'GTC_STOP';
          const gtcBadge = pos.gtc_stop_healthy === false
            ? 'STOP UNARMED'
            : (pos.breakeven_armed ? `${stopTag} · Breakeven+` : `${stopTag} Armed`);
          const earnHint = pos.earnings_countdown_td != null
            ? ` · Earn T-${pos.earnings_countdown_td}d (${pos.earnings_timing || 'UNK'})`
            : '';
          const holdDisplay = pos.days_held > 0
            ? `${pos.days_held}d${earnHint}`
            : `${pos.hold_hours}h (${pos.bars_held_15m}b)${earnHint}`;
          return `
            <tr>
              <td><strong>${esc(pos.symbol)}</strong> <span class="txt-muted" style="font-size:11px;">${esc(pos.sector)}</span></td>
              <td>${esc(humanizeSetup(pos.setup))}</td>
              <td class="num">${pos.shares}</td>
              <td class="num">${fmtUsd(pos.entry_price)} → <strong>${fmtUsd(pos.mark_price)}</strong></td>
              <td class="num">${fmtUsd(pos.market_value_usd)}</td>
              <td class="num ${cls}">${fmtUsd(upnl, true)} (${fmtPct(pos.unrealized_pnl_pct)})</td>
              <td class="num ${pos.current_r >= 0 ? 'txt-up' : 'txt-down'}">${pos.current_r >= 0 ? '+' : ''}${Number(pos.current_r).toFixed(2)}R</td>
              <td class="num txt-warn">${fmtUsd(pos.current_stop)}</td>
              <td class="num">${esc(holdDisplay)}</td>
              <td>${statusBadge(gtcBadge, pos.gtc_stop_healthy === false ? 'down' : (pos.breakeven_armed ? 'up' : 'neutral'), false)}</td>
            </tr>
          `;
        })
        .join('');
    }
  }
}
