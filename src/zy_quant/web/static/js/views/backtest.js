import {
  esc,
  fmtPct,
  fmtUsd,
  humanizeExitReason,
  humanizeSetup,
  metricTile,
  statusBadge,
} from '../atoms.js';
import { EquityChart } from '../charts.js';

export class BacktestLabView {
  constructor(container, api) {
    this.container = container;
    this.api = api;
    this._mounted = false;
    this.days = 30;
  }

  mount(strategySummary = {}) {
    if (this._mounted) return;
    this.container.innerHTML = `
      <!-- Active Strategy Blueprint -->
      <section class="panel">
        <div class="panel-header">
          <div>
            <div class="panel-title">Active Buy / Sell Strategy Blueprint</div>
            <div class="panel-subtitle">All parameters are loaded from configuration files and shared identically between live execution and historical simulation</div>
          </div>
          <span class="badge badge-neutral" id="btProfileBadge">—</span>
        </div>
        <div class="rule-list" id="btRuleGrid"></div>
      </section>

      <!-- Simulation Controls & Results -->
      <section class="panel">
        <div class="panel-header">
          <div>
            <div class="panel-title">Historical Strategy Backtest</div>
            <div class="panel-subtitle">Dual-Clock replay (1d Primary Clock &amp; 15:45 ET Causal Hybrid Clock) with exact 2026 Futu HK fee floors and slippage</div>
          </div>
          <span class="txt-muted" id="btWindowLabel" style="font-family:var(--font-mono);font-size:12px;">—</span>
        </div>

        <div class="filter-bar">
          <span class="txt-muted" style="font-size:12px;">Lookback Window:</span>
          <div class="preset-group" id="btDayPresets">
            <button class="preset-btn" data-days="14">14D</button>
            <button class="preset-btn active" data-days="30">30D</button>
            <button class="preset-btn" data-days="55">55D</button>
            <button class="preset-btn" data-days="180">180D</button>
            <button class="preset-btn" data-days="365">1Y</button>
          </div>

          <span class="txt-muted" style="font-size:12px;margin-left:6px;">Custom Period:</span>
          <input type="date" id="btStart" />
          <span class="txt-muted">→</span>
          <input type="date" id="btEnd" />

          <input type="text" id="btSymbols" placeholder="Symbols (optional: NVDA,MSFT,META)" style="width:210px;" />
          <select id="btClockMode" title="Simulation Clock Mode">
            <option value="auto">Clock: Auto (1d / Hybrid)</option>
            <option value="1d">Swing 1d Primary Clock</option>
            <option value="hybrid_1545">Swing 1d + 15:45 ET Hybrid</option>
            <option value="intraday_15m">Legacy Intraday 15m</option>
          </select>
          <button class="btn btn-accent" id="btRunBtn">Run Backtest</button>
        </div>

        <div id="btDataIntegrityBanner" style="display:none;padding:10px 14px;margin-bottom:12px;border-radius:8px;background:rgba(246,70,93,0.14);border:1px solid rgba(246,70,93,0.45);color:#ff8093;font-size:12px;font-weight:600;"></div>

        <div class="kpi-grid" id="btKpis" style="margin-bottom:14px;"></div>

        <svg id="btEquityChart" class="chart" style="width:100%;height:220px;background:#0d1117;border:1px solid var(--border-subtle);border-radius:8px;margin-bottom:16px;"></svg>

        <div class="panel-header" style="margin-bottom:8px;">
          <div class="panel-title" style="font-size:13.5px;">Simulated Trade Breakdown</div>
          <span class="txt-muted" id="btRegimeStats" style="font-size:12px;"></span>
        </div>
        <div class="table-wrap">
          <table class="pro-table">
            <thead>
              <tr>
                <th>Exit Time (UTC)</th>
                <th>Symbol</th>
                <th>Trigger Setup</th>
                <th class="num">Shares</th>
                <th class="num">Entry → Exit</th>
                <th class="num">Gross PnL</th>
                <th class="num">Futu Fee+Slip</th>
                <th class="num">Net PnL</th>
                <th class="num">R-Multiple</th>
                <th>Exit Rule</th>
              </tr>
            </thead>
            <tbody id="btTradesBody"></tbody>
          </table>
        </div>
      </section>
    `;

    this.container.querySelectorAll('#btDayPresets .preset-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        this.days = parseInt(btn.dataset.days, 10);
        document.getElementById('btStart').value = '';
        document.getElementById('btEnd').value = '';
        this.container
          .querySelectorAll('#btDayPresets .preset-btn')
          .forEach((b) => b.classList.toggle('active', b === btn));
        this.run();
      });
    });

    document.getElementById('btRunBtn').addEventListener('click', () => this.run());
    this._mounted = true;
    this.renderBlueprint(strategySummary);
  }

  renderBlueprint(s = {}) {
    if (!this._mounted || !s.profile) return;
    document.getElementById('btProfileBadge').textContent = s.profile;
    document.getElementById('btRuleGrid').innerHTML = `
      <div class="rule-item">
        <div class="rk">Entry Triggers (${esc(s.timeframe || '15m')})</div>
        <div class="rv">${esc((s.entry_setups || []).join(' · '))}</div>
      </div>
      <div class="rule-item">
        <div class="rk">Initial Downside Protection</div>
        <div class="rv">${esc(s.initial_stop_rule || '')}</div>
      </div>
      <div class="rule-item">
        <div class="rk">Profit Locking Formula</div>
        <div class="rv">${esc(s.profit_protection || '')}</div>
      </div>
      <div class="rule-item">
        <div class="rk">Time & Expectancy Gate</div>
        <div class="rv">${esc(s.time_barrier || '')} · ≥ ${esc(s.min_expectancy || '')}</div>
      </div>
    `;
  }

  async run() {
    this.mount();
    const btn = document.getElementById('btRunBtn');
    btn.disabled = true;
    btn.textContent = 'Simulating...';
    try {
      const startDate = document.getElementById('btStart').value || null;
      const endDate = document.getElementById('btEnd').value || null;
      const symRaw = document.getElementById('btSymbols').value.trim();
      const symbols = symRaw
        ? symRaw.split(',').map((s) => s.trim().toUpperCase()).filter(Boolean)
        : null;

      const clockEl = document.getElementById('btClockMode');
      const clockMode = clockEl ? clockEl.value : 'auto';
      const bt = await this.api.runBacktest({
        days: this.days,
        startDate,
        endDate,
        symbols,
        clockMode,
      });

      if (bt.strategy_summary) this.renderBlueprint(bt.strategy_summary);
      const sourceNames = {
        ALPACA_MARKET_DATA: 'Verified Alpaca Historical Bars (Split-Adjusted)',
        LOCAL_BAR_CACHE: 'Verified Local SQLite Bar Cache',
        FUTU_REALTIME: 'Verified Futu Real Historical Bars',
      };
      const isVerifiedSource = Boolean(sourceNames[bt.data_source]);
      const srcLabel = `${sourceNames[bt.data_source] || bt.data_source} [${bt.clock_mode || clockMode}]`;
      document.getElementById('btWindowLabel').textContent =
        bt.period_start && bt.period_end
          ? `${srcLabel} · ${bt.period_start} → ${bt.period_end}`
          : srcLabel;

      const warnEl = document.getElementById('btDataIntegrityBanner');
      if (bt.is_synthetic_data || !isVerifiedSource || (bt.data_warnings && bt.data_warnings.length)) {
        warnEl.style.display = 'block';
        const details = (bt.data_warnings || []).join(' | ') || `Data source degraded: ${bt.data_source}`;
        warnEl.textContent = `DATA FALLBACK / INTEGRITY WARNING (${bt.data_source}): ${details}`;
      } else {
        warnEl.style.display = 'none';
      }

      const totalBars = (bt.green_bars || 0) + (bt.rest_bars || 0);
      const greenPct = totalBars > 0 ? ((bt.green_bars / totalBars) * 100).toFixed(0) : 0;
      document.getElementById('btRegimeStats').textContent =
        `Regime Exposure: ${bt.green_bars} Bullish Bars (${greenPct}%) · ${bt.rest_bars} Defensive Cash Bars`;

      document.getElementById('btKpis').innerHTML = [
        metricTile({
          label: 'Net Return',
          value: fmtPct(bt.net_return_pct),
          valueClass: bt.net_return_pct >= 0 ? 'txt-up' : 'txt-down',
          hint: `${fmtUsd(bt.initial_equity_usd)} → ${fmtUsd(bt.final_equity_usd)}`,
          tooltip: 'Total percentage return after subtracting realistic Futu fees and spread slippage.',
        }),
        metricTile({
          label: 'Net Profit / Loss',
          value: fmtUsd(bt.net_pnl_usd, true),
          valueClass: bt.net_pnl_usd >= 0 ? 'txt-up' : 'txt-down',
          hint: `Gross ${fmtUsd(bt.gross_pnl_usd, true)}`,
        }),
        metricTile({
          label: 'Futu Fee Drag',
          value: `-${fmtUsd(bt.total_fees_usd)}`,
          valueClass: 'txt-down',
          hint: `${Number(bt.fee_drag_of_gross_pct || 0).toFixed(1)}% of Gross`,
          tooltip: 'Total transaction friction paid. Keeping fee drag low is critical for short-term profitability.',
        }),
        metricTile({
          label: 'Win Rate & Count',
          value: `${Number(bt.win_rate_pct || 0).toFixed(1)}%`,
          hint: `${bt.total_closed_trades || 0} Round-Trip Trades`,
        }),
        metricTile({
          label: 'Profit Factor',
          value: Number(bt.profit_factor || 0).toFixed(2),
          valueClass: bt.profit_factor >= 1.5 ? 'txt-up' : '',
          hint: `Avg ${bt.avg_r_multiple >= 0 ? '+' : ''}${Number(bt.avg_r_multiple || 0).toFixed(2)}R / trade`,
        }),
        metricTile({
          label: 'Max Drawdown',
          value: `-${Number(bt.max_drawdown_pct || 0).toFixed(2)}%`,
          valueClass: bt.max_drawdown_pct > 4 ? 'txt-down' : '',
          hint: 'Worst peak-to-valley dip',
        }),
      ].join('');

      EquityChart.render(
        document.getElementById('btEquityChart'),
        bt.equity_curve || [],
        bt.initial_equity_usd,
        220
      );

      const trades = (bt.trades || []).slice().reverse();
      const tbody = document.getElementById('btTradesBody');
      if (!trades.length) {
        tbody.innerHTML = `
          <tr>
            <td colspan="10" style="text-align:center;padding:28px;color:var(--text-secondary);">
              0 trades triggered in this window — Market Regime filter or Expectancy-to-Fee gate kept 100% of capital safely in cash.
            </td>
          </tr>
        `;
      } else {
        tbody.innerHTML = trades
          .map((t) => {
            const net = Number(t.net_pnl_usd || 0);
            return `
              <tr>
                <td style="font-family:var(--font-mono);color:var(--text-secondary);">${esc(String(t.exit_time || '').slice(0, 16).replace('T', ' '))}</td>
                <td><strong>${esc(t.symbol)}</strong></td>
                <td>${esc(humanizeSetup(t.setup))}</td>
                <td class="num">${t.shares}</td>
                <td class="num">${fmtUsd(t.entry_price)} → ${fmtUsd(t.exit_price)}</td>
                <td class="num">${fmtUsd(t.gross_pnl_usd, true)}</td>
                <td class="num txt-down">-${fmtUsd(t.fee_usd)}</td>
                <td class="num ${net >= 0 ? 'txt-up' : 'txt-down'}"><strong>${fmtUsd(net, true)}</strong></td>
                <td class="num ${t.r_multiple >= 0 ? 'txt-up' : 'txt-down'}">${t.r_multiple >= 0 ? '+' : ''}${Number(t.r_multiple).toFixed(2)}R</td>
                <td>${statusBadge(humanizeExitReason(t.exit_reason), net >= 0 ? 'up' : 'neutral', false)}</td>
              </tr>
            `;
          })
          .join('');
      }
    } finally {
      btn.disabled = false;
      btn.textContent = 'Run Backtest';
    }
  }
}
