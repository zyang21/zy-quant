import {
  esc,
  fmtUsd,
  humanizeExitReason,
  humanizeSetup,
  metricTile,
  statusBadge,
} from '../atoms.js';

export class TradeHistoryView {
  constructor(container, api) {
    this.container = container;
    this.api = api;
    this._mounted = false;
    this.state = {
      preset: '30D',
      startDate: '',
      endDate: '',
      symbol: '',
      exitReason: '',
      subTab: 'trades', // 'trades' | 'orders'
    };
    this.lastData = null;
  }

  mount() {
    if (this._mounted) return;
    this.container.innerHTML = `
      <section class="panel">
        <div class="panel-header">
          <div>
            <div class="panel-title">Trade History & Execution Ledger</div>
            <div class="panel-subtitle">Query realized round-trip trades, fee impact, and raw order audit records across any date window</div>
          </div>
          <div class="preset-group" id="histSubTabs">
            <button class="preset-btn active" data-sub="trades">Closed Trades</button>
            <button class="preset-btn" data-sub="orders">Order Audit Journal</button>
          </div>
        </div>

        <div class="filter-bar">
          <div class="preset-group" id="histPresets">
            ${['24H', '7D', '30D', '90D', 'YTD', 'ALL']
              .map(
                (p) =>
                  `<button class="preset-btn ${p === this.state.preset ? 'active' : ''}" data-preset="${p}">${p}</button>`
              )
              .join('')}
          </div>

          <label class="txt-muted" style="font-size:12px;">From</label>
          <input type="date" id="histStart" />
          <label class="txt-muted" style="font-size:12px;">To</label>
          <input type="date" id="histEnd" />

          <input type="text" id="histSymbol" placeholder="Symbol (e.g. NVDA)" style="width:140px;" />

          <select id="histReason">
            <option value="">All Exit Rules</option>
            <option value="CHANDELIER_TRAIL">Chandelier Trailing Profit</option>
            <option value="SCALE_OUT_2R">Scale-Out Profit Lock</option>
            <option value="EOD_T0_FLATTEN">Futu HK T+0 EOD Shield</option>
            <option value="BREAKEVEN_STOP">Breakeven Guard</option>
            <option value="INITIAL_ATR_STOP">Initial ATR Stop</option>
            <option value="TIME_BARRIER">4h Time Barrier</option>
          </select>

          <button class="btn btn-accent" id="histQueryBtn">Query Records</button>
          <button class="btn" id="histResetBtn">Reset</button>
        </div>

        <div class="kpi-grid" id="histKpis" style="margin-bottom:16px;"></div>

        <div class="table-wrap" id="histTableContainer"></div>
      </section>
    `;

    this.container.querySelectorAll('#histPresets .preset-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        this.state.preset = btn.dataset.preset;
        this.state.startDate = '';
        this.state.endDate = '';
        document.getElementById('histStart').value = '';
        document.getElementById('histEnd').value = '';
        this._syncPresetButtons();
        this.refresh();
      });
    });

    this.container.querySelectorAll('#histSubTabs .preset-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        this.state.subTab = btn.dataset.sub;
        this.container
          .querySelectorAll('#histSubTabs .preset-btn')
          .forEach((b) => b.classList.toggle('active', b.dataset.sub === this.state.subTab));
        if (this.lastData) this._renderTable(this.lastData);
      });
    });

    document.getElementById('histQueryBtn').addEventListener('click', () => {
      this.state.startDate = document.getElementById('histStart').value;
      this.state.endDate = document.getElementById('histEnd').value;
      this.state.symbol = document.getElementById('histSymbol').value.trim().toUpperCase();
      this.state.exitReason = document.getElementById('histReason').value;
      if (this.state.startDate || this.state.endDate) {
        this.state.preset = '';
        this._syncPresetButtons();
      }
      this.refresh();
    });

    document.getElementById('histResetBtn').addEventListener('click', () => {
      this.state = { ...this.state, preset: 'ALL', startDate: '', endDate: '', symbol: '', exitReason: '' };
      document.getElementById('histStart').value = '';
      document.getElementById('histEnd').value = '';
      document.getElementById('histSymbol').value = '';
      document.getElementById('histReason').value = '';
      this._syncPresetButtons();
      this.refresh();
    });

    this._mounted = true;
  }

  _syncPresetButtons() {
    this.container.querySelectorAll('#histPresets .preset-btn').forEach((b) => {
      b.classList.toggle('active', b.dataset.preset === this.state.preset);
    });
  }

  async refresh() {
    this.mount();
    const data = await this.api.getHistory(this.state);
    this.lastData = data;
    const s = data.summary || {};

    document.getElementById('histKpis').innerHTML = [
      metricTile({
        label: 'Queried Net PnL',
        value: fmtUsd(s.net_pnl_usd, true),
        valueClass: s.net_pnl_usd >= 0 ? 'txt-up' : 'txt-down',
        hint: `${s.trades_count || 0} Closed Trades`,
        tooltip: 'Net profit/loss over the selected date range after all Futu fees.',
      }),
      metricTile({
        label: 'Gross PnL',
        value: fmtUsd(s.gross_pnl_usd, true),
        valueClass: s.gross_pnl_usd >= 0 ? 'txt-up' : 'txt-down',
        hint: 'Pre-fee price gain',
      }),
      metricTile({
        label: 'Period Fees',
        value: `-${fmtUsd(s.fees_usd)}`,
        valueClass: 'txt-down',
        hint: `${s.orders_count || 0} Order Executions`,
      }),
      metricTile({
        label: 'Win Rate',
        value: `${Number(s.win_rate_pct || 0).toFixed(1)}%`,
        hint: 'Profitable round-trips',
      }),
      metricTile({
        label: 'Profit Factor',
        value: Number(s.profit_factor || 0).toFixed(2),
        valueClass: s.profit_factor >= 1.5 ? 'txt-up' : '',
        hint: 'Gross Wins ÷ Gross Losses',
        tooltip: 'Ratio of total winning dollars to total losing dollars. > 1.5 indicates strong edge.',
      }),
      metricTile({
        label: 'Avg R-Multiple',
        value: `${s.avg_r_multiple >= 0 ? '+' : ''}${Number(s.avg_r_multiple || 0).toFixed(2)}R`,
        valueClass: s.avg_r_multiple >= 0 ? 'txt-up' : 'txt-down',
        hint: 'Avg return per unit of risk',
        tooltip: 'Average payoff relative to initial stop-loss risk. Positive R means winners outweigh losers.',
      }),
    ].join('');

    this._renderTable(data);
  }

  _renderTable(data) {
    const wrap = document.getElementById('histTableContainer');
    if (this.state.subTab === 'orders') {
      const orders = data.orders || [];
      if (!orders.length) {
        wrap.innerHTML = `<div style="text-align:center;padding:32px;color:var(--text-secondary);">No order executions found in the selected time range.</div>`;
        return;
      }
      wrap.innerHTML = `
        <table class="pro-table">
          <thead>
            <tr>
              <th>Timestamp (UTC)</th>
              <th>Symbol</th>
              <th>Side</th>
              <th class="num">Shares</th>
              <th class="num">Fill Price</th>
              <th class="num">Friction Fee</th>
              <th>Status</th>
              <th>Signal / Exit Rationale</th>
            </tr>
          </thead>
          <tbody>
            ${orders
              .map(
                (o) => `
              <tr>
                <td style="font-family:var(--font-mono);color:var(--text-secondary);">${esc(String(o.created_at || '').slice(0, 19).replace('T', ' '))}</td>
                <td><strong>${esc(o.symbol)}</strong></td>
                <td>${statusBadge(o.side, o.side === 'BUY' ? 'up' : 'warn', false)}</td>
                <td class="num">${Number(o.filled_shares || 0) > 0 && Number(o.filled_shares) < Number(o.shares) ? `${o.filled_shares} / ${o.shares}` : o.shares}</td>
                <td class="num">${fmtUsd(o.avg_fill_price || o.fill_price || o.limit_price)}</td>
                <td class="num txt-down">-${fmtUsd(o.fee_usd)}</td>
                <td>${statusBadge(o.status, o.status === 'FILLED' ? 'up' : (o.status === 'PARTIAL_FILLED' || o.status === 'SUBMITTED' ? 'warn' : 'neutral'), false)}</td>
                <td class="txt-muted">${esc(o.reason)}</td>
              </tr>
            `
              )
              .join('')}
          </tbody>
        </table>
      `;
      return;
    }

    const trades = data.closed_trades || [];
    if (!trades.length) {
      wrap.innerHTML = `<div style="text-align:center;padding:32px;color:var(--text-secondary);">No closed trades recorded in the selected time window.</div>`;
      return;
    }
    wrap.innerHTML = `
      <table class="pro-table">
        <thead>
          <tr>
            <th>Exit Time (UTC)</th>
            <th>Symbol</th>
            <th>Entry Setup</th>
            <th class="num">Shares</th>
            <th class="num">Entry → Exit</th>
            <th class="num">Gross PnL</th>
            <th class="num">Total Fee</th>
            <th class="num">Net PnL</th>
            <th class="num">R-Multiple</th>
            <th>Exit Rule</th>
          </tr>
        </thead>
        <tbody>
          ${trades
            .map((t) => {
              const net = Number(t.net_pnl_usd || 0);
              return `
                <tr>
                  <td style="font-family:var(--font-mono);color:var(--text-secondary);">${esc(String(t.exit_time || '').slice(0, 19).replace('T', ' '))}</td>
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
            .join('')}
        </tbody>
      </table>
    `;
  }
}
