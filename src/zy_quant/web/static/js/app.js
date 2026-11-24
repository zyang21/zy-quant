import { QuantApiClient } from './api.js';
import { fmtPct, statusBadge } from './atoms.js';
import { LogConsoleController } from './components/log_console.js';
import { BacktestLabView } from './views/backtest.js';
import { TradeHistoryView } from './views/history.js';
import { OverviewView } from './views/overview.js';

class QuantWorkspaceApp {
  constructor() {
    this.api = new QuantApiClient();
    this.activeRoute = 'overview';
    this.halted = false;
    this.lastDashboard = null;

    this.views = {
      overview: new OverviewView(document.getElementById('view-overview')),
      history: new TradeHistoryView(document.getElementById('view-history'), this.api),
      backtest: new BacktestLabView(document.getElementById('view-backtest'), this.api),
    };
    this.logConsole = new LogConsoleController(document.getElementById('logDock'), this.api);
  }

  async init() {
    this._bindNavigation();
    this._bindActions();

    const path = window.location.pathname.replace('/', '') || 'overview';
    this.switchRoute(['overview', 'history', 'backtest'].includes(path) ? path : 'overview', false);

    await this.syncDashboard();
    await this.logConsole.poll();

    setInterval(() => this.syncDashboard(), 10000);
    setInterval(() => this.logConsole.poll(), 4000);
  }

  _bindNavigation() {
    document.querySelectorAll('.nav-tab').forEach((btn) => {
      btn.addEventListener('click', () => {
        this.switchRoute(btn.dataset.route, true);
      });
    });
    window.addEventListener('popstate', () => {
      const p = window.location.pathname.replace('/', '') || 'overview';
      this.switchRoute(['overview', 'history', 'backtest'].includes(p) ? p : 'overview', false);
    });
  }

  _bindActions() {
    const scanBtn = document.getElementById('hdrScanBtn');
    scanBtn.addEventListener('click', async () => {
      scanBtn.disabled = true;
      scanBtn.textContent = 'Scanning...';
      try {
        await this.api.triggerScan();
        await this.syncDashboard();
        await this.logConsole.poll();
      } finally {
        scanBtn.disabled = false;
        scanBtn.textContent = 'Refresh Scan';
      }
    });

    const haltBtn = document.getElementById('hdrHaltBtn');
    haltBtn.addEventListener('click', async () => {
      await this.api.toggleHalt(!this.halted, 'Operator header toggle');
      await this.syncDashboard();
      await this.logConsole.poll();
    });
  }

  async switchRoute(route, pushHistory = true) {
    this.activeRoute = route;
    if (pushHistory) {
      const url = route === 'overview' ? '/' : `/${route}`;
      window.history.pushState({}, '', url);
    }

    document.querySelectorAll('.nav-tab').forEach((b) => {
      b.classList.toggle('active', b.dataset.route === route);
    });

    ['overview', 'history', 'backtest'].forEach((r) => {
      document.getElementById(`view-${r}`).style.display = r === route ? 'block' : 'none';
    });

    if (route === 'history') {
      await this.views.history.refresh();
    } else if (route === 'backtest') {
      this.views.backtest.mount(this.lastDashboard?.strategy_summary || {});
      if (!this.views.backtest._hasRunOnce) {
        this.views.backtest._hasRunOnce = true;
        await this.views.backtest.run();
      }
    }
  }

  async syncDashboard() {
    try {
      const d = await this.api.getDashboard();
      this.lastDashboard = d;
      this._renderHeaderAndRibbon(d);
      this.views.overview.render(d);
      if (this.views.backtest._mounted && d.strategy_summary) {
        this.views.backtest.renderBlueprint(d.strategy_summary);
      }
    } catch {
      document.getElementById('hdrConnBadge').innerHTML = statusBadge('Reconnecting...', 'warn');
    }
  }

  _renderHeaderAndRibbon(d) {
    const conn = d.connection || {};
    const ms = d.market_session || {};
    const reg = d.regime || {};

    // API Connection Badge (No raw host/IP/key exposed)
    document.getElementById('hdrConnBadge').innerHTML = conn.connected
      ? statusBadge(`API Connected · ${conn.execution_mode}`, 'up')
      : statusBadge(`Fallback Feed · ${conn.execution_mode}`, 'warn');

    // Market Open/Closed Badge
    document.getElementById('hdrMarketBadge').innerHTML = ms.is_open
      ? statusBadge('US Market Open', 'up')
      : statusBadge('Market Closed · Scan Paused', 'neutral', false);

    // Kill-Switch Button
    this.halted = Boolean(d.halt_state && d.halt_state.halted);
    const haltBtn = document.getElementById('hdrHaltBtn');
    if (this.halted) {
      haltBtn.textContent = 'Resume Trading';
      haltBtn.className = 'btn btn-up';
    } else {
      haltBtn.textContent = 'Kill-Switch';
      haltBtn.className = 'btn btn-danger';
    }

    // Data Integrity / Fallback Banner (Explicit alert whenever fallback or synthetic data is active)
    const fbBanner = document.getElementById('fallbackAlertBanner');
    if (conn.is_fallback_or_fake && conn.fallback_alert) {
      fbBanner.style.display = 'flex';
      document.getElementById('fallbackAlertText').textContent = `ALERT: ${conn.fallback_alert}`;
    } else {
      fbBanner.style.display = 'none';
    }

    // Closed Market Banner
    const banner = document.getElementById('sessionBanner');
    if (ms.is_open === false) {
      banner.style.display = 'flex';
      document.getElementById('sessionBannerText').textContent = ms.message || '';
      document.getElementById('sessionBannerTime').textContent = `Server ${d.server_time_utc || ''}`;
    } else {
      banner.style.display = 'none';
    }

    // Sub-header Binance Ticker Ribbon
    const light = reg.light || 'YELLOW';
    const lightCls = light === 'GREEN' ? 'txt-up' : light === 'RED' ? 'txt-down' : 'txt-warn';
    const qRet = Number(reg.qqq_intraday_return_pct || 0);
    document.getElementById('ribTraffic').innerHTML = `<span class="${lightCls}">${light}</span>`;
    document.getElementById('ribQqq').innerHTML = `$${reg.qqq_price ?? '—'} <span class="${qRet >= 0 ? 'txt-up' : 'txt-down'}">${fmtPct(qRet)}</span>`;
    document.getElementById('ribVwap').textContent = `$${reg.qqq_vwap ?? '—'}`;
    document.getElementById('ribSpy').textContent = `$${reg.spy_price ?? '—'}`;
    document.getElementById('ribVix').textContent = `${reg.vix_level ?? '—'}`;
    document.getElementById('ribFeed').textContent = conn.data_feed || '—';
  }
}

window.addEventListener('DOMContentLoaded', () => {
  const app = new QuantWorkspaceApp();
  app.init();
});
