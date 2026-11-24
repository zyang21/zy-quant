import { esc } from '../atoms.js';

export class LogConsoleController {
  constructor(dockEl, api) {
    this.dockEl = dockEl;
    this.api = api;
    this.latestId = 0;
    this.level = 'ALL';
    this.autoScroll = true;
    this.entries = [];
    this.mode = 'normal'; // 'collapsed' | 'normal' | 'expanded'
    this._renderShell();
  }

  _renderShell() {
    this.dockEl.innerHTML = `
      <div class="log-dock-bar">
        <div style="display:flex;align-items:center;gap:12px;">
          <strong style="font-family:var(--font-mono);font-size:12px;letter-spacing:0.02em;">BACKEND RUNTIME LOGS</strong>
          <span class="txt-muted" id="logCountBadge">0 events</span>
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
          <select id="logLevelSelect" style="padding:3px 8px;font-size:11.5px;">
            <option value="ALL">All Levels</option>
            <option value="INFO">INFO</option>
            <option value="WARNING">WARN</option>
            <option value="ERROR">ERROR</option>
          </select>
          <button class="btn" id="logExpandBtn" style="padding:3px 9px;font-size:11.5px;">Expand</button>
          <button class="btn" id="logToggleBtn" style="padding:3px 9px;font-size:11.5px;">Minimize</button>
        </div>
      </div>
      <div class="log-dock-body" id="logBody"></div>
    `;

    document.getElementById('logLevelSelect').addEventListener('change', (e) => {
      this.level = e.target.value;
      this.latestId = 0;
      this.entries = [];
      this.poll();
    });

    document.getElementById('logExpandBtn').addEventListener('click', () => {
      this.mode = this.mode === 'expanded' ? 'normal' : 'expanded';
      this._applyMode();
    });

    document.getElementById('logToggleBtn').addEventListener('click', () => {
      this.mode = this.mode === 'collapsed' ? 'normal' : 'collapsed';
      this._applyMode();
    });
  }

  _applyMode() {
    this.dockEl.classList.toggle('collapsed', this.mode === 'collapsed');
    this.dockEl.classList.toggle('expanded', this.mode === 'expanded');
    document.getElementById('logToggleBtn').textContent =
      this.mode === 'collapsed' ? 'Show Logs' : 'Minimize';
    document.getElementById('logExpandBtn').textContent =
      this.mode === 'expanded' ? 'Restore' : 'Expand';
  }

  async poll() {
    try {
      const res = await this.api.getLogs({
        sinceId: this.latestId,
        level: this.level,
        limit: 150,
      });
      const incoming = res.logs || [];
      if (incoming.length > 0) {
        this.latestId = res.latest_id || this.latestId;
        this.entries.push(...incoming);
        if (this.entries.length > 300) {
          this.entries = this.entries.slice(-300);
        }
        this._renderRows();
      }
    } catch {
      // Keep console resilient if server restarts
    }
  }

  _renderRows() {
    const body = document.getElementById('logBody');
    document.getElementById('logCountBadge').textContent = `${this.entries.length} events`;
    body.innerHTML = this.entries
      .map(
        (r) => `
        <div class="log-row">
          <span class="log-ts">${esc(r.timestamp)}</span>
          <span class="log-lvl-${esc(r.level)}">${esc(r.level)}</span>
          <span class="log-mod">[${esc(r.module)}]</span>
          <span class="log-msg">${esc(r.message)}</span>
        </div>
      `
      )
      .join('');
    if (this.autoScroll) {
      body.scrollTop = body.scrollHeight;
    }
  }
}
