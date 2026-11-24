export class QuantApiClient {
  async getDashboard() {
    const res = await fetch('/api/dashboard');
    if (!res.ok) throw new Error(`Dashboard HTTP ${res.status}`);
    return res.json();
  }

  async getHistory({ preset = 'ALL', startDate = '', endDate = '', symbol = '', exitReason = '' } = {}) {
    const params = new URLSearchParams();
    if (preset) params.set('preset', preset);
    if (startDate) params.set('start_date', startDate);
    if (endDate) params.set('end_date', endDate);
    if (symbol) params.set('symbol', symbol);
    if (exitReason) params.set('exit_reason', exitReason);
    const res = await fetch(`/api/history?${params.toString()}`);
    if (!res.ok) throw new Error(`History HTTP ${res.status}`);
    return res.json();
  }

  async runBacktest({ days = 30, startDate = null, endDate = null, symbols = null } = {}) {
    const payload = { days };
    if (startDate) payload.start_date = startDate;
    if (endDate) payload.end_date = endDate;
    if (symbols && symbols.length) payload.symbols = symbols;
    const res = await fetch('/api/backtest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error(`Backtest HTTP ${res.status}`);
    return res.json();
  }

  async getLogs({ sinceId = 0, level = 'ALL', limit = 150 } = {}) {
    const params = new URLSearchParams({ since_id: String(sinceId), limit: String(limit) });
    if (level && level !== 'ALL') params.set('level', level);
    const res = await fetch(`/api/logs?${params.toString()}`);
    if (!res.ok) throw new Error(`Logs HTTP ${res.status}`);
    return res.json();
  }

  async triggerScan() {
    const res = await fetch('/api/scan', { method: 'POST' });
    return res.json();
  }

  async toggleHalt(halted, reason = 'Operator dashboard action') {
    const res = await fetch('/api/halt', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ halted, reason }),
    });
    return res.json();
  }
}
