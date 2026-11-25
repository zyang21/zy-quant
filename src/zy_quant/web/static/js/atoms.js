export const fmtUsd = (v, signed = false) => {
  const n = Number(v || 0);
  const abs = Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (signed) return `${n >= 0 ? '+' : '-'}$${abs}`;
  return `${n < 0 ? '-' : ''}$${abs}`;
};

export const fmtPct = (v, signed = true, digits = 2) => {
  const n = Number(v || 0);
  const s = signed && n > 0 ? '+' : '';
  return `${s}${n.toFixed(digits)}%`;
};

export const esc = (str) =>
  String(str ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');

export function infoTip(explanation) {
  return `<span class="info-tip" title="${esc(explanation)}">?</span>`;
}

export function statusBadge(label, variant = 'neutral', withDot = true) {
  const dot = withDot ? '<span class="badge-dot"></span>' : '';
  return `<span class="badge badge-${variant}">${dot}${esc(label)}</span>`;
}

export function metricTile({ label, value, valueClass = '', hint = '', tooltip = '' }) {
  const tip = tooltip ? infoTip(tooltip) : '';
  return `
    <div class="kpi-tile">
      <div class="label">${esc(label)} ${tip}</div>
      <div class="value ${valueClass}">${esc(value)}</div>
      ${hint ? `<div class="hint">${esc(hint)}</div>` : ''}
    </div>
  `;
}

export function scoreBar(score) {
  const s = Math.max(0, Math.min(100, Number(score || 0)));
  return `
    <div class="score-cell">
      <span style="width:36px;text-align:right;font-weight:600;">${s.toFixed(1)}</span>
      <div class="score-track"><div class="score-fill" style="width:${s}%"></div></div>
    </div>
  `;
}

export function humanizeExitReason(code) {
  const map = {
    INITIAL_ATR_STOP: 'Initial Volatility Stop (ATR)',
    BREAKEVEN_STOP: 'Breakeven Capital Guard',
    CHANDELIER_TRAIL: 'Chandelier Trailing Profit Lock',
    TIME_BARRIER: '4h Stagnation Time Exit',
    EOD_T0_FLATTEN: 'Futu HK T+0 EOD Shield',
    SCALE_OUT_2R: 'Partial Profit Scale-Out',
    KILL_SWITCH: 'Emergency Kill-Switch',
  };
  return map[code] || code || 'Closed';
}

export function humanizeSetup(code) {
  const map = {
    VWAP_PULLBACK: 'VWAP Institutional Pullback',
    ORB_30M: '30m Opening Range Breakout',
    RECONCILED_ADOPTED: 'Broker Synced Position',
  };
  return map[code] || code || 'Trend';
}
