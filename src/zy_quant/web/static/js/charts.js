export class EquityChart {
  static render(svgEl, points, baselineEquity, height = 190) {
    if (!svgEl) return;
    if (!points || points.length === 0) {
      svgEl.innerHTML = `<text x="50%" y="50%" text-anchor="middle" fill="#5e6673" font-size="12">Waiting for equity curve data points...</text>`;
      return;
    }

    const W = 720;
    const H = height;
    svgEl.setAttribute('viewBox', `0 0 ${W} ${H}`);

    const vals = points.map((p) => Number(p.equity_usd || baselineEquity));
    const base = Number(baselineEquity || vals[0] || 10000);
    const minV = Math.min(base, ...vals) * 0.9985;
    const maxV = Math.max(base, ...vals) * 1.0015;
    const span = Math.max(0.01, maxV - minV);

    const padX = 12;
    const padY = 16;
    const usableW = W - padX * 2;
    const usableH = H - padY * 2;

    const toXY = (val, idx) => {
      const x = points.length === 1 ? W / 2 : padX + (idx / (points.length - 1)) * usableW;
      const y = H - padY - ((val - minV) / span) * usableH;
      return [x, y];
    };

    const coords = vals.map((v, i) => toXY(v, i));
    const linePts = coords.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
    const firstX = coords[0][0].toFixed(1);
    const lastX = coords[coords.length - 1][0].toFixed(1);
    const areaPts = `${firstX},${H - padY} ${linePts} ${lastX},${H - padY}`;

    const [, baseY] = toXY(base, 0);
    const isPositive = vals[vals.length - 1] >= base;
    const stroke = isPositive ? '#0ecb81' : '#f6465d';
    const gradId = `eqGrad_${svgEl.id || 'main'}`;

    svgEl.innerHTML = `
      <defs>
        <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="${stroke}" stop-opacity="0.28"/>
          <stop offset="100%" stop-color="${stroke}" stop-opacity="0.0"/>
        </linearGradient>
      </defs>
      <line x1="${padX}" y1="${baseY.toFixed(1)}" x2="${W - padX}" y2="${baseY.toFixed(1)}"
            stroke="rgba(255,255,255,0.14)" stroke-width="1" stroke-dasharray="4 4"/>
      <polygon points="${areaPts}" fill="url(#${gradId})"/>
      <polyline fill="none" stroke="${stroke}" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" points="${linePts}"/>
    `;
  }
}
