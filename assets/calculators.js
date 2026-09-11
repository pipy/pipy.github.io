'use strict';
// Pure calculations are also exported for regression checks without a browser.
function profit({sell, buy, shares, rate, days, fees}) {
  const gross = (sell - buy) * shares;
  const borrowing = sell * shares * (rate / 100) * days / 365;
  return {gross, borrowing, net:gross - borrowing - fees};
}
function change(previous, current) {
  return {delta:current - previous, rate:previous === 0 ? null : (current - previous) / previous * 100};
}
if (typeof module !== 'undefined') module.exports = {profit, change};
if (typeof document !== 'undefined') {
  const number = (n, digits = 4) => new Intl.NumberFormat('ja-JP', {maximumFractionDigits:digits}).format(n);
  const signed = n => `${n > 0 ? '+' : ''}${number(n)}`;
  function bind(name, calculate) {
    const form = document.getElementById(`${name}Form`);
    if (!form) return;
    const output = document.getElementById(`${name}Result`);
    const error = document.getElementById(`${name}Error`);
    function run() {
      error.hidden = true;
      if (!form.checkValidity()) { output.hidden = true; form.reportValidity(); return; }
      const data = Object.fromEntries([...form.querySelectorAll('input')].map(input => [input.name, input.valueAsNumber]));
      if (Object.values(data).some(n => !Number.isFinite(n))) {
        error.textContent = '各項目に有効な数値を入力してください。'; error.hidden = false; output.hidden = true; return;
      }
      const html = calculate(data);
      if (!html) { error.textContent = '計算できる範囲の数値を入力してください。'; error.hidden = false; output.hidden = true; return; }
      output.innerHTML = html; output.hidden = false;
    }
    form.addEventListener('submit', e => { e.preventDefault(); run(); });
    form.addEventListener('input', () => { output.hidden = true; error.hidden = true; });
    run();
  }
  bind('profit', data => {
    const r = profit(data);
    if (Object.values(r).some(n => !Number.isFinite(n) || Math.abs(n) > Number.MAX_SAFE_INTEGER)) return null;
    return `<p>税引前の概算損益</p><strong>${number(r.net, 0)} 円</strong><p>価格差損益：${number(r.gross, 0)}円<br>貸株料の概算：${number(r.borrowing, 0)}円<br>その他費用：${number(data.fees, 0)}円</p><p class="hint">入力された仮定による概算です。実際の精算額ではありません。</p>`;
  });
  for (const name of ['change', 'points']) bind(name, data => {
    const r = change(data.previous, data.current);
    if (!Number.isFinite(r.delta) || (r.rate !== null && !Number.isFinite(r.rate))) return null;
    return `<p>${name === 'points' ? 'パーセントポイント差' : '増減'}</p><strong>${signed(r.delta)}${name === 'points' ? ' ポイント' : ''}</strong><p>${r.rate === null ? '前回が0のため、増減率は計算できません。' : `前回に対する増減率：${signed(r.rate)}%`}</p>`;
  });
}
