'use strict';
const esc = (s = '') => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt = n => new Intl.NumberFormat('ja-JP').format(Number(n) || 0);
const norm = s => String(s || '').normalize('NFKC').toLowerCase().replace(/[ァ-ヶ]/g, c => String.fromCharCode(c.charCodeAt(0) - 0x60));
const by = s => document.querySelector(s);
const searchBox = by('#searchBox'), suggestions = by('#suggestList'), table = by('#shortTable');
const noResult = by('#noResult'), datesPanel = by('#datesPanel');
let stocks = [], matches = [], active = -1, composing = false;
const DEFAULT_TITLE = document.title;
// Remove the unused persistence from the previous version; unavailable storage must not break search.
try { localStorage.removeItem('shorts_last_query_code'); } catch (_) {}
function closeSuggestions() {
  suggestions.style.display = 'none';
  searchBox.setAttribute('aria-expanded', 'false');
  searchBox.removeAttribute('aria-activedescendant');
  active = -1;
}
function updateSuggestions() {
  const term = norm(searchBox.value.trim());
  matches = term ? stocks.filter(s => norm(s.code).startsWith(term) || norm(s.name).includes(term) || norm(s.reading).includes(term)).slice(0, 30) : [];
  active = -1;
  searchBox.removeAttribute('aria-activedescendant');
  suggestions.innerHTML = matches.map((s, i) => `<div class="suggestItem" id="suggest-${i}" role="option" aria-selected="false" data-index="${i}">${esc(s.code)}　${esc(s.name)}</div>`).join('');
  suggestions.style.display = matches.length ? 'block' : 'none';
  searchBox.setAttribute('aria-expanded', String(matches.length > 0));
}
function updateURL(code, mode = 'push') {
  const u = new URL(location.href);
  if (code) u.searchParams.set('q', code); else u.searchParams.delete('q');
  history[mode === 'push' ? 'pushState' : 'replaceState']({ q: code }, '', u);
}
function clearResult(message = '') {
  table.style.display = 'none'; datesPanel.style.display = 'none';
  by('#initialState').hidden = Boolean(message);
  by('#pageTitle').textContent = '機関空売り残高を調べる';
  document.title = DEFAULT_TITLE;
  noResult.style.display = message ? 'block' : 'none';
  noResult.textContent = message;
  closeSuggestions();
}
function render(stock) {
  noResult.style.display = 'none'; by('#initialState').hidden = true;
  by('#pageTitle').textContent = `${stock.code} ${stock.name}`;
  document.title = `${stock.code} ${stock.name}の公表空売り残高 | 空売りデータノート`;
  const rows = stock.shorts.filter(r => !r.is_advanced);
  by('#stockCaption').textContent = `基準日：${stock.base_date || '不明（保存情報のみ）'} · 比較元残高は掲載残高から差分を逆算`;
  table.querySelector('tbody').innerHTML = rows.map(r => {
    const pos = Number(r.position) || 0, change = Number(r.change) || 0;
    const status = r.status === 'reporting_lost' ? '<span class="status lost">公表基準未満</span>' : r.status === 'carried_forward' ? '<span class="status">前回値を継続</span>' : '';
    return `<tr><td>${esc(r.name || '名称不明')} ${status}<br><span class="meta">計算日：${esc(r.calc_date || '不明')}</span></td><td class="pos">${fmt(pos - change)}</td><td class="pos">${fmt(pos)}</td><td class="pos change ${change < 0 ? 'neg' : 'pos'}">${change > 0 ? '+' : ''}${fmt(change)}</td></tr>`;
  }).join('');
  const sum = rows.reduce((s, r) => [s[0] + (Number(r.position) || 0), s[1] + (Number(r.change) || 0)], [0, 0]);
  by('#totalPrev').textContent = fmt(sum[0] - sum[1]);
  by('#totalPos').textContent = fmt(sum[0]);
  by('#totalChange').textContent = `${sum[1] > 0 ? '+' : ''}${fmt(sum[1])}`;
  table.style.display = rows.length ? 'table' : 'none';
  if (!rows.length) {
    noResult.textContent = 'この銘柄には主表の集計対象となる行がありません。空売り残高がゼロであることを意味しません。保存されている日付別内訳は下で確認できます。';
    noResult.style.display = 'block';
  }
  datesPanel.style.display = stock.dates.length ? 'block' : 'none';
  datesPanel.innerHTML = '<details><summary>保存されている日付別内訳を見る</summary><p class="hint">公表ファイル内の行の内訳です。連続した営業日の残高推移ではなく、主表と集計範囲も異なります。</p>' + stock.dates.map(d => `<section><h3>${esc(d.date)} <span class="meta">内訳合計 ${fmt(d.total)}株 ${d.is_advanced ? '（基準日より後・主表の集計外）' : ''}</span></h3><ul>${(d.items || []).map(r => `<li>${esc(r.name)}：${fmt(r.position)}株</li>`).join('')}</ul></section>`).join('') + '</details>';
}
function selectStock(stock, mode = 'push') {
  searchBox.value = `${stock.code} ${stock.name}`;
  closeSuggestions(); updateURL(stock.code, mode); render(stock);
}
function submitSearch() {
  if (composing) return;
  const term = norm(searchBox.value.trim());
  if (!term) { updateURL(''); clearResult(); return; }
  const selected = active >= 0 && searchBox.getAttribute('aria-expanded') === 'true' ? matches[active] :
    stocks.find(s => norm(s.code) === term || norm(`${s.code} ${s.name}`) === term) ||
    stocks.find(s => norm(s.code).startsWith(term) || norm(s.name).includes(term) || norm(s.reading).includes(term));
  if (selected) selectStock(selected);
  else { updateURL(''); clearResult('該当する銘柄が見つかりません。コードや会社名を変えてお試しください。公表・保存データに含まれない銘柄もあります。'); }
}
by('#searchForm').addEventListener('submit', e => { e.preventDefault(); submitSearch(); });
searchBox.addEventListener('compositionstart', () => { composing = true; });
searchBox.addEventListener('compositionend', () => { composing = false; updateSuggestions(); });
searchBox.addEventListener('input', e => { if (!composing && !e.isComposing) updateSuggestions(); });
searchBox.addEventListener('keydown', e => {
  if (composing || e.isComposing || e.keyCode === 229) return;
  if (e.key === 'Escape') { closeSuggestions(); return; }
  if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
    e.preventDefault();
    if (searchBox.getAttribute('aria-expanded') !== 'true') updateSuggestions();
    if (!matches.length) return;
    active = (active + (e.key === 'ArrowDown' ? 1 : active < 0 ? 0 : -1) + matches.length) % matches.length;
    [...suggestions.children].forEach((node, i) => node.setAttribute('aria-selected', String(i === active)));
    searchBox.setAttribute('aria-activedescendant', `suggest-${active}`);
    suggestions.children[active].scrollIntoView({block:'nearest'});
  }
});
// Keep the input focused until a pointer selection is applied. Otherwise focusout
// can hide the list before the browser dispatches the option's click.
suggestions.addEventListener('mousedown', e => { e.preventDefault(); });
suggestions.addEventListener('click', e => {
  const item = e.target.closest('[data-index]');
  if (item && matches[Number(item.dataset.index)]) selectStock(matches[Number(item.dataset.index)]);
});
document.addEventListener('click', e => { if (!e.target.closest('.searchWrap')) closeSuggestions(); });
by('#searchForm').addEventListener('focusout', e => { if (!e.currentTarget.contains(e.relatedTarget)) closeSuggestions(); });
function restoreURL() {
  const code = new URL(location.href).searchParams.get('q') || '';
  const stock = stocks.find(s => norm(s.code) === norm(code));
  if (stock) { searchBox.value = `${stock.code} ${stock.name}`; closeSuggestions(); render(stock); }
  else { searchBox.value = code; clearResult(code ? '指定された銘柄は保存データにありません。コードを確認してください。' : ''); }
}
window.addEventListener('popstate', restoreURL);
(async () => {
  try {
    const response = await fetch('latest_shorts.json', {cache:'no-cache'});
    if (!response.ok) throw new Error('データを取得できませんでした。');
    const raw = await response.json();
    const items = Array.isArray(raw) ? raw : raw.items;
    if (!Array.isArray(items) || !items.length) throw new Error('利用できるデータがありません。');
    stocks = items.map(s => ({...s, code:String(s.code || ''), shorts:Array.isArray(s.shorts) ? s.shorts : [], dates:Array.isArray(s.dates) ? s.dates : []}));
    const latest = stocks.flatMap(s => s.dates.map(d => d.date).concat(s.shorts.map(r => r.calc_date), [s.base_date])).filter(Boolean).sort().at(-1);
    by('#metaInfo').textContent = `収録 ${fmt(stocks.length)}銘柄 · データ内の最新計算日 ${latest || '不明'}`;
    const date = latest ? new Date(`${latest.replaceAll('/', '-')}T00:00:00+09:00`) : null;
    if (!date || !Number.isFinite(date.getTime()) || Date.now() - date.getTime() > 7 * 86400000) {
      by('#dataWarning').hidden = false;
      by('#dataWarning').textContent = 'データが古い、または日付を確認できない可能性があります。最新の公表内容はJPXの原資料をご確認ください。';
    }
    by('#searchButton').disabled = false;
    restoreURL();
  } catch (_) {
    by('#metaInfo').textContent = 'データを読み込めませんでした';
    clearResult('データの取得に失敗しました。ページを再読み込みしてください。解説記事やJPXの原資料は引き続き参照できます。');
  }
})();
