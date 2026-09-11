const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');
const root = path.resolve(__dirname, '..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8');
const data = JSON.parse(read('latest_shorts.json'));
const tick = () => new Promise(resolve => setTimeout(resolve, 0));
async function setup({query='', failure=false, payload=data, storageFailure=false} = {}) {
  const dom = new JSDOM(read('index.html'), {url:`https://pipy.github.io/${query}`, runScripts:'outside-only'});
  const w = dom.window;
  w.HTMLElement.prototype.scrollIntoView = function() {};
  w.fetch = async url => {
    assert.equal(url, 'latest_shorts.json');
    if (failure) throw new Error('offline');
    return {ok:true, json:async () => payload};
  };
  if (storageFailure) Object.defineProperty(w, 'localStorage', {get() {throw new Error('storage disabled');}});
  w.eval(read('assets/viewer.js'));
  await tick();
  return dom;
}
function submit(w, value) {
  w.document.querySelector('#searchBox').value = value;
  w.document.querySelector('#searchBox').dispatchEvent(new w.Event('input', {bubbles:true}));
  w.document.querySelector('#searchForm').dispatchEvent(new w.Event('submit', {bubbles:true, cancelable:true}));
}
(async () => {
  const dom = await setup({storageFailure:true});
  const w = dom.window, doc = w.document;
  assert.equal(doc.querySelector('#searchButton').disabled, false);
  assert.equal(doc.querySelector('#shortTable').style.display, 'none');
  assert.ok(doc.querySelector('#metaInfo').textContent.includes(new Intl.NumberFormat('ja-JP').format(data.length)));
  // Actual data: selection, arithmetic, filtering, empty rows, HTML safety.
  const sample = [data.find(s => s.code === '7201'), data.find(s => !s.shorts.length),
    data.find(s => s.shorts.some(r => r.is_advanced)), data.find(s => s.shorts.some(r => r.status === 'reporting_lost'))].filter(Boolean);
  for (const stock of sample) {
    submit(w, stock.code);
    assert.equal(new URL(w.location.href).searchParams.get('q'), stock.code);
    assert.match(doc.querySelector('#pageTitle').textContent, new RegExp(stock.code));
    const rows = stock.shorts.filter(r => !r.is_advanced);
    assert.equal(doc.querySelectorAll('#shortTable tbody tr').length, rows.length);
    const expected = rows.reduce((s, r) => s + (Number(r.position) || 0), 0);
    assert.equal(Number(doc.querySelector('#totalPos').textContent.replaceAll(',', '')), expected);
    assert.equal(doc.querySelector('#shortTable').style.display, rows.length ? 'table' : 'none');
    if (!rows.length) assert.match(doc.querySelector('#noResult').textContent, /ゼロ.*意味しません/);
  }
  submit(w, '７２０１');
  assert.equal(new URL(w.location.href).searchParams.get('q'), '7201');
  const alpha = data.find(s => /[A-Z]/.test(s.code));
  if (alpha) {
    submit(w, alpha.code.toLowerCase());
    assert.equal(new URL(w.location.href).searchParams.get('q'), alpha.code);
  }
  submit(w, '日産');
  assert.match(doc.querySelector('#pageTitle').textContent, /日産/);
  submit(w, 'zzzz_no_such_stock');
  assert.equal(doc.querySelector('#shortTable').style.display, 'none');
  assert.match(doc.querySelector('#noResult').textContent, /見つかりません/);
  assert.equal(new URL(w.location.href).searchParams.has('q'), false);
  const input = doc.querySelector('#searchBox');
  input.value = '7'; input.dispatchEvent(new w.Event('input'));
  input.dispatchEvent(new w.KeyboardEvent('keydown', {key:'ArrowDown', bubbles:true}));
  assert.equal(input.getAttribute('aria-activedescendant'), 'suggest-0');
  doc.querySelector('#searchForm').dispatchEvent(new w.Event('submit', {cancelable:true}));
  assert.equal(input.getAttribute('aria-expanded'), 'false');
  input.value = '7'; input.dispatchEvent(new w.Event('input'));
  const option = doc.querySelector('.suggestItem');
  const down = new w.MouseEvent('mousedown', {bubbles:true, cancelable:true});
  option.dispatchEvent(down); assert.equal(down.defaultPrevented, true);
  const code = option.textContent.trim().split(/\s/)[0]; option.click();
  assert.equal(new URL(w.location.href).searchParams.get('q'), code);
  input.value = '7'; input.dispatchEvent(new w.Event('input'));
  input.dispatchEvent(new w.KeyboardEvent('keydown', {key:'Escape'}));
  assert.equal(input.getAttribute('aria-expanded'), 'false');
  w.history.replaceState({}, '', '?q=7201'); w.dispatchEvent(new w.PopStateEvent('popstate'));
  assert.match(doc.querySelector('#pageTitle').textContent, /7201/);
  w.history.replaceState({}, '', '/'); w.dispatchEvent(new w.PopStateEvent('popstate'));
  assert.equal(doc.querySelector('#initialState').hidden, false);
  dom.window.close();
  for (const config of [{failure:true}, {payload:[]}]) {
    const d = await setup(config);
    assert.match(d.window.document.querySelector('#noResult').textContent, /失敗/);
    assert.equal(d.window.document.querySelector('#searchButton').disabled, true);
    d.window.close();
  }
  const invalid = await setup({query:'?q=nonexistent'});
  assert.match(invalid.window.document.querySelector('#noResult').textContent, /指定された銘柄/);
  invalid.window.close();
  const hostile = await setup({query:'?q=9999', payload:[{code:'9999', name:'<img src=x onerror=alert(1)>', shorts:[], dates:[{date:'2020/01/01', total:1, items:[{name:'<script>alert(1)</script>', position:1}]}]}]});
  assert.equal(hostile.window.document.querySelector('#pageTitle img'), null);
  assert.equal(hostile.window.document.querySelector('#datesPanel script'), null);
  assert.equal(hostile.window.document.querySelector('#dataWarning').hidden, false);
  hostile.window.close();
  // Form wiring, invalid inputs and result refresh in both calculator pages.
  for (const file of ['short-profit', 'change-rate']) {
    const d = new JSDOM(read(`tools/${file}.html`), {runScripts:'outside-only'});
    d.window.eval(read('assets/calculators.js'));
    for (const form of d.window.document.querySelectorAll('form')) {
      const result = d.window.document.getElementById(form.id.replace('Form', 'Result'));
      assert.equal(result.hidden, false);
      const input = form.querySelector('input'); input.value = '-1';
      input.dispatchEvent(new d.window.Event('input', {bubbles:true}));
      form.dispatchEvent(new d.window.Event('submit', {cancelable:true}));
      assert.equal(result.hidden, true);
      input.value = file === 'short-profit' ? '1000' : '0';
      form.dispatchEvent(new d.window.Event('submit', {cancelable:true}));
      assert.equal(result.hidden, false);
      if (file === 'change-rate') assert.match(result.textContent, /前回が0/);
    }
    d.window.close();
  }
  console.log('PASS: real stock data, search, keyboard/pointer selection, URL restore, empty/error/stale states, escaping, calculator forms');
})().catch(e => {console.error(e); process.exitCode = 1;});
