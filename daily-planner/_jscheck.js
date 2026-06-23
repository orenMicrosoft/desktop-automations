// Headless sanity harness: load todo-data.js + app.html inline script in a
// DOM-light VM and exercise every view. Exits non-zero on any JS error.
const fs = require('fs'); const vm = require('vm'); const path = require('path');
const dir = __dirname;
const dataJs = fs.readFileSync(path.join(dir, 'todo-data.js'), 'utf8');
const html = fs.readFileSync(path.join(dir, 'app.html'), 'utf8');

const scripts = [...html.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
if (!scripts.length) { console.log('JS_FAIL: no inline script found'); process.exit(1); }
const appScript = scripts[scripts.length - 1];

const store = {};
function makeEl() {
  return { innerHTML: '', textContent: '', style: {}, value: '',
    classList: { toggle() {}, add() {}, remove() {} },
    setAttribute() {}, getAttribute() { return ''; },
    addEventListener() {}, appendChild() {}, querySelector() { return null; },
    offsetLeft: 0, clientWidth: 600, scrollLeft: 0 };
}
const els = {};
const document = {
  getElementById: id => (els[id] || (els[id] = makeEl())),
  createElement: () => ({ style: {}, click() {}, setAttribute() {}, appendChild() {}, href: '' }),
  querySelector: () => null, querySelectorAll: () => [],
};
const localStorage = {
  getItem: k => (k in store ? store[k] : null),
  setItem: (k, v) => { store[k] = String(v); },
  removeItem: k => { delete store[k]; },
};
const ctx = {
  window: {}, document, localStorage, console, Date, JSON, Math,
  fetch: () => Promise.resolve({ ok: true, json: () => Promise.resolve({ ok: true }) }),
  navigator: { clipboard: { writeText: () => Promise.resolve() } },
  setInterval: () => 0, setTimeout: () => 0, clearInterval: () => {},
  confirm: () => true, alert: () => {}, prompt: () => 'https://msazure.visualstudio.com/One/_workitems/edit/12345',
  location: { protocol: 'http:', host: 'localhost:8101' },
  URL: { createObjectURL: () => 'blob:x' }, Blob: function () {},
};
ctx.globalThis = ctx;
vm.createContext(ctx);
try {
  vm.runInContext(dataJs, ctx, { filename: 'todo-data.js' });
  if (!ctx.window.TODO_DATA || !ctx.window.TODO_DATA.days) throw new Error('TODO_DATA missing');
  vm.runInContext(appScript, ctx, { filename: 'app-inline.js' });   // runs render() on load
  ctx.sel('prompts');
  const promptsHtml = els['list'] ? els['list'].innerHTML : '';
  if (!/Start of day/.test(promptsHtml) || !/Close the day/.test(promptsHtml))
    throw new Error('prompts view did not render the skills');
  ctx.sel('backlog');
  const days = ctx.window.TODO_DATA.days;
  const k = Object.keys(days)[0];
  ctx.sel(k);
  const id = days[k].tasks[0].id;
  ctx.cycle(id);                 // exercise click-to-cycle + autosave path
  ctx.addLink(id);               // attach a link (uses stubbed prompt)
  if (!(days[k].tasks.find(t => t.id === id).links || []).length) throw new Error('addLink did not attach');
  ctx.removeLink({ preventDefault() {}, stopPropagation() {} }, id, 0);
  ctx.gotoToday();
  console.log('JS_OK views=today,backlog,prompts links=ok');
} catch (e) {
  console.log('JS_FAIL: ' + (e && e.message ? e.message : e));
  process.exit(1);
}
