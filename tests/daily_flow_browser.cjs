/* Explicit local-browser acceptance of the approved flow, using the existing UI. */
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');

async function main() {
  const [playwrightPath, browserPath, baseURL = 'http://127.0.0.1:18766'] = process.argv.slice(2);
  assert(playwrightPath && browserPath, 'Pass the installed Playwright module and browser executable paths.');
  assert(['127.0.0.1', 'localhost'].includes(new URL(baseURL).hostname));
  const base = path.resolve(__dirname, '../tmp/daily-flow-20261029');
  const output = path.join(base, 'browser');
  fs.mkdirSync(output, {recursive: true});
  const {chromium} = require(playwrightPath);
  const evidence = {started_at: new Date().toISOString(), node: process.version, browser_executable: browserPath,
    playwright_version: require(path.join(playwrightPath, 'package.json')).version,
    base_url: baseURL, checks: [], console_errors: [], request_failures: [], external_requests: []};
  const browser = await chromium.launch({executablePath: browserPath, headless: true});
  evidence.browser_version = browser.version();
  const context = await browser.newContext({viewport: {width: 1440, height: 1000}, locale: 'zh-CN'});
  const page = await context.newPage();
  page.on('pageerror', error => evidence.console_errors.push(error.message));
  page.on('console', message => {if (message.type() === 'error') evidence.console_errors.push(message.text());});
  page.on('requestfailed', request => evidence.request_failures.push({url: request.url(), error: request.failure()}));
  await context.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (url.origin !== new URL(baseURL).origin) {
      evidence.external_requests.push(url.href);
      await route.abort();
    } else await route.continue();
  });
  try {
    const initial = await page.goto(baseURL);
    assert.equal(initial.status(), 200);
    await page.locator('[data-view="daily"]').click();
    await page.locator('#daily-filters').waitFor();
    assert.equal(await page.locator('#daily-filters [name="adjustment"]').inputValue(), 'none');
    for (const adjustment of ['none', 'forward', 'backward']) {
      await page.locator('#daily-filters [name="start_date"]').fill('2026-10-29');
      await page.locator('#daily-filters [name="end_date"]').fill('2026-10-29');
      await page.locator('#daily-filters [name="asset_type"]').selectOption('stock');
      const loaded = page.waitForResponse(response => response.url().includes('/api/daily?') && response.status() === 200);
      if (adjustment === 'none') await page.locator('#daily-filters button').click();
      else await page.locator('#daily-filters [name="adjustment"]').selectOption(adjustment);
      const response = await loaded;
      await page.waitForFunction(a => document.querySelector('#daily-filters [name="adjustment"]')?.value === a &&
        document.querySelectorAll('#app tbody tr').length === 3, adjustment);
      const actual = await response.json();
      const expected = JSON.parse(fs.readFileSync(path.join(base, `checks/api-${adjustment}.json`), 'utf8'));
      assert.deepEqual(actual, expected);
      const rows = await page.locator('#app tbody tr').evaluateAll(items =>
        items.map(row => [...row.querySelectorAll('td')].map(cell => cell.textContent.trim())));
      for (const item of expected.items) {
        const row = rows.find(cells => cells[0].startsWith(item.symbol + ' / '));
        assert(row, `Missing visible symbol: ${item.symbol}`);
        assert.equal(row[1], '2026-10-29');
        assert.equal(row[2], {none: '不复权', forward: '前复权', backward: '后复权'}[adjustment] + ' / CNY');
        for (const [column, key] of ['open', 'high', 'low', 'close'].entries())
          assert.equal(row[column + 3], String(item.values[key]), `${item.symbol}/${adjustment}/${key}`);
        assert(row[7].includes('tencent / 可用'));
      }
      await page.screenshot({path: path.join(output, `daily-${adjustment}.png`), fullPage: true});
      evidence.checks.push({view: 'daily', adjustment, api_rows: actual.total, visible_rows: rows.length,
        visible_ohlc_checks: 12, passed: true});
    }
    for (const view of ['tasks', 'publications', 'raw']) {
      await page.locator(`[data-view="${view}"]`).click();
      await page.locator('#app .loading').waitFor({state: 'detached'});
      const text = await page.locator('#app').innerText();
      if (view === 'tasks') assert(text.includes('three-stock-20261029-live') && text.includes('已发布'));
      if (view === 'publications') assert(text.includes('2026-10-29') && text.includes('daily_bar'));
      if (view === 'raw') assert(text.includes('2026-10-29'));
      await page.screenshot({path: path.join(output, `${view}.png`), fullPage: true});
      evidence.checks.push({view, passed: true});
    }
    assert.deepEqual(evidence.console_errors, []);
    assert.deepEqual(evidence.request_failures, []);
    assert.deepEqual(evidence.external_requests, []);
    evidence.passed = true;
  } catch (error) {
    evidence.passed = false;
    evidence.error = error.stack;
    await page.screenshot({path: path.join(output, 'failure.png'), fullPage: true});
    throw error;
  } finally {
    evidence.finished_at = new Date().toISOString();
    fs.writeFileSync(path.join(output, 'results.json'), JSON.stringify(evidence, null, 2) + '\n');
    await browser.close();
  }
  console.log(JSON.stringify(evidence));
}
main().catch(error => {console.error(error); process.exitCode = 1;});
