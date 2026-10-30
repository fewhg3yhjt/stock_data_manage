/* Browser acceptance of the configured daily and source views; no external requests. */
const fs=require('node:fs'), path=require('node:path'), assert=require('node:assert/strict'), vm=require('node:vm');
async function main(){
 const [playwrightPath,browserPath,url='http://127.0.0.1:18767',root=path.resolve(__dirname,'../tmp/presentation-20261029')]=process.argv.slice(2);
 assert(playwrightPath && browserPath && ['127.0.0.1','localhost'].includes(new URL(url).hostname));
 const output=path.join(root,'browser');fs.mkdirSync(output,{recursive:true});
 const evidence={started_at:new Date().toISOString(),base_url:url,node:process.version,checks:[],errors:[],external:[]};
 const script=fs.readFileSync(path.resolve(__dirname,'../src/service/static/app.js'),'utf8');
 const format=vm.runInNewContext(script.slice(script.indexOf('function decimalText'),script.indexOf('function displayCell'))+';decimalText');
 for(const [raw,min,expected] of [['11.30000000',2,'11.30'],['10.15000000',2,'10.15'],['4.369',2,'4.369'],['3.029',2,'3.029'],['9007199254740993.125',2,'9007199254740993.125'],['0.00000000',0,'0']]) assert.equal(format(raw,min),expected);
 evidence.decimal_cases=6;
 const browser=await require(playwrightPath).chromium.launch({executablePath:browserPath,headless:true});
 evidence.browser_version=browser.version();evidence.playwright_version=require(path.join(playwrightPath,'package.json')).version;
 const context=await browser.newContext({viewport:{width:1440,height:1000},locale:'zh-CN'});const page=await context.newPage();
 page.on('pageerror',e=>evidence.errors.push(e.message));page.on('requestfailed',r=>evidence.errors.push(r.url()+': '+r.failure()?.errorText));
 page.on('console',m=>{if(m.type()==='error')evidence.errors.push(m.text());});
 await context.route('**/*',async route=>{if(new URL(route.request().url()).origin!==new URL(url).origin){evidence.external.push(route.request().url());await route.abort();}else await route.continue();});
 try{
  assert.equal((await page.goto(url)).status(),200);
  evidence.synthetic_types=[];
  for(const [asset,unit,label] of [['etf','CNY','元/份'],['lof','CNY','元/份'],['index','point','点']]){
   const html=await page.evaluate(({asset,unit})=>displayCell({asset_type:asset,values:{open:'4.36900000'},field_metadata:{open:{unit,status:'available'}}},{key:'open',path:'values.open',format:'decimal',min_decimals:2,trim_trailing_zeros:true}),{asset,unit});
   assert(html.includes('4.369</span>')&&html.includes(label));evidence.synthetic_types.push({asset,unit,passed:true});
  }
  for(const width of [1440,1024]){
   await page.setViewportSize({width,height:1000});await page.locator('[data-view="daily"]').click();await page.locator('#daily-filters').waitFor();
   for(const adjustment of ['none','forward','backward']){
    await page.locator('#daily-filters [name="start_date"]').fill('2026-10-29');await page.locator('#daily-filters [name="end_date"]').fill('2026-10-29');
    await page.locator('#daily-filters [name="asset_type"]').selectOption('stock');
    const loaded=page.waitForResponse(r=>r.url().includes('/api/daily?')&&r.status()===200);
    if(await page.locator('#daily-filters [name="adjustment"]').inputValue()!==adjustment)await page.locator('#daily-filters [name="adjustment"]').selectOption(adjustment);
    else await page.locator('#daily-filters button').click();
    const actual=await (await loaded).json();const expected=JSON.parse(fs.readFileSync(path.join(root,`checks/api-${adjustment}.json`),'utf8'));
    assert.deepEqual(actual,expected);
    await page.waitForFunction(a=>document.querySelector('#daily-filters [name="adjustment"]')?.value===a&&document.querySelectorAll('#app tbody tr').length===3,adjustment);
    assert.deepEqual(await page.locator('#app th').allTextContents(),expected.columns.map(c=>c.label));
    const rows=await page.locator('#app tbody tr').evaluateAll(items=>items.map(tr=>Object.fromEntries([...tr.querySelectorAll('td')].map(td=>[td.dataset.column,{text:td.querySelector('span')?.textContent,all:td.textContent,nowrap:getComputedStyle(td).whiteSpace}]))));
    for(const item of expected.items){
     const row=rows.find(r=>r.symbol.text===item.symbol);assert(row);assert.equal(row.security_name.text,item.security_name);assert.equal(row.asset_type.text,'股票');assert.equal(row.trade_date.text,'2026-10-29');assert.equal(row.trade_date.nowrap,'nowrap');
     for(const key of ['open','high','low','close'])assert.equal(row[key].text,format(item.values[key],2));
     if(item.symbol==='bj920001'){assert.equal(row.volume.text,'—');assert(row.volume.all.includes('单位未核验'));}else assert(row.volume.all.includes('股')&&row.volume.all.includes('来源舍入'));
     assert(row.open.all.includes('元/股'));assert(row.security_name.all.includes('2026-10-09'));
    }
    await page.screenshot({path:path.join(output,`daily-${width}-${adjustment}.png`),fullPage:true});
    await page.locator('#app .table-wrap').evaluate(el=>el.scrollLeft=el.scrollWidth);
    await page.screenshot({path:path.join(output,`daily-${width}-${adjustment}-right.png`),fullPage:true});
    evidence.checks.push({view:'daily',width,adjustment,rows:3,columns:13,ohlc:12,passed:true});
   }
   await page.locator('[data-view="data"]').click();await page.locator('#data-filters').waitFor();
   assert.equal(await page.locator('#data-filters [name="dataset"]').inputValue(),'daily_bar');
   await page.locator('#data-filters [name="start_date"]').fill('2026-10-29');await page.locator('#data-filters [name="end_date"]').fill('2026-10-29');await page.locator('#data-filters [name="security"]').fill('');await page.locator('#data-filters [name="asset_type"]').selectOption('stock');
   const loaded=page.waitForResponse(r=>r.url().includes('/api/data?')&&new URL(r.url()).searchParams.get('asset_type')==='stock'&&!new URL(r.url()).searchParams.get('security')&&r.status()===200);await page.locator('#data-filters button').click();assert.equal((await (await loaded).json()).total,9);
   await page.waitForFunction(()=>document.querySelectorAll('#app tbody tr').length===9&&!document.querySelector('#data-filters button')?.disabled&&document.querySelector('#data-filters [name="asset_type"]')?.value==='stock');
   assert.equal(await page.locator('#app th').count(),12);assert(!(await page.locator('#app thead').innerText()).includes('source_api'));
   const rows=await page.locator('#app tbody tr').evaluateAll(items=>items.map(tr=>Object.fromEntries([...tr.querySelectorAll('td')].map(td=>[td.dataset.column,td.textContent]))));
   const bj=rows.find(row=>row.symbol==='bj920001');assert(bj.volume.includes('11829.00')&&bj.volume.includes('单位未核验'));const sh=rows.find(row=>row.symbol==='sh600519');assert(sh.volume.includes('42992.00')&&sh.volume.includes('手')&&sh.amount.includes('万元'));
   const sourceExpected=JSON.parse(fs.readFileSync(path.join(root,'checks/source-api.json'),'utf8'));
   const spans=await page.locator('#app tbody tr').evaluateAll(items=>items.map(tr=>Object.fromEntries([...tr.querySelectorAll('td')].map(td=>[td.dataset.column,td.querySelector('span')?.textContent]))));
   for(let i=0;i<spans.length;i++){
    for(const [field,key] of Object.entries({open:'1',high:'3',low:'4',close:'2',volume:'5',amount:'8'}))assert.equal(spans[i][field],sourceExpected.items[i].values[key]);
    if(!rows[i].symbol.startsWith('bj'))assert(rows[i].volume.includes('手')&&rows[i].amount.includes('万元'));
   }
   await page.screenshot({path:path.join(output,`source-${width}.png`),fullPage:true});await page.locator('#app .table-wrap').evaluate(el=>el.scrollLeft=el.scrollWidth);await page.screenshot({path:path.join(output,`source-${width}-right.png`),fullPage:true});
   await page.locator('#app tbody tr').first().click();await page.locator('#detail-dialog').waitFor();const detail=await page.locator('#dialog-content').innerText();assert(detail.includes('来源字段9（含义未核验）')&&detail.includes('source_bar_key'));await page.screenshot({path:path.join(output,`source-details-${width}.png`),fullPage:true});await page.locator('.dialog-close').click();
   await page.locator('#data-filters [name="security"]').fill('贵州茅台');const named=page.waitForResponse(r=>r.url().includes('/api/data?')&&new URL(r.url()).searchParams.get('security')==='贵州茅台'&&r.status()===200);await page.locator('#data-filters button').click();assert.equal((await (await named).json()).total,3);
   await page.waitForFunction(()=>document.querySelectorAll('#app tbody tr').length===3&&!document.querySelector('#data-filters button')?.disabled);
   evidence.checks.push({view:'source',width,stock_rows:9,name_rows:3,columns:12,raw_values_preserved:true,passed:true});
  }
  assert.deepEqual(evidence.errors,[]);assert.deepEqual(evidence.external,[]);evidence.passed=true;
 }catch(error){evidence.passed=false;evidence.error=error.stack;await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});throw error;}
 finally{evidence.finished_at=new Date().toISOString();fs.writeFileSync(path.join(output,'results.json'),JSON.stringify(evidence,null,2)+'\n');await browser.close();}
 console.log(JSON.stringify(evidence));
}
main().catch(error=>{console.error(error);process.exitCode=1;});
