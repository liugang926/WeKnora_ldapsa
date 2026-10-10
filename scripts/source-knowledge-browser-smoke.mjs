// Production frontend with a controlled API fixture. This is UI acceptance,
// not an LDAP, graph backend, or cross-service integration test.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.WEKNORA_PLAYWRIGHT_MODULE || 'playwright');
const dist = path.resolve(new URL('../frontend/dist/', import.meta.url).pathname);
assert.ok(fs.existsSync(path.join(dist, 'index.html')), 'Build the frontend before the browser test');
const user = { id:'synthetic-source-reader', username:'source-reader', tenant_id:7, preferences:{} };
const tenant = { id:7, name:'已有企业知识空间', owner_id:'ordinary-owner' };
const memberships = [{ tenant_id:7, tenant_name:tenant.name, role:'viewer', scoped_only:true }];
const requests = []; const qaBodies = []; const errors = [];
let revoked = false;
const json = (res, data, status=200) => { res.writeHead(status, {'Content-Type':'application/json'}); res.end(JSON.stringify(data)); };
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost'); const p = url.pathname;
  if (p.startsWith('/api/')) {
    requests.push([req.method,p]);
    if (p === '/api/v1/auth/me') return json(res, {success:true,data:{user,tenant,memberships,capabilities:{can_create_tenant:false}}});
    if (p === '/api/v1/knowledge-bases') return json(res, {success:true,data:[{id:'source-kb',name:'研发资料'}]});
    if (p === '/api/v1/integrations/nextcloud/ask-target') return json(res, {success:true,data:{knowledge_id:'current-source-document',knowledge_base_id:'source-kb',title:'规范.txt',source_etag:'etag-1'}},revoked?403:200);
    if (p === '/api/v1/sessions' && req.method === 'POST') return json(res,{success:true,data:{id:'private-source-session'}});
    if (p === '/api/v1/knowledge-chat/private-source-session') {
      let body='';for await(const b of req) body+=b;
      qaBodies.push(JSON.parse(body));
      res.writeHead(revoked?403:200, {'Content-Type':'text/event-stream','Cache-Control':'no-store'});
      if (!revoked) res.write('data: '+JSON.stringify({response_type:'answer',content:'只从已授权来源回答。'})+'\n\n');
      return res.end();
    }
    if (p === '/api/v1/messages/private-source-session/load') return json(res,
      revoked?{success:false,error:'source_unavailable'}:{success:true,data:[{id:'question-1',role:'user',content:'规范是什么？'},{id:'answer-1',role:'assistant',content:'只从已授权来源回答。'}]},revoked?403:200);
    if (p === '/api/v1/auth/me/preferences' || p === '/api/v1/auth/logout') return json(res,{success:true,data:{}});
    return json(res,{success:false,error:'unexpected_fixture_api'},403);
  }
  const staticFile = path.resolve(dist,'.'+p);
  const file = fs.existsSync(staticFile) && fs.statSync(staticFile).isFile() ? staticFile :
    (path.extname(p) ? staticFile : path.join(dist,'index.html'));
  if (!file.startsWith(dist+path.sep) || !fs.existsSync(file)) { res.writeHead(404); return res.end(); }
  const ext=path.extname(file);res.setHeader('Content-Type',({'.html':'text/html','.js':'application/javascript','.css':'text/css','.svg':'image/svg+xml','.woff2':'font/woff2'})[ext]||'application/octet-stream');
  fs.createReadStream(file).pipe(res);
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin='http://127.0.0.1:'+server.address().port;
let browser;
try {
  browser=await chromium.launch({headless:true,...(process.env.WEKNORA_BROWSER_EXECUTABLE?{executablePath:process.env.WEKNORA_BROWSER_EXECUTABLE}:{})});
  const page=await browser.newPage({viewport:{width:1100,height:900}});
  page.on('pageerror',e=>errors.push(e.message));
  await page.addInitScript(({user,tenant,memberships})=>{
    localStorage.setItem('weknora_token','synthetic-browser-token');
    localStorage.setItem('weknora_user',JSON.stringify(user));
    localStorage.setItem('weknora_tenant',JSON.stringify(tenant));
    localStorage.setItem('weknora_memberships',JSON.stringify(memberships));
  },{user,tenant,memberships});
  await page.goto(origin+'/platform/knowledge-bases');
  await page.waitForURL('**/source-knowledge');
  await page.getByRole('heading',{name:'我的来源知识库'}).waitFor();
  await page.getByLabel('你的问题').fill('规范是什么？');
  await page.getByRole('button',{name:'提问',exact:true}).click();
  await page.getByText('只从已授权来源回答。',{exact:true}).waitFor();
  assert.equal(qaBodies.length,1);assert.deepEqual(qaBodies[0].knowledge_base_ids,['source-kb']);
  assert.equal(qaBodies[0].agent_enabled,false);assert.equal(qaBodies[0].web_search_enabled,false);
  revoked=true;
  await page.getByText('无法验证当前来源权限。内容已隐藏，请刷新或返回 Nextcloud。').waitFor({timeout:12000});
  assert.equal(await page.getByText('只从已授权来源回答。',{exact:true}).count(),0,'revoked history remained in the DOM');
  revoked=false;
  await page.goto(origin+'/platform/nextcloud-ask?instance_id=nc-1&binding_id=binding-1&file_id=99&source_etag=etag-1');
  await page.getByRole('heading',{name:'提问文件：规范.txt'}).waitFor();
  await page.getByLabel('你的问题').fill('规范是什么？');
  await page.getByRole('button',{name:'提问',exact:true}).click();
  await page.getByText('只从已授权来源回答。',{exact:true}).waitFor();
  assert.deepEqual(qaBodies.at(-1).knowledge_ids,['current-source-document']);
  await page.setViewportSize({width:390,height:844});
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),'mobile page overflow');
  if(process.env.WEKNORA_BROWSER_SCREENSHOT) await page.screenshot({path:process.env.WEKNORA_BROWSER_SCREENSHOT,fullPage:true});
  assert.deepEqual(errors,[],'uncaught frontend errors');
  assert.equal(requests.some(([,p])=>/\/models|\/agents|\/invitations|deployment-capabilities|\/tenants$/.test(p)),false,'source view requested unrelated tenant resources');
  console.log(JSON.stringify({scope:'production_frontend_controlled_api_fixture',passed:['scoped_redirect','explicit_kb_qa','file_qa','revoked_history_hidden','no_unrelated_resource_fetch','mobile_width','no_page_errors'],qa_requests:qaBodies.length}));
} finally { await browser?.close();await new Promise(r=>server.close(r)); }
