#!/usr/bin/env python3
"""Check a fresh HTTP deployment without printing credentials or tokens."""
import json, pathlib, re, subprocess, sys, urllib.error, urllib.request
base = pathlib.Path('/srv/weknora-production')
origin = sys.argv[1] if len(sys.argv) > 1 else 'http://127.0.0.1:18080'
release = (base/'current').resolve()
manifest = json.loads((release/'manifest.json').read_text())
credentials = json.loads((base/'secrets/admin.json').read_text())
cmd = ['docker', 'compose', '--env-file', str(base/'secrets/app.env'), '--env-file', str(release/'images.env'), '-f', str(release/'production/compose.yaml')]
def api(path, payload=None, token=None):
    headers = {'Host': 'ai-doc.tianjizn.com'}
    if payload is not None: headers['Content-Type'] = 'application/json'
    if token: headers['Authorization'] = 'Bearer '+token
    request = urllib.request.Request(origin+path, data=json.dumps(payload).encode() if payload is not None else None, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response: return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()
def output(args):
    return subprocess.check_output(cmd+args, text=True).strip()
status, html = api('/')
assert status == 200 and b'<html' in html.lower(), 'Frontend HTML failed'
assets = re.findall(rb'<script[^>]+src="([^"]+)"', html)
assert assets, 'No frontend script assets'
for asset in assets:
    status, body = api(asset.decode())
    assert status == 200 and len(body)>100, 'Frontend asset failed'
status, body = api('/api/v1/auth/config')
config=json.loads(body)
assert status == 200 and config.get('registration_mode')=='invite_only', 'Registration is not closed'
assert config.get('complex_password_enabled') is True, 'Password policy missing'
status, body = api('/api/v1/auth/login', {'email':credentials['email'], 'password':credentials['password']})
login=json.loads(body)
assert status==200 and login.get('success') and login.get('user',{}).get('is_system_admin'), 'Administrator login failed'
token=login['token']
status, _ = api('/api/v1/knowledge-bases')
assert status == 401, 'Anonymous knowledge base access was not rejected'
status, _ = api('/api/v1/knowledge-bases', token=token)
assert status == 200, 'Authenticated database-backed request failed'
status, _ = api('/api/v1/auth/register', credentials)
assert status == 403, 'Public registration accepted'
subprocess.run(['bash',str(base/'bin/check-schema.sh'),str(release)],check=True,stdout=subprocess.DEVNULL)
app_id=output(['ps','-q','app'])
environment=json.loads(subprocess.check_output(['docker','inspect',app_id,'--format','{{json .Config.Env}}'],text=True))
assert 'WEKNORA_BUILD_COMMIT='+manifest['commit'] in environment, 'Application revision mismatch'
output(['exec','-T','frontend','sh','-c','grep -Rl --include="*.js" '+manifest['commit']+' /usr/share/nginx/html >/dev/null'])
images={}
for service,key in [('app','APP_IMAGE'),('frontend','UI_IMAGE'),('postgres','POSTGRES_IMAGE'),('redis','REDIS_IMAGE')]:
    container=output(['ps','-q',service])
    record=json.loads(subprocess.check_output(['docker','inspect',container],text=True))[0]
    assert record['Image']==manifest['images'][key], 'Image identity mismatch: '+service
    assert record['State'].get('Health',{}).get('Status')=='healthy', 'Unhealthy container: '+service
    assert record['HostConfig']['CgroupParent']=='weknora-production.slice', 'Wrong resource slice'
    assert not record['HostConfig'].get('DeviceRequests') and not record['HostConfig'].get('Devices'), 'Unexpected GPU/device allocation'
    images[service]={'image':record['Image'],'health':'healthy','restart_policy':record['HostConfig']['RestartPolicy']['Name'],'memory_bytes':record['HostConfig']['Memory'],'nano_cpus':record['HostConfig']['NanoCpus']}
result={'origin':origin,'release':manifest['tag'],'commit':manifest['commit'],'frontend_and_assets':'PASS','admin_login':'PASS','authenticated_database_request':'PASS','registration_closed':'PASS','anonymous_access_denied':'PASS','migration_gate':'PASS','images':images}
print(json.dumps(result,indent=2))
