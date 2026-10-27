#!/usr/bin/env python3
"""Check a fresh HTTP deployment without printing credentials or tokens."""
import json, pathlib, re, subprocess, sys
base = pathlib.Path('/srv/weknora-production')
origin = sys.argv[1] if len(sys.argv) > 1 else 'http://127.0.0.1:18080'
sys.path.insert(0, str(base/'bin'))
from image_identity import resolve_images
from local_http import LocalHTTPClient
http = LocalHTTPClient(origin)
release = (base/'current').resolve()
manifest = json.loads((release/'manifest.json').read_text())
credentials = json.loads((base/'secrets/admin.json').read_text())
cmd = ['docker', 'compose', '--env-file', str(base/'secrets/app.env'), '--env-file', str(release/'images.env'), '-f', str(release/'production/compose.yaml')]
def api(path, payload=None, token=None):
    return http.request(path, payload, token)
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
status, body = api('/health/readiness')
readiness=json.loads(body)
assert status == 200 and readiness.get('status') == 'ready', 'Mandatory deployment readiness failed'
checks=readiness.get('checks', {})
assert checks.get('required') is True, 'Strict production capabilities are not enabled'
assert checks.get('ready') is True and checks.get('pdf') is True and checks.get('docx') is True, 'anydoc PDF/DOCX self-test missing'
assert checks.get('anydoc_version'), 'anydoc version is missing from readiness'
status, body = api('/metrics')
metrics=body.decode()
assert status == 200 and 'candidate_staging_total' in metrics and 'graph_manifest_unprepared_total' in metrics, 'Nextcloud metrics unavailable'
status, body = api('/api/v1/auth/login', {'email':credentials['email'], 'password':credentials['password']})
login=json.loads(body)
assert status==200 and login.get('success') and login.get('user',{}).get('is_system_admin'), 'Administrator login failed'
token=login['token']
status, body = api('/api/v1/system/admin/directory/config', token=token)
directory=json.loads(body)
assert status == 200, 'Directory administration unavailable'
assert directory.get('source') in ('default', 'database') and directory.get('read_only_fields') == [], 'Directory administration is deployment-locked'
status, body = api('/api/v1/system/admin/directory/status', token=token)
directory_health=json.loads(body)
assert status == 200, 'Directory health unavailable'
ldap_runtime='NOT_CONFIGURED'
if directory.get('enabled'):
    assert directory_health.get('available') and not directory_health.get('access_paused'), 'Configured directory is not ready'
    ldap_runtime='PASS'
status, _ = api('/api/v1/system/admin/directory/config')
assert status == 401, 'Anonymous directory administration was not rejected'
status, _ = api('/api/v1/knowledge-bases')
assert status == 401, 'Anonymous knowledge base access was not rejected'
status, _ = api('/api/v1/knowledge-bases', token=token)
assert status == 200, 'Authenticated database-backed request failed'
status, body = api('/api/v1/system/parser-engines', token=token)
parser_config=json.loads(body)
assert status == 200 and parser_config.get('connected') is True, 'DocReader gRPC is not connected'
assert parser_config.get('docreader_addr') == 'docreader:50051', 'DocReader address mismatch'
assert parser_config.get('docreader_transport') == 'grpc', 'DocReader transport mismatch'
anydoc_engine=next((item for item in parser_config.get('data', []) if item.get('name')=='anydoc'), None)
assert anydoc_engine and anydoc_engine.get('available') is True, 'anydoc engine is not available'
status, _ = api('/api/v1/auth/register', credentials)
assert status == 403, 'Public registration accepted'
nextcloud_routes='NOT_DECLARED'
if 'nextcloud_onboarding_v1' in manifest.get('required_features', []):
    # Machine provisioning credentials are intentionally separate from a web
    # administrator JWT. A guarded route must exist and reject that JWT.
    status, body = api('/api/v1/integrations/nextcloud/onboarding/candidates', token=token)
    assert status == 401 and json.loads(body).get('error') == 'onboarding_credential_required', 'Nextcloud onboarding route missing or unguarded'
    status, _ = api('/api/v1/integrations/nextcloud/files/status/v2', token=token)
    assert status == 400, 'Nextcloud V2 status route missing or unguarded'
    status, _ = api('/api/v1/datasource/nextcloud-gc-jobs', token=token)
    assert status == 200, 'Nextcloud GC administration unavailable'
    nextcloud_routes='PASS'
retention_journal='NOT_DECLARED'
if 'original_body_journal' in manifest.get('required_features', []):
    subprocess.run(['python3',str(base/'bin/retention.py'),'status',str(release)],check=True,stdout=subprocess.DEVNULL)
    retention_journal='PASS'
subprocess.run(['bash',str(base/'bin/check-schema.sh'),str(release)],check=True,stdout=subprocess.DEVNULL)
app_id=output(['ps','-q','app'])
environment=json.loads(subprocess.check_output(['docker','inspect',app_id,'--format','{{json .Config.Env}}'],text=True))
assert 'WEKNORA_BUILD_COMMIT='+manifest['commit'] in environment, 'Application revision mismatch'
assert output(['exec','-T','frontend','sh','-c',"find /usr/share/nginx/html -type f -name '*.js' -exec grep -l "+manifest['commit'][:7]+" {} + | head -n 1"]), 'Frontend build revision missing'
resolved=resolve_images(manifest)
assert resolved==json.loads((release/'resolved-images.json').read_text()), 'Recorded image identity proof differs'
images={}
for service,key in [('app','APP_IMAGE'),('frontend','UI_IMAGE'),('docreader','DOCREADER_IMAGE'),('postgres','POSTGRES_IMAGE'),('redis','REDIS_IMAGE')]:
    container=output(['ps','-q',service])
    record=json.loads(subprocess.check_output(['docker','inspect',container],text=True))[0]
    assert record['Image']==resolved['local_images'][key], 'Image identity mismatch: '+service
    assert record['State'].get('Health',{}).get('Status')=='healthy', 'Unhealthy container: '+service
    assert record['HostConfig']['CgroupParent']=='weknora-production.slice', 'Wrong resource slice'
    assert not record['HostConfig'].get('DeviceRequests') and not record['HostConfig'].get('Devices'), 'Unexpected GPU/device allocation'
    images[service]={'image':record['Image'],'health':'healthy','restart_policy':record['HostConfig']['RestartPolicy']['Name'],'memory_bytes':record['HostConfig']['Memory'],'nano_cpus':record['HostConfig']['NanoCpus']}
result={'origin':origin,'release':manifest['tag'],'commit':manifest['commit'],'frontend_and_assets':'PASS','deployment_readiness':'PASS','anydoc_self_test':'PASS','nextcloud_metrics':'PASS','admin_login':'PASS','directory_administration':'PASS','ldap_runtime':ldap_runtime,'authenticated_database_request':'PASS','docreader_grpc':'PASS','registration_closed':'PASS','anonymous_access_denied':'PASS','migration_gate':'PASS','nextcloud_routes':nextcloud_routes,'nextcloud_business_pairing':'NOT_ACCEPTED_BY_THIS_CHECK','retention_journal':retention_journal,'images':images}
print(json.dumps(result,indent=2))
