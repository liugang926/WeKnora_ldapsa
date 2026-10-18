#!/usr/bin/env python3
"""One-time private bootstrap; never print credentials or tokens."""
import json, pathlib, subprocess
base = pathlib.Path('/srv/weknora-production')
envfile = base/'secrets/app.env'
assert 'BIND_IP=127.0.0.1\n' in envfile.read_text(), 'Bootstrap must remain loopback-only'
release = (base/'current').resolve()
cmd = ['docker', 'compose', '--env-file', str(envfile), '--env-file', str(release/'images.env'), '-f', str(release/'production/compose.yaml')]
credentials = json.loads((base/'secrets/admin.json').read_text())
def request(path, payload):
    result = subprocess.run(cmd+['exec','-T','app','curl','-fsS','-H','Content-Type: application/json','--data-binary','@-', 'http://localhost:8080/api/v1/'+path], input=json.dumps(payload), text=True, capture_output=True)
    if result.returncode: raise RuntimeError('Bootstrap API failed: '+path)
    return json.loads(result.stdout)
request('auth/register', credentials)
envfile.write_text(envfile.read_text().replace('DISABLE_REGISTRATION=false\n','DISABLE_REGISTRATION=true\n'))
subprocess.run(cmd+['up','-d','--pull','never','--wait','--wait-timeout','420','app'],check=True)
request('auth/login', {'email':credentials['email'],'password':credentials['password']})
(base/'BOOTSTRAPPED').touch(mode=0o600)
print('Admin created and login verified; registration closed. Credentials remain in secrets/admin.json.')
