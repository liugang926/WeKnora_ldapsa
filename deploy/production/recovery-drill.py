#!/usr/bin/env python3
"""First-install restart drill. Refuses to stop a publicly bound stack."""
import json,os,pathlib,subprocess,uuid
base=pathlib.Path('/srv/weknora-production');release=(base/'current').resolve()
assert (base/'BOOTSTRAPPED').exists(), 'Bootstrap must be complete'
assert 'BIND_IP=127.0.0.1\n' in (base/'secrets/app.env').read_text(), 'Drill is private-only'
assert not (base/'ACTIVATION_PENDING').exists() and not (base/'UPDATE_FAILED').exists()
os.environ['COMPOSE_PROGRESS']='plain'
cmd=['docker','compose','--env-file',str(base/'secrets/app.env'),'--env-file',str(release/'images.env'),'-f',str(release/'production/compose.yaml')]
def output(args):return subprocess.check_output(cmd+args,text=True).strip()
def redis(*args):
 return output(['exec','-T','redis','sh','-c','export REDISCLI_AUTH="$(sed -n "s/^requirepass //p" /usr/local/etc/redis/redis.conf)"; exec redis-cli "$@"','redis-cli',*args])
def admins():
 return output(['exec','-T','postgres','psql','-X','-At','-v','ON_ERROR_STOP=1','-U','weknora','-d','weknora','-c','SELECT count(*) FROM users WHERE is_system_admin=true'])
probe=pathlib.Path('/data/weknora-production/runtime/files/.production-persistence-probe')
assert not probe.exists(), 'An earlier probe requires operator review'
value=uuid.uuid4().hex;key='deployment:persistence-probe:'+value
probe.write_text(value);probe.chmod(0o600)
assert redis('SET',key,value,'EX','900')=='OK'
before=admins();assert int(before)>=1
subprocess.run(['systemctl','enable','weknora-production.service'],check=True)
subprocess.run(cmd+['stop','--timeout','90'],check=True)
assert not subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project=weknora-production'],text=True).strip(), 'Not all containers stopped'
subprocess.run(['systemctl','restart','weknora-production.service'],check=True)
assert probe.read_text()==value, 'File data lost'
assert redis('GET',key)==value, 'Redis AOF data lost'
assert admins()==before, 'Database administrator data changed'
result=subprocess.check_output(['python3',str(base/'support/acceptance.py'),'http://127.0.0.1:18080'],text=True)
(base/'acceptance-after-restart.json').write_text(result)
probe.unlink();assert redis('DEL',key)=='1'
evidence={'stopped_all_four_containers':True,'restored_by_systemd':True,'database_admins_persisted':True,'redis_aof_persisted':True,'files_persisted':True,'http_acceptance_after_restart':'PASS'}
(base/'recovery-drill.json').write_text(json.dumps(evidence,indent=2)+'\n')
print(json.dumps(evidence,indent=2))
