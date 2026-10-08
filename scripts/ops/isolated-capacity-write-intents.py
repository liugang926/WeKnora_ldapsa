#!/usr/bin/env python3
"""Run real local original-file capacity crash/retry tests in owned stock PG.

Creates only nonce-labelled internal/no-port PG and Go fixtures and preserves
existing module/build caches. The suite creates and removes nonce schemas.
"""
import json,secrets,subprocess,time,os
from pathlib import Path
root=Path(__file__).resolve().parents[2]
nonce=secrets.token_hex(8);name='capacity-intent-'+nonce;label='org.weknora.capacity.intent.owner='+nonce
pgimage='postgres@sha256:20edbde7749f822887a1a022ad526fde0a47d6b2be9a8364433605cf65099416'
goimage='weknora-go-test@sha256:5a74e8b5137496d3e22c72c01d862c4cbb1fe6ce476ddf5616fd9a807d6ae9ac'
def run(*args):
 r=subprocess.run(args,capture_output=True,text=True,timeout=600)
 if r.returncode:raise RuntimeError(args[0]+' failed, exit '+str(r.returncode))
 return r.stdout.strip()
contexts=json.loads(run('docker','context','inspect'));assert contexts[0]['Endpoints']['docker']['Host'].startswith('unix://')
assert not os.environ.get('DOCKER_HOST') or os.environ['DOCKER_HOST'].startswith('unix://')
run('docker','stats','--no-stream','--format','{{.Name}} {{.CPUPerc}} {{.MemUsage}}')
network=run('docker','network','create','--internal','--label',label,name)
pg=None;go=None
try:
 password='x'+secrets.token_urlsafe(20)
 pg=run('docker','run','-d','--name',name+'-pg','--label',label,'--cpus','1','--memory','512m','--network',network,'--network-alias','capacity-db','--tmpfs','/var/lib/postgresql/data:size=256m,mode=700','-e','POSTGRES_USER=capacity','-e','POSTGRES_DB=capacity','-e','POSTGRES_PASSWORD='+password,pgimage)
 for i in range(60):
  ready=subprocess.run(['docker','exec',pg,'pg_isready','-U','capacity','-d','capacity'],capture_output=True)
  if ready.returncode==0:break
  time.sleep(.5)
 else:raise RuntimeError('owned PG not ready')
 inspect=json.loads(run('docker','inspect',pg))[0];assert inspect['HostConfig']['Memory']==512*1024**2 and inspect['HostConfig']['NanoCpus']==10**9 and not inspect['HostConfig']['PortBindings']
 assert json.loads(run('docker','network','inspect',network))[0]['Internal'] is True
 dsn='host=capacity-db user=capacity dbname=capacity password='+password+' sslmode=disable'
 script='gofmt -w cmd/capacity-reconcile/main.go internal/application/repository/nextcloud_capacity_intents.go internal/application/repository/nextcloud_capacity.go internal/application/service/nextcloud_capacity_intent_test.go && go build -buildvcs=false -p 1 -o /tmp/capacity-reconcile ./cmd/capacity-reconcile && WEKNORA_CAPACITY_RECONCILE_CLI=/tmp/capacity-reconcile go test -p 1 ./internal/application/service ./internal/application/repository ./internal/application/service/file ./internal/database -run "Test(CapacityIntent|PostgresCapacityIntent|CapacityCrashChild|NextcloudCapacity|PostgresNextcloudCapacity|SQLite|MigrationDirectoriesLoad|Local|ResourceCatalog|BackendScoped|CreateKnowledgeFromFile)" -count=1 -v'
 go=run('docker','run','-d','--name',name+'-go','--label',label,'--cpus','1','--memory','2g','--network',network,'-v',str(root)+':/app','-v','weknora-go-mod:/go/pkg/mod:ro','-v','weknora-go-build:/root/.cache/go-build','-w','/app','-e','WEKNORA_CAPACITY_TEST_POSTGRES_DSN='+dsn,'-e','WEKNORA_LEASE_TEST_POSTGRES_DSN='+dsn,goimage,'sh','-c',script)
 exit_code=run('docker','wait',go)
 logs=subprocess.run(['docker','logs',go],capture_output=True,text=True)
 out=(logs.stdout+logs.stderr).replace(password,'[redacted]')
 Path('/tmp/capacity-intent-owned-tests-20261009.log').write_text(out)
 remaining=run('docker','exec',pg,'psql','-U','capacity','-d','capacity','-At','-c',"SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'capacity_intent_%' OR nspname LIKE 'content_lease_%'")
 assert remaining=='0'
 evidence={'status':'PASS' if exit_code=='0' else 'FAIL','parent':'a7680b69f2a65859eb256b25cdb0f9c28cd89208','go_image':goimage,'postgres_image':pgimage,'go_container_id':go,'pg_container_id':pg,'network_id':network,'internal_network':True,'host_ports':False,'limits':{'go':'1 CPU/2 GiB','postgres':'1 CPU/512 MiB/tmpfs'},'remaining_nonce_schemas':int(remaining),'physical_source_files':'real local filesystem files, full uploads and partial files retained by original intent','crash':'actual subprocess os.Exit(77) without End cleanup, SQLite and PostgreSQL','reconciliation':'actual owner-only capacity-reconcile CLI after the crash','shared_target_modified':False,'tests_log_sha256':__import__('hashlib').sha256(out.encode()).hexdigest()}
 print(json.dumps({'status':evidence['status'],'pg_id':pg,'go_id':go,'remaining_schemas':remaining}))
 if exit_code!='0':print('Focused tests failed; retained private log: /tmp/capacity-intent-owned-tests-20261009.log')
finally:
 for cid in (go,pg):
  if cid:
   item=json.loads(run('docker','inspect',cid))[0];assert item['Config']['Labels']['org.weknora.capacity.intent.owner']==nonce
   run('docker','rm','-f',cid)
 item=json.loads(run('docker','network','inspect',network))[0];assert item['Labels']['org.weknora.capacity.intent.owner']==nonce and not item['Containers'];run('docker','network','rm',network)
 if 'evidence' in globals():
  evidence['cleanup_verified']=True
  path=root/'docs/evidence/capacity-write-intents-20261009.json';path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(evidence,indent=2)+'\n')
