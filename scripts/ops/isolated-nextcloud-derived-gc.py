#!/usr/bin/env python3
"""Read-only source probe of actual local derived pipeline + RunDue on owned SQLite/stock PG/native pgvector."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import time

ROOT=Path(__file__).resolve().parents[2]
STOCK='postgres@sha256:20edbde7749f822887a1a022ad526fde0a47d6b2be9a8364433605cf65099416'
NATIVE='paradedb/paradedb@sha256:58bf87d2a6f1e72f56590b0f0ae1467e82c3f99ca203dde646672c8ac13148b2'
GO='weknora-go-test@sha256:5a74e8b5137496d3e22c72c01d862c4cbb1fe6ce476ddf5616fd9a807d6ae9ac'
def run(*args,timeout=600):
 r=subprocess.run(args,capture_output=True,text=True,timeout=timeout)
 if r.returncode:raise RuntimeError(args[0]+' failed, exit '+str(r.returncode))
 return r.stdout.strip()
def write(path,value):
 path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n');path.chmod(0o600)
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--evidence-dir',type=Path,required=True);parser.add_argument('--native-vector',action='store_true');a=parser.parse_args();os.umask(0o077)
 PG=NATIVE if a.native_vector else STOCK
 output=a.evidence_dir
 if not output.is_absolute() or output.parent.resolve()!=output.parent:raise RuntimeError('absolute output without symlink ancestors required')
 output.mkdir(mode=0o700)
 contexts=json.loads(run('docker','context','inspect'));assert contexts[0]['Endpoints']['docker']['Host'].startswith('unix://')
 assert not os.environ.get('DOCKER_HOST') or os.environ['DOCKER_HOST'].startswith('unix://')
 commit=run('git','-C',str(ROOT),'rev-parse','HEAD');before=run('git','-C',str(ROOT),'status','--porcelain=v1','--untracked-files=all');assert not before,'probe source must be frozen and clean'
 run('docker','stats','--no-stream','--format','{{.Name}} {{.CPUPerc}} {{.MemUsage}}')
 nonce=secrets.token_hex(8);name='gc-derived-'+nonce;label='org.weknora.gc.derived.owner='+nonce
 network=None;pg=None;go=None;result={'schema_version':1,'actual_source_commit':commit,'source_read_only':True,'source_initially_clean':True,'production_coverage_inserted':False,'shared_target_modified':False,'native_pgvector':a.native_vector,'audit_fulltext_purge_complete':False,'external_derived_cleanup_complete':False,'status':'INCOMPLETE'}
 try:
  network=run('docker','network','create','--internal','--label',label,name)
  password='x'+secrets.token_urlsafe(22)
  pg=run('docker','run','-d','--name',name+'-pg','--label',label,'--cpus','1','--memory','512m','--network',network,'--network-alias','gc-db','--tmpfs','/var/lib/postgresql/data:size=256m,mode=700','-e','POSTGRES_DB=gc','-e','POSTGRES_USER=gc','-e','POSTGRES_PASSWORD='+password,PG)
  for _ in range(90):
   ready=subprocess.run(['docker','exec',pg,'pg_isready','-h','127.0.0.1','-U','gc','-d','gc'],capture_output=True)
   if ready.returncode==0:break
   time.sleep(.5)
  else:raise RuntimeError('owned TCP PostgreSQL did not become ready')
  inspected=json.loads(run('docker','inspect',pg))[0];assert inspected['HostConfig']['Memory']==512*1024**2 and inspected['HostConfig']['NanoCpus']==10**9 and not inspected['HostConfig']['PortBindings'];assert json.loads(run('docker','network','inspect',network))[0]['Internal']
  dsn='host=gc-db user=gc dbname=gc password='+password+' sslmode=disable'
  tests='^Test(NextcloudLocalDerivedActualSQLitePipeline|PostgresNextcloudLocalDerivedActualPipeline|LocalGC|LocalCapacity|RetrieveReturns|VectorRetrieveFiltersBeforeTopK|NextcloudActualGC|PostgresNextcloudActualGC|NextcloudActualRunDue|PostgresNextcloudActualRunDue|NextcloudGC|PostgresNextcloudGC|NextcloudExactChunkGC|PostgresNextcloudExactChunkGC|NextcloudContentLease|PostgresNextcloudContentLease|SQLite.*Migration)'
  if a.native_vector:tests='^TestPostgresNextcloudLocalDerivedActualPipeline$'
  go=run('docker','run','-d','--name',name+'-go','--label',label,'--cpus','1','--memory','2g','--network',network,'-v',str(ROOT)+':/app:ro','-v','weknora-go-mod:/go/pkg/mod:ro','-v','weknora-go-build:/root/.cache/go-build','-w','/app','-e','WEKNORA_LEASE_TEST_POSTGRES_DSN='+dsn,'-e','WEKNORA_GC_TEST_POSTGRES_DSN='+dsn,'-e','WEKNORA_AUTO_TAG_TEST_POSTGRES_DSN='+dsn,'-e','WEKNORA_LOCAL_DERIVED_NATIVE_VECTOR='+('1' if a.native_vector else '0'),'-e','GOMAXPROCS=2','-e','GOTOOLCHAIN=local','-e','GOPROXY=off',GO,'go','test','-p','1','-tags','sqlite_fts5','./internal/application/repository','./internal/application/repository/retriever/sqlite','./internal/application/service/file','./internal/application/service','./internal/database','-run',tests,'-count=1','-v')
  go_inspected=json.loads(run('docker','inspect',go))[0];assert go_inspected['HostConfig']['Memory']==2*1024**3 and go_inspected['HostConfig']['NanoCpus']==10**9 and not go_inspected['HostConfig']['PortBindings'];assert any(m['Destination']=='/app' and m['Type']=='bind' and not m['RW'] for m in go_inspected['Mounts'])
  exit_code=run('docker','wait',go)
  logged=subprocess.run(['docker','logs',go],capture_output=True,text=True,timeout=30);logs=(logged.stdout+logged.stderr).replace(password,'[redacted]');logpath=output/'tests.log';logpath.write_text(logs);logpath.chmod(0o600)
  remaining=run('docker','exec',pg,'psql','-U','gc','-d','gc','-At','-c',"SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'content_lease_%' OR nspname LIKE 'gc_bind_%' OR nspname LIKE 'gc_chunk_%' OR nspname LIKE 'auto_tag_%'");assert remaining=='0'
  result.update({'status':'PASS' if exit_code=='0' else 'FAIL','pg_container_id':pg,'go_container_id':go,'network_id':network,'postgres_image':PG,'go_image':GO,'limits':{'postgres':'1 CPU/512 MiB/tmpfs','go':'1 CPU/2 GiB'},'internal_network':True,'host_ports':False,'remaining_nonce_schemas':0,'sqlite_vec_module':'actual sqlite_vec.Auto()/FTS5','pipeline':'actual signed source fixture + input provenance + Composite/KVHybrid/model + local provider/catalog + RunDue','tests_log_sha256':hashlib.sha256(logs.encode()).hexdigest()})
 finally:
  for cid in (go,pg):
   if cid:
    item=json.loads(run('docker','inspect',cid))[0];assert item['Config']['Labels']['org.weknora.gc.derived.owner']==nonce;run('docker','rm','-f',cid)
  if network:
   item=json.loads(run('docker','network','inspect',network))[0];assert item['Labels']['org.weknora.gc.derived.owner']==nonce and not item['Containers'];run('docker','network','rm',network)
  result['cleanup_verified']=True;result['source_finally_clean']=not run('git','-C',str(ROOT),'status','--porcelain=v1','--untracked-files=all');result['source_commit_unchanged']=commit==run('git','-C',str(ROOT),'rev-parse','HEAD');assert result['source_finally_clean'] and result['source_commit_unchanged'];write(output/'evidence.json',result)
 print(json.dumps({'status':result['status'],'evidence':str(output/'evidence.json')}));return 0 if result['status']=='PASS' else 1
if __name__=='__main__':raise SystemExit(main())
