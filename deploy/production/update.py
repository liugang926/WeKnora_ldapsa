#!/usr/bin/env python3
"""Pull only complete, published prod-* releases from the configured public fork."""
import hashlib, json, os, pathlib, re, shutil, subprocess, tarfile, tempfile, urllib.request
from validate_release import validate
BASE = pathlib.Path('/srv/weknora-production')
REPO = 'liugang926/WeKnora_ldapsa'

def get(url):
    return urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'weknora-production-updater', 'Accept': 'application/vnd.github+json'}), timeout=90)

def main():
    if (BASE/'UPDATE_FAILED').exists():
        raise RuntimeError('Previous deployment failed; operator must resolve and remove UPDATE_FAILED')
    releases = json.load(get(f'https://api.github.com/repos/{REPO}/releases?per_page=30'))
    eligible = [r for r in releases if not r['draft'] and not r['prerelease'] and re.fullmatch(r'prod-[A-Za-z0-9._-]+', r['tag_name'])]
    if not eligible: return
    r = max(eligible, key=lambda r: r['published_at'])
    tag = r['tag_name']
    if (BASE/'active-release').exists() and (BASE/'active-release').read_text().strip() == tag: return
    target = BASE/'releases'/tag
    if not target.exists():
        if shutil.disk_usage('/data').free < 30*1024**3: raise RuntimeError('Less than 30 GiB free on /data')
        assets = {a['name']: a for a in r['assets']}
        for name in ['SHA256SUMS', 'manifest.json', 'deployment.tar.gz']:
            if name not in assets: raise RuntimeError('Incomplete release')
        parts = sorted(n for n in assets if re.fullmatch(r'images.tar.gz.part[0-9]{2}', n))
        if not parts or parts != [f'images.tar.gz.part{i:02}' for i in range(len(parts))]:
            raise RuntimeError('Missing image archive parts')
        scratch = pathlib.Path(tempfile.mkdtemp(prefix='release-', dir='/data/weknora-production/downloads'))
        try:
            for name in ['SHA256SUMS', 'manifest.json', 'deployment.tar.gz'] + parts:
                url = assets[name]['browser_download_url']
                if not url.startswith(f'https://github.com/{REPO}/releases/download/{tag}/'): raise RuntimeError('Untrusted asset URL')
                with get(url) as response, open(scratch/name, 'wb') as output: shutil.copyfileobj(response, output)
            hashes = {}
            for line in (scratch/'SHA256SUMS').read_text().splitlines():
                digest, name = line.split(None, 1); name = name.lstrip('*')
                if not re.fullmatch('[a-f0-9]{64}', digest) or '/' in name: raise RuntimeError('Invalid checksum record')
                hashes[name] = digest
            for name in ['manifest.json', 'deployment.tar.gz'] + parts:
                with open(scratch/name, 'rb') as f: digest = hashlib.file_digest(f, 'sha256').hexdigest()
                if hashes.get(name) != digest: raise RuntimeError('Checksum mismatch: '+name)
            manifest = validate(json.loads((scratch/'manifest.json').read_text()))
            if manifest['tag'] != tag: raise RuntimeError('Tag mismatch')
            active = BASE/'current'/'manifest.json'
            if active.exists():
                old = validate(json.loads(active.read_text()))
                comparison = json.load(get(f"https://api.github.com/repos/{REPO}/compare/{old['commit']}...{manifest['commit']}"))
                if comparison.get('status') not in ('ahead', 'identical'):
                    raise RuntimeError('Automatic database downgrade/divergent release refused')
            # The complete image stream is loaded without an intermediate uncompressed archive.
            with open(scratch/'images.tar.gz', 'wb') as out:
                for name in parts:
                    with open(scratch/name, 'rb') as inp: shutil.copyfileobj(inp, out)
                    (scratch/name).unlink()
            subprocess.run(['docker', 'load', '-i', str(scratch/'images.tar.gz')], check=True)
            with tarfile.open(scratch/'deployment.tar.gz') as archive:
                for member in archive.getmembers():
                    p = pathlib.PurePosixPath(member.name)
                    if p.is_absolute() or '..' in p.parts or not p.parts or p.parts[0] != 'production' or not (member.isfile() or member.isdir()):
                        raise RuntimeError('Unsafe deployment archive')
                archive.extractall(scratch, filter='data')
            (scratch/'images.env').write_text(''.join(k+'='+v+'\n' for k,v in sorted(manifest['images'].items())))
            (scratch/'images.tar.gz').unlink()
            shutil.move(str(scratch), str(target))
        finally:
            if scratch.exists(): shutil.rmtree(scratch)
    subprocess.run(['bash', str(BASE/'bin/activate.sh'), str(target)], check=True)

if __name__ == '__main__': main()
