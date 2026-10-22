#!/usr/bin/env python3
"""Pull only complete, published prod-* releases from the configured public fork."""
import argparse, hashlib, json, os, pathlib, re, shutil, subprocess, tarfile, tempfile, time, urllib.request
from validate_release import validate
from parallel_download import download_parallel
from image_identity import resolve_images
BASE = pathlib.Path('/srv/weknora-production')
REPO = 'liugang926/WeKnora_ldapsa'

def get(url, accept='application/vnd.github+json'):
    return urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'weknora-production-updater', 'Accept': accept}), timeout=90)

def asset_url(asset):
    url = asset.get('url')
    if not isinstance(url, str) or not re.fullmatch(r'https://api\.github\.com/repos/' + re.escape(REPO) + r'/releases/assets/[0-9]+', url):
        raise RuntimeError('Untrusted asset URL')
    return url

def download_asset(url, destination, expected_size):
    # Metadata is small and fetched afresh. Commit it only after an exact,
    # bounded read so a failed attempt cannot replace previously valid data.
    if not isinstance(expected_size, int) or not 0 <= expected_size <= 16*1024*1024:
        raise ValueError('Invalid metadata size')
    for attempt in range(3):
        temporary = None
        try:
            with get(url, accept='application/octet-stream') as response:
                content = response.read(expected_size + 1)
            if len(content) != expected_size:
                raise RuntimeError('Metadata size mismatch')
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as output:
                temporary = pathlib.Path(output.name)
                output.write(content)
                output.flush(); os.fsync(output.fileno())
            os.replace(temporary, destination)
            return
        except Exception:
            if attempt == 2:
                raise RuntimeError('Metadata download failed: '+destination.name) from None
            time.sleep(0.5 * (attempt+1))
        finally:
            if temporary is not None: temporary.unlink(missing_ok=True)

def read_checksums(path):
    hashes = {}
    for line in path.read_text().splitlines():
        digest, name = line.split(None, 1); name = name.lstrip('*')
        if not re.fullmatch('[a-f0-9]{64}', digest) or '/' in name:
            raise RuntimeError('Invalid checksum record')
        hashes[name] = digest
    return hashes

def file_sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()

def cached_asset_valid(path, expected_digest):
    if not path.is_file() or not expected_digest:
        return False
    return file_sha256(path) == expected_digest

def main(stage_only=False):
    if (BASE/'ACTIVATION_PENDING').exists():
        (BASE/'UPDATE_FAILED').touch(mode=0o600)
        raise RuntimeError('Interrupted activation requires recovery; original backup pointer retained')
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
        scratch = pathlib.Path('/data/weknora-production/downloads')/tag
        scratch.mkdir(mode=0o700, exist_ok=True)
        try:
            hashes = {}
            for name in ['SHA256SUMS', 'manifest.json', 'deployment.tar.gz'] + parts:
                url = asset_url(assets[name])
                if name not in parts and name != 'SHA256SUMS' and cached_asset_valid(scratch/name, hashes.get(name)):
                    continue
                if name in parts:
                    download_parallel(url, scratch/name, assets[name]['size'])
                else:
                    download_asset(url, scratch/name, assets[name]['size'])
                if name == 'SHA256SUMS':
                    hashes = read_checksums(scratch/name)
            for name in ['manifest.json', 'deployment.tar.gz'] + parts:
                digest = file_sha256(scratch/name)
                if hashes.get(name) != digest:
                    (scratch/name).unlink()
                    raise RuntimeError('Checksum mismatch: '+name)
            manifest = validate(json.loads((scratch/'manifest.json').read_text()))
            if manifest['tag'] != tag: raise RuntimeError('Tag mismatch')
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
        except Exception:
            # Keep partial downloads for the next retry, without touching live data.
            raise
    # Recheck cached releases too: a previously downloaded older release must
    # never bypass the schema downgrade gate when republished.
    manifest = validate(json.loads((target/'manifest.json').read_text()))
    if manifest.get('deployment_protocol', 1) >= 2 and not (BASE/'bin/retention.py').is_file():
        raise RuntimeError('Reviewed original-body retention helper must be installed before activation')
    active = BASE/'current'/'manifest.json'
    if active.exists():
        # Releases published before docreader was introduced are valid comparison
        # baselines, but must never be accepted as the target release.
        old = validate(json.loads(active.read_text()), allow_legacy_images=True)
        comparison = json.load(get(f"https://api.github.com/repos/{REPO}/compare/{old['commit']}...{manifest['commit']}"))
        if comparison.get('status') not in ('ahead', 'identical'):
            raise RuntimeError('Automatic database downgrade/divergent release refused')
    resolved = resolve_images(manifest)
    (target/'resolved-images.json').write_text(json.dumps(resolved, indent=2)+'\n')
    (target/'images.env').write_text(''.join(k+'='+v+'\n' for k,v in sorted(resolved['local_images'].items())))
    if stage_only:
        print(json.dumps({'staged_release': tag, 'activated': False}))
        return
    subprocess.run(['bash', str(BASE/'bin/activate.sh'), str(target)], check=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage-only', action='store_true', help='Verify and load a release without changing services or the database')
    main(stage_only=parser.parse_args().stage_only)
