import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def snapshot(source, root):
    if (source.resolve(strict=True) != source or root.parent.resolve(strict=True) != root.parent
            or root.is_relative_to(Path(str(source) + '.d'))):
        raise ValueError('noncanonical or overlapping snapshot location')
    directory = Path(str(source) + '.d')

    def inventory():
        if any(os.path.lexists(str(source) + suffix) for suffix in ('-wal', '-shm', '-journal')):
            raise ValueError('source has SQLite companions')
        result = {}
        for path in (source, directory, *directory.rglob('*')):
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                valid = path == directory or path.parent == directory and re.fullmatch('[0-9a-f]{2}', path.name)
            else:
                valid = (stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                         and (path.suffix == '.sqlite' or path == directory / '.DS_Store'))
            if not valid:
                raise ValueError('unexpected member layout entry')
            result[path] = info
        return result

    with source.open('rb') as fence:
        fcntl.lockf(fence, fcntl.LOCK_SH | fcntl.LOCK_NB, 512, 0x40000000)
        original = inventory()
        if not os.path.samestat(os.fstat(fence.fileno()), original[source]):
            raise ValueError('entrance changed during locking')
        root.mkdir(mode=0o700)
        subprocess.run(['/bin/cp', '-cRp', source, directory, root], check=True)
        records = {}
        finder = None
        for path, expected in original.items():
            relative = path.relative_to(source.parent)
            copy = root / relative
            actual = copy.lstat()
            if actual.st_dev != expected.st_dev or actual.st_ino == expected.st_ino:
                raise ValueError('snapshot identity mismatch')
            copy.chmod(0o700 if stat.S_ISDIR(expected.st_mode) else 0o600)
            if stat.S_ISREG(expected.st_mode):
                if actual.st_size != expected.st_size or not stat.S_ISREG(actual.st_mode):
                    raise ValueError('snapshot content mismatch')
                proof = dict(name=str(relative), bytes=actual.st_size, sha256=digest(copy))
                # Closing another fd on the entrance would release our POSIX process lock.
                if path == source:
                    original_hash = hashlib.sha256(fence.read()).hexdigest()
                else:
                    original_hash = digest(path)
                if proof['sha256'] != original_hash:
                    raise ValueError('snapshot digest mismatch')
                if path == directory / '.DS_Store':
                    finder = proof
                else:
                    records[str(relative)] = proof
        after = inventory()
        if after.keys() != original.keys() or any(not os.path.samestat(info, after[path]) or
                (info.st_size, info.st_mtime_ns, info.st_ctime_ns) !=
                (after[path].st_size, after[path].st_mtime_ns, after[path].st_ctime_ns)
                for path, info in original.items()):
            raise ValueError('source changed during snapshot')
        with os.fdopen(os.open(root / 'snapshot.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as receipt:
            json.dump(dict(files=records, finder=finder), receipt, sort_keys=True)
            receipt.flush()
            os.fsync(receipt.fileno())


if __name__ == '__main__':
    snapshot(*map(Path, sys.argv[1:]))
