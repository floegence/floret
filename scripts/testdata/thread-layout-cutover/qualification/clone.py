import ctypes
import os
from pathlib import Path
import stat
import sys


def identity(entry):
    return (entry.st_dev, entry.st_ino, entry.st_size, entry.st_mtime_ns, entry.st_ctime_ns)


def offline(path):
    entry = path.lstat()
    if path.resolve(strict=True) != path or not stat.S_ISREG(entry.st_mode) or entry.st_nlink != 1:
        raise ValueError("invalid source")
    if any(os.path.lexists(str(path) + suffix) for suffix in ("-wal", "-shm", "-journal")):
        raise ValueError("source has SQLite companions")
    return identity(entry)


def clone(source, destinations):
    expected = tuple(int(field) for field in os.environ["LOCKED_SOURCE_IDENTITY"].split(":"))
    root = Path(os.environ["FLOEGENCE_TEST_RUN_DIR"]).resolve(strict=True)
    function = ctypes.CDLL(None, use_errno=True).clonefile
    function.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
    function.restype = ctypes.c_int
    if offline(source) != expected:
        raise ValueError("source differs from locked file")
    for target in destinations:
        if target.parent.resolve(strict=True) != target.parent or not target.is_relative_to(root):
            raise ValueError("invalid destination")
        if function(os.fsencode(source), os.fsencode(target), 0) != 0:
            raise OSError(ctypes.get_errno(), "APFS clone failed")
        target.chmod(0o600)
        copy = target.stat()
        if copy.st_dev != expected[0] or copy.st_ino == expected[1] or copy.st_size != expected[2] or offline(source) != expected:
            raise ValueError("inconsistent clone")


if __name__ == "__main__":
    clone(Path(sys.argv[1]), list(map(Path, sys.argv[2:])))
