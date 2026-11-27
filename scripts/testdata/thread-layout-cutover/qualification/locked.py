import fcntl
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from clone import identity, offline


def qualify(source, command):
    expected = offline(source)
    descriptor = os.open(source, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        fcntl.lockf(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB, 512, 0x40000000)
        if identity(os.fstat(descriptor)) != expected or offline(source) != expected:
            raise ValueError("source changed before lock acquisition")
        env = dict(os.environ, LOCKED_SOURCE_IDENTITY=":".join(map(str, expected)))
        result = subprocess.run([Path.home() / ".local/bin/floegence-test", *command], env=env)
        if identity(os.fstat(descriptor)) != expected or offline(source) != expected:
            raise ValueError("source changed during qualification")
        return result.returncode
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    sys.exit(qualify(Path(sys.argv[1]), sys.argv[2:]))
