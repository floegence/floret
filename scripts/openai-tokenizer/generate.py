#!/usr/bin/env python3
"""Regenerate embedded official ranks from hash-verified local downloads."""
import gzip
import hashlib
from pathlib import Path
import sys

SOURCES = {
    "cl100k_base": "223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7",
    "o200k_base": "446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d",
}
root = Path(__file__).resolve().parents[2] / "internal/openaitokenizer"
for name, expected in SOURCES.items():
    data = (Path(sys.argv[1]) / (name + ".tiktoken")).read_bytes()
    if hashlib.sha256(data).hexdigest() != expected:
        raise SystemExit("Official vocabulary checksum mismatch: " + name)
    (root / (name + ".tiktoken.gz")).write_bytes(gzip.compress(data, mtime=0))
