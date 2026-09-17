"""Generate the offline V4 vocabulary from DeepSeek's official tokenizer archive."""
import base64
import gzip
import hashlib
import json
from pathlib import Path
import sys
import zipfile

archive = Path(sys.argv[1]).read_bytes()
with zipfile.ZipFile(__import__('io').BytesIO(archive)) as source:
    raw = source.read('deepseek_v4_tokenizer/tokenizer.json')
    assert hashlib.sha256(raw).hexdigest() == '89085f12ef79460ac5f66d1119325ddfc694b4ab209d80bbd81d35f081dc9614', 'unexpected tokenizer source'
    tokenizer = json.loads(raw)
# Reverse the standard ByteLevel byte-to-Unicode alphabet used by the model.
visible = list(range(33, 127)) + list(range(161, 173)) + list(range(174, 256))
characters = visible[:]
for byte in range(256):
    if byte not in visible:
        visible.append(byte)
        characters.append(256 + len(characters) - 188)
alphabet = dict(zip(map(chr, characters), visible))
specials = {item['content'] for item in tokenizer['added_tokens']}
ranks = {}
for token, rank in tokenizer['model']['vocab'].items():
    if token not in specials:
        ranks[base64.b64encode(bytes(alphabet[c] for c in token)).decode()] = rank
output = {
    'source_sha256': hashlib.sha256(raw).hexdigest(),
    'patterns': [part['pattern']['Regex'] for part in tokenizer['pre_tokenizer']['pretokenizers'] if part['type'] == 'Split'],
    'ranks': ranks,
}
root = Path(__file__).resolve().parents[2]
(root / 'internal/deepseektokenizer/v4.json.gz').write_bytes(gzip.compress(json.dumps(output, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode(), mtime=0))
print(output['source_sha256'])
