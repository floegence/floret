# DeepSeek V4 request estimation

The offline vocabulary is derived from DeepSeek's [official tokenizer archive](https://cdn.deepseek.com/api-docs/deepseek_v4_tokenizer.zip), linked by its [token usage documentation](https://api-docs.deepseek.com/quick_start/token_usage). The source `tokenizer.json` SHA-256 is `89085f12ef79460ac5f66d1119325ddfc694b4ab209d80bbd81d35f081dc9614`.

The vocabulary uses the MIT license included in `internal/deepseektokenizer/LICENSE`, published at DeepSeek-V4-Flash revision `60d8d70770c6776ff598c94bb586a859a38244f1`. Regenerate with `python3 scripts/deepseek-tokenizer/generate.py /path/to/deepseek_v4_tokenizer.zip`. The generator rejects a changed source and emits deterministic compressed data. No model calls, downloads, Python, or native extensions are required at runtime.

The Go counter uses the official sequential isolated pre-tokenizer splits and byte BPE ranks. Reference tests were generated with Hugging Face `tokenizers`, without added special tokens. Spans longer than 512 runes are split to bound work on untrusted tool output. Special-token-looking user text is counted as ordinary text.

Preparation counts the complete rendered JSON, replacing only actual image transport URLs with their documented 1024-token image budget. It adds 10% text headroom. This is a conservative estimate, not an exact server count: server prompt templates, tool markup, image processing and long-span boundaries can differ. It does not change the transmitted body or fingerprint. Native usage plus a validated complete-request estimate delta remains the preferred pressure signal; native overflow still triggers recovery. The source version changes when the counting method changes so old byte estimates cannot calibrate the new tokenizer.
