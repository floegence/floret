# DeepSeek V4 request estimation

The offline vocabulary is derived from DeepSeek's [official tokenizer archive](https://cdn.deepseek.com/api-docs/deepseek_v4_tokenizer.zip), linked by its [token usage documentation](https://api-docs.deepseek.com/quick_start/token_usage). The source `tokenizer.json` SHA-256 is `89085f12ef79460ac5f66d1119325ddfc694b4ab209d80bbd81d35f081dc9614`.

The vocabulary uses the MIT license included in `internal/deepseektokenizer/LICENSE`, published at DeepSeek-V4-Flash revision `60d8d70770c6776ff598c94bb586a859a38244f1`. Regenerate with `python3 scripts/deepseek-tokenizer/generate.py /path/to/deepseek_v4_tokenizer.zip`. The generator rejects a changed source and emits deterministic compressed data. No model calls, downloads, Python, or native extensions are required at runtime.

The Go counter uses the official sequential isolated pre-tokenizer splits and byte BPE ranks. Reference tests were generated with Hugging Face `tokenizers`, without added special tokens. Spans longer than 512 runes are split to bound work on untrusted tool output. Special-token-looking user text is counted as ordinary text.

Preparation extracts decoded model text from the complete frozen request. Tool schemas are serialized once. Content distribution and conservative budgets share these counts; the 10% text reserve, media bounds and protocol framing are separate. Images retain the documented 1024-token budget; opaque inputs force relative-weight presentation. Special-looking user text is ordinary text.

The trusted tool template in `internal/requestestimate/deepseek_tools.txt` is copied from DeepSeek's MIT-licensed [official encoding implementation](https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash/raw/main/encoding/encoding_dsv4.py), SHA-256 `bdbd57c132a1b3725042323d02b98b9d1df28e5f388f134399555d041f5055e0`. Its special-token-aware reference count is 221. The license is retained next to the template. Protocol boundary allowances are estimates of the documented DSML/message wrappers, not a difference fitted to native usage. Server serialization, V4.1 changes and long-span boundaries can differ; this remains an estimate. Native usage and validated complete-request deltas retain pressure authority. The source version changes to invalidate older calibration.

The [fixed 24-request report](../context-qualification/REPORT.md) retains every original/new result and reserve. Category accuracy is tested with official vocabulary references and source mappings, independently of native total usage.

The regexp2 dependency must retain its idle-clock timeout fix (v1.12.0 or later). Timeout errors omit original input. Regression tests cover the first large count after an idle interval and error sanitization.
