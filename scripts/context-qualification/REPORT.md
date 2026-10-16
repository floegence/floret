# DeepSeek Flash context qualification

24 fixed synthetic requests; eight input classes × three sizes (1, 32, 512 repeats). Each input was sent once before implementation. Both counters use that same recorded native usage. No sample was retried or removed.

| Case | Original distribution | Original budget | New content + framing | New budget | Reserve | Native | Content error |
|---|---:|---:|---:|---:|---:|---:|---:|
| chinese-1 | 238 | 348 | 366 | 377 | 11 | 359 | 1.950% |
| chinese-32 | 1478 | 1549 | 1017 | 1093 | 76 | 1010 | 0.693% |
| chinese-512 | 20678 | 20149 | 11097 | 12181 | 1084 | 11090 | 0.063% |
| code-1 | 247 | 358 | 384 | 397 | 13 | 377 | 1.857% |
| code-32 | 1758 | 1830 | 1593 | 1727 | 134 | 1586 | 0.441% |
| code-512 | 25158 | 24630 | 20313 | 22319 | 2006 | 20306 | 0.034% |
| english-1 | 220 | 330 | 360 | 370 | 10 | 353 | 1.983% |
| english-32 | 878 | 950 | 825 | 882 | 57 | 818 | 0.856% |
| english-512 | 11078 | 10550 | 8025 | 8802 | 777 | 8018 | 0.087% |
| html-1 | 316 | 360 | 386 | 399 | 13 | 378 | 2.116% |
| html-32 | 3958 | 1910 | 1657 | 1797 | 140 | 1649 | 0.485% |
| html-512 | 60358 | 25910 | 21337 | 23445 | 2108 | 21329 | 0.038% |
| json-1 | 258 | 349 | 372 | 383 | 11 | 365 | 1.918% |
| json-32 | 2118 | 1550 | 1209 | 1304 | 95 | 1202 | 0.582% |
| json-512 | 30918 | 20150 | 14169 | 15560 | 1391 | 14162 | 0.049% |
| mixed-1 | 257 | 344 | 370 | 381 | 11 | 363 | 1.928% |
| mixed-32 | 2078 | 1429 | 1145 | 1234 | 89 | 1138 | 0.615% |
| mixed-512 | 30278 | 18229 | 13145 | 14434 | 1289 | 13138 | 0.053% |
| rss-1 | 388 | 370 | 396 | 410 | 14 | 388 | 2.062% |
| rss-32 | 6278 | 2230 | 1977 | 2149 | 172 | 1969 | 0.406% |
| rss-512 | 97478 | 31030 | 26457 | 29077 | 2620 | 26449 | 0.030% |
| tool_output-1 | 242 | 349 | 374 | 386 | 12 | 367 | 1.907% |
| tool_output-32 | 1598 | 1550 | 1273 | 1375 | 102 | 1266 | 0.553% |
| tool_output-512 | 22598 | 20150 | 15193 | 16687 | 1494 | 15186 | 0.046% |

Original distribution absolute relative error: median 37.921%, P95 218.842%, maximum 268.551%.
Original budget absolute relative error: median 16.729%, P95 53.366%, maximum 81.686%.
New content + protocol framing absolute relative error: median 0.568%, P95 2.062%, maximum 2.116%.

All 24 meet the fixed content-error thresholds (median ≤5%, P95 ≤10%, maximum ≤15%). Budget reserve is reported separately and is not included in category shares. Native totals validate overall error only; category tests use tokenizer references and typed source mappings.

The fixture stores synthetic payloads, original counts, hashes and native usage, without credentials. Recompute offline with FLORET_RECORDED_CONTEXT_DIR or run the committed fixture test. Live recording requires explicit environment opt-in and refuses to overwrite cases.

This change does not alter prompts. The earlier prompt qualification result remains 119/120; its remaining failure is outside this context-estimation qualification.
