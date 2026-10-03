# a-stock-data live capability audit — 2026-10-01

## Scope and method

Source checkout: `D:\Project\PythonProgram\external\a-stock-data`, commit `f814dcfe209dd7958f4858f9d878d591ee85fb56`.

The capability inventory has 87 counted endpoint capabilities and 2 explanatory rows (the EastMoney industry-report parameter variant and the local northbound-history cache). The project-wide offline suite completed with 165 passes, 3 opt-in live groups skipped, and no failures/errors. Offline contract tests establish parser and error behavior; they do not prove current provider availability.

The opt-in V3.9 and V3.10 live suites were run with `RawResponseArchive` intercepting `requests` at `Session.send`, before response parsing. Every retained response body is gzip-compressed and hash-addressed; the manifest records sanitized URL/headers, time, HTTP status, byte count, SHA-256, test scope, or the transport error. Rechecking all retained body hashes found zero integrity failures.

## Live results

| Release group | Test result | Captured HTTP events | Saved exact response bodies | Finding |
|---|---:|---:|---:|---|
| V3.9 additions (25 capability cases) | 115 total suite tests; 1 live subtest error | 54 | 50; 4 transport errors | 24 capability cases returned usable data; `sse_e_interaction` failed on an unrecognized relative time. |
| V3.10 additions (2 capability cases) | 26 tests; all passed | 69 | 69 | Tencent ticks and futures daily K-line returned data with provenance. |

The V3.9 error is reproducible from the archived source page: a reply-time field is rendered as “今天 18:18” (“today, 18:18”), while the parser accepts absolute date/time values. The endpoint returned HTTP 200, but parsing failed loudly instead of presenting malformed data as a valid empty result. Raw page hash: `58e61372a553f03b019257b18913d4a5b11dee80311098bd07779d857f13640e`.

V3.9 response and transport evidence:

- Raw manifest: [manifest.ndjson](../results/raw/2026-10-01-v39-live-escalated/manifest.ndjson)
- Test output: [live test log](../results/legacy/2026-10-01-v39-live-escalated.log)
- Bodies: `../results/raw/2026-10-01-v39-live-escalated/bodies/`

V3.10 response evidence:

- Raw manifest: [manifest.ndjson](../results/raw/2026-10-01-v310-live/manifest.ndjson)
- Test output: [live test log](../results/legacy/2026-10-01-v310-live.log)
- Bodies: `../results/raw/2026-10-01-v310-live/bodies/`

## Coverage of the 87 published capabilities

| Audit state | Count | Meaning |
|---|---:|---|
| Verified live with raw retained | 26 | 24 V3.9 cases plus both V3.10 cases passed a real-data test, and exact response bodies are archived. |
| Live response archived, parser rejected it | 1 | SSE interaction currently has a source-format incompatibility (`今天 18:18`); not usable for that sampled record until parsing supports it. |
| Historical live evidence, raw not retained | 7 | Six V3.8 capabilities and F10 have dated real-data notes in the project docs, but their original payloads cannot be inspected. They were not called again only to recreate missing archives. |
| Documented only; no current live evidence found | 51 | README/SKILL describes the capability, but this audit did not find a reviewable live payload or a dedicated current live test for it. |
| Documented unavailable or partial | 1 | The archived mootdx market commands return empty data since 2026-09. README says finance/F10 is a separate capability. |
| Requires credential; not live checked | 1 | iwencai NL search requires an API key; no key was supplied. |

The inventory also retains 2 non-endpoint rows: the EastMoney industry-report qType variant was not separately requested during this run, and the northbound-history row is a local cache behavior that needs a separate persistence test.

## Decision

This audit does **not** certify all 87 capabilities as currently usable. It certifies 26 with current archived live evidence, records 1 concrete parser incompatibility, and makes the remaining gaps explicit in [the capability inventory](../coverage/a-stock-data-capability-inventory.csv). The older V3.8 evidence and all pre-V3.8 live claims lack original payloads, so they remain historical/documented evidence rather than current verification.

No code in the read-only upstream checkout was changed. The SSE issue is recorded for a targeted fix in the consumer copy; the archived page makes that fix reviewable without requesting the provider again.
