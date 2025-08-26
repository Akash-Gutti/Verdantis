# Verdantis Alerts — Model Card (Streaming & Scoring)

_Generated: 2025-08-26T09:28:45.375690+00:00Z_

## Overview
Streaming alerts ingest events, filter and deduplicate them (M10), and publish to channels and role portals (M11). This card summarizes current metrics and evaluation signals (M12.3).

## Key Metrics
| Metric | Value |
|---|---|
| Build info | verdantis_build_info |
| Events (total) | 6 |
| Events (unmatched) | 3 |
| Dedupe kept | 3 |
| Dedupe suppressed | 0 |
| Channels sent | 4 |
| Channels skipped | 0 |
| RAG micro F1 | 0.3333 |
| RAG NLI acc | 0.6667 |
| Causal ΔRMSE mean | 0.0513 |
| Change p@10 | 0.8 |
| CI lint ok | True |
| CI tests ok | True |
| Bundle files | 176 |
| Git commit | cad74c5 |
| Git branch | main |

## Intended Use & Limitations
- **Use**: Operational monitoring of material events and risk signals.
- **Limits**: Sample datasets; stubs for channels; evaluation sizes are small.
- **Safety**: PII is masked in Public portal; tokens gate role data.
