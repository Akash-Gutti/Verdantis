# Verdantis v3R — ESG & Risk Digital Twin

Verdantis is a pragmatic, end-to-end reference implementation for real-time ESG/risk monitoring. It ingests material events, filters and deduplicates them, routes alerts to channels, and exposes role-based portals (Regulator, Investor, Public). It ships with first-class observability and a lightweight evaluation harness so you can trust what it’s doing and why.

This repository is designed for product teams who need something reliable, explainable, and shippable—not a throwaway demo.

---

## What’s inside

* **Streaming alerts:** filter subscriptions, rate-limit/dedupe, flapping suppression, channel fan-out, and a clean UI feed.
* **Role-based portals:** token-gated outputs for regulator, investor, and public stakeholders with data masking and configurable visibility.
* **Observability:** Prometheus-style metrics, Loki-style structured logs, and a tiny `/metrics` server for local scraping.
* **Evaluation harness:** RAG veracity (citations + NLI), causal fit (pre/post RMSE + placebo), and change-detection precision\@K.
* **Documentation artifacts:** auto-generated Model & Data Cards populated from live metrics and eval reports.
* **Single entrypoint:** `verdctl` (CLI) orchestrates everything consistently across modules.

---

## How it works (at a glance)

Events land on the bus (or via the included sample generator). Subscriptions and rule filters select what matters. Dedupe and flapping suppression protect recipients from noise while preserving at-least-once delivery. Channels (email/webhook stubs) and a normalized alert feed power the portals. Auth is HMAC-signed and role-aware. Metrics and logs are emitted at each step; a small evaluation harness computes compact, defensible KPIs from synthetic datasets. Model/Data cards summarize the state of the system so you can explain it to auditors, boards, or customers.

---

## Quick start

### Environment

Use **Python 3.10+** with a virtual environment. Optional dev tools: `flake8`, `pytest`.

### Generate sample data

Run the sample event and evaluation data generators; then run **filters → dedupe → feed** to see the full pipeline.

### Role tokens

Demo users live in `configs/m11_auth.json`. Create tokens via:

```bash
python scripts/verdctl.py m11 auth-login
```

Tokens are saved under `data/processed/m11/auth/tokens/*.jwt`.

### Portals

Build regulator, investor, and public outputs (each requires its respective token):

```bash
python scripts/verdctl.py m11 reg-build
python scripts/verdctl.py m11 inv-build
python scripts/verdctl.py m11 pub-build
```

Public outputs enforce severity floors and PII masking by policy.

### Observability

```bash
# Export metrics to Prometheus textfile
python scripts/verdctl.py m12 metrics-export

# Create sample logs
python scripts/verdctl.py m12 logs-demo

# Ingest real artefacts into logs
python scripts/verdctl.py m12 logs-ingest

# Serve /metrics locally
python scripts/verdctl.py m12 serve-metrics
```

### Evaluation

```bash
python scripts/verdctl.py m12 eval-rag
python scripts/verdctl.py m12 eval-causal
python scripts/verdctl.py m12 eval-change
python scripts/verdctl.py m12 eval-all
```

Sample datasets live under `data/eval/*`.

### Model/Data Cards

```bash
python scripts/verdctl.py m12 cards-build
```

Outputs land in `docs/cards/`.

### CI

```bash
python scripts/verdctl.py m12 ci-run
```

Runs lint (`flake8`), tests (`pytest`), and produces a reproducible bundle under `dist/`.

> **Tip:** sanity-check each module:
>
> ```bash
> python scripts/verdctl.py verify -m <module>
> # e.g.
> python scripts/verdctl.py verify -m m10
> python scripts/verdctl.py verify -m m11
> python scripts/verdctl.py verify -m m12
> ```

---

## Configuration you’ll actually change

| Area              | File                        | Notes                                                                |
| ----------------- | --------------------------- | -------------------------------------------------------------------- |
| Filters           | `configs/m10_filters.json`  | Topics, severities, assets/AOIs, rule types.                         |
| Dedupe & flapping | `configs/m10_dedupe.json`   | TTL, cooldown, key fields, and flapping window/threshold.            |
| Channels          | `configs/m10_channels.json` | Route to email/webhook stubs and rate limits.                        |
| Auth & roles      | `configs/m11_auth.json`     | Demo users, role capabilities, token TTL defaults.                   |
| Public policy     | `configs/m11_public.json`   | Severity floor, visible fields, asset anonymization, region mapping. |

All config is JSON; changes are picked up on the next CLI run.

---

## Guarantees & design choices

* **At-least-once delivery** with stateful dedupe and cooldowns to reduce spam while preserving correctness on retries/replays.
* **Flapping suppression** detects rapid on/off oscillations and mutes them until stable.
* **Defense-in-depth for privacy:** PII masking in public outputs, strict visible-fields policy, and role-gated access via signed tokens.
* **Deterministic artefacts:** every step writes explicit JSON outputs and metrics so you can diff, audit, and reproduce.
* **Evaluation that fits in a sprint:** compact but meaningful KPIs—micro/macro citation scores, NLI accuracy, ΔRMSE, placebo checks, and precision\@K.

---

## Operating the system

### Common workflows

* **Refresh the streaming pipeline end-to-end:** run filters → dedupe → channels → feed; then rebuild portals.
* **Rotate secrets:** set `PORTALS_AUTH_SECRET` for tokens and `PUBLIC_MASK_SECRET` for public pseudonyms; re-issue tokens after changes.
* **Reset dedupe state:** remove `data/processed/m10/state/dedupe_state.json` for a clean slate (e.g., for demos).

### Observability drill-down

* **Metrics:** `data/observability/metrics.prom` (Prometheus text format).
* **Logs:** `data/observability/logs/` (one JSON line per event; Loki-friendly).

### Evidence pack

* **Evaluation reports:** `data/eval/reports/*.json`
* **Cards:** `docs/cards/model_card_alerts.md`, `docs/cards/data_card_eval_sets.md`
* **CI report:** `data/observability/ci/ci_report.json`
* **Repro bundle:** `dist/verdantis_bundle.zip`

---

## Security & compliance notes

* Demo credentials are for local use only. Replace or externalize the user store for any real deployment.
* Public outputs default to severity filtering and asset anonymization. Review `configs/m11_public.json` before exposure.
* Secrets are read from environment variables; avoid committing them. Consider a proper secret manager for non-dev environments.

---

## Roadmap suggestions (post-MVP)

* Plug real channels (email/SMS/webhook queues) behind the stubs with backoff and delivery receipts.
* Promote the `/metrics` server to a sidecar or node exporter textfile collector; wire logs to Loki or OpenSearch.
* Expand evaluation with real datasets and larger sample sizes; add drift and data-quality checks.
* Introduce a thin web front-end for `/web/portals` that reads the generated JSONs and renders cards/tables/maps.
* Optional: containerize and wire a minimal CD job (Render/Cloud Run) using the `dist/` bundle.

---

## Support & contribution

This codebase is intentionally dependency-light and file-oriented to make reviews and audits straightforward. If you extend it:

* Keep JSON interfaces stable and documented.
* Preserve dedupe state semantics and the public masking policy.
* Add metrics when you add logic, and update Model/Data cards accordingly.

If you need a hand turning this into a production service with managed infra, SSO, and policy packs, that’s a natural next step—but this repository already gives you a clean, defendable core.
