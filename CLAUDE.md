# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project context

dbt project used for the Indicium "Formação de Analytics Engineering" course, working with the Banvic (fictional bank) dataset. Configured for Databricks. The repo is normally forked by students and developed against their own Databricks workspace.

The dbt project name is `my_new_project` (boilerplate — referenced in `dbt_project.yml` under both `models:` and `seeds:`); rename in both places if changing it.

The dbt profile is `default` (see `dbt_project.yml`). `profiles.yml` is not committed — it lives in the user's `~/.dbt/` and must define a `default` profile pointing at Databricks before any dbt command will run.

## Common commands

```bash
dbt seed                    # load all CSVs in seeds/ into the warehouse
dbt seed -s <csv_name>      # load a single seed (filename without .csv)
dbt run                     # build all models
dbt run -s <model_name>     # build a single model
dbt test                    # run tests defined in models/schema.yml
dbt build                   # seed + run + test in dependency order
dbt clean                   # remove target/ and dbt_packages/
```

If the dbt CLI hangs after a long `dbt seed` (known issue called out in the README), restart the IDE / terminal rather than killing the process — dbt may have finished but the shell stays busy.

## Architecture

### Seeds are the data source
The Banvic source tables (`agencias`, `clientes`, `colaboradores`, `colaborador_agencia`, `contas`, `propostas_credito`, `transacoes`, `localidades`) live as CSVs in `seeds/banvic/` and are loaded into the warehouse with `dbt seed`. There is no external source database — `dbt seed` *is* the ingestion step.

`seeds/banvic/_seed_schema.yml` pins every column type to `string` so nothing gets coerced on load (dates, codes, decimals all land as text). Downstream models are expected to cast.

In `dbt_project.yml`, the `banvic` seed group is configured with `+enabled: true` and `+schema: erp_banvic`, so `dbt seed` materializes them into the `erp_banvic` schema rather than the target's default schema. Flip `+enabled` to `false` (or override on the CLI) if you need to skip them in a build.

### Custom schema routing — `macros/generate_schema_name.sql`
This macro overrides dbt's default schema-naming behavior and is load-bearing. The rules:

- **`prod` / `dev` targets**: a model's `+schema:` value is used verbatim (no prefix). This is the "real" environment behavior.
- **Any other target** (e.g. a developer's personal target): `+schema:` is prefixed with the target schema → `<target.schema>_<custom>`. This is what isolates each developer's builds.
- **Special case**: `erp_banvic` is *always* used verbatim, even in non-prod/dev targets, so the seeded source tables land in one shared schema everyone can read from.
- **No custom schema** specified → falls back to the target's default schema.

Keep this in mind when adding new model groups: if you give them a `+schema:` config, devs will get prefixed schemas while prod/dev get the bare name. If you ever introduce another shared schema like `erp_banvic`, add it to the `elif` branch.

### Model layers
Three-tier layout under `models/`:

- `staging/erp_banvic/` — one `stg_erp__<entity>.sql` per source table. Each reads from `{{ source('erp', '<entity>') }}`, casts every column to its real type (seeds load as `string`), and renames keys to `pk_<entity>` / `fk_<related>`. Sources are declared in `models/staging/erp_banvic/_erp_banvic.yml` under the `erp` source pointing at the `erp_banvic` schema.
- `intermediate/` — `int_*.sql` joins/enriches staging models (e.g. `int_dimensao_clientes` joins clientes ↔ localidades). Reference upstream via `{{ ref(...) }}`, never `source()`.
- `marts/` — final `dim_*` / `fact_*` models that downstream consumers query. Should `ref()` intermediates (or staging directly when no enrichment is needed).

### Conventions visible in existing models
- CTE-per-step style: `fonte_<x>` → `renomeado` / `<x>_enriquecido` → final `select *`. Keep the leading-comma SQL style used throughout.
- Key prefixes: `pk_` for the entity's own key, `fk_` for foreign keys. Suffix descriptive columns with the entity name (`nome_cliente`, `cep_cliente`) when they'd otherwise collide after joins.
- CPF/CNPJ and CEP are stripped of punctuation with `regexp_replace(..., '[^a-zA-Z0-9]', '')` in staging — do the same for any new document/postal-code field rather than re-cleaning downstream.
- Source/model docs and descriptions are written in Portuguese (course language) — match that when adding `_*.yml` entries.
- Tests and docs sit alongside the SQL: sources are documented in `models/staging/erp_banvic/_erp_banvic.yml`; mart-level docs/tests go in a sibling `<model>.yml` (see `models/marts/dim_clientes.yml`). Primary keys get `unique` + `not_null` data tests by convention.
