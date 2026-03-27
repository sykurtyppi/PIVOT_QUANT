# Air/Mini Research Sync

## Topology
- MacBook Air: production truth
  - live collection
  - live scoring
  - governance
  - active manifests
- Mac mini: research compute
  - imported Air snapshot
  - expanded historical DB
  - Monte Carlo / Markov / replay work
  - experimental model and ranking analysis

The Mini should not become a second production source of truth.

## Workflow
1. Export a research bundle on the Air.
2. Copy the bundle directory to the Mini.
3. Import the bundle on the Mini.
4. Run research jobs against the imported working DB.
5. Keep the imported baseline DB immutable.

## Air Export
```bash
cd ~/PIVOT_QUANT
python3 scripts/export_research_sync_bundle.py
```

Optional:
```bash
python3 scripts/export_research_sync_bundle.py --bundle-name air_sync_20260327
python3 scripts/export_research_sync_bundle.py --skip-models-archive
```

Default output root:
`backups/research_sync/`

## Mini Import
After copying the bundle directory from the Air:

```bash
cd ~/PIVOT_QUANT
python3 scripts/import_research_sync_bundle.py \
  --bundle-dir /path/to/copied/bundle
```

This creates:
- `data/air_research_sync/<bundle>/baseline/pivot_events.sqlite`
- `data/air_research_sync/<bundle>/working/pivot_events.sqlite`
- `data/air_research_sync/<bundle>/models/`
- `data/air_research_sync/<bundle>/air_research.env`

Convenience pointers:
- `data/air_research_sync/latest.json`
- `data/air_research_sync/latest.env`

## Research Session
Use the imported working DB, not the baseline DB:

```bash
cd ~/PIVOT_QUANT
source data/air_research_sync/latest.env
```

After that, research scripts will read:
- `PIVOT_DB` from the imported working DB
- `RF_MODEL_DIR` from the imported Air model set
- `RF_MANIFEST_PATH` from the imported manifest

## Recommended Mini Pattern
1. Import the latest Air bundle.
2. Leave `baseline/pivot_events.sqlite` untouched.
3. Run backfills and label builds against `working/pivot_events.sqlite`.
4. Export parquet / DuckDB / simulation artifacts from the working DB.
5. Keep production decisions on the Air only.

## Notes
- The export includes a source parity snapshot and manifest metadata.
- The import report checks SQLite integrity and records whether local git matches the source bundle.
- If you want a fresh import into the same folder, rerun with `--overwrite`.
