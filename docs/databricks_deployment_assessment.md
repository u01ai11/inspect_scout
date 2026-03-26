# Databricks Deployment Assessment for Scout View

## Is this possible?

Yes—with an integration layer.

Inspect Scout already provides:
- A FastAPI server that serves the React frontend plus API endpoints.
- Runtime support for proxy base paths via `root_path` (useful behind Databricks app/proxy URLs).
- Transcript/scans storage from filesystem-like URIs (local and S3 are explicitly documented).

What it does **not** currently provide out of the box is a native Databricks table source (e.g., Unity Catalog Delta tables) for transcript reads/writes. Today, transcript persistence is built around Scout's parquet transcript DB abstraction.

## What this implies for Databricks

To use Databricks tables as your source of truth, there are two practical architectures:

1. **Materialization approach (fastest to implement)**
   - Read from Databricks tables.
   - Transform rows into Scout transcript schema.
   - Write/update a Scout transcript DB in parquet (e.g., DBFS volume, cloud object store).
   - Point `scout view -T` and scans at that parquet location.

2. **Native source approach (more engineering, better long-term DX)**
   - Add a new Scout source (e.g., `databricks()`) under `inspect_scout.sources`.
   - Support importing from SQL Warehouse/Unity Catalog directly through the existing import pipeline.
   - Optionally support incremental sync (watermark by timestamp/version).

## Proposed implementation steps

1. Confirm hosting model on Databricks
   - Databricks Apps vs custom reverse-proxied service.
   - Set host/port and `UVICORN_ROOT_PATH` correctly.

2. Confirm data model mapping
   - Map your table schema to Scout-required fields (`transcript_id`, `messages`) and optional fields (`events`, `metadata`, etc.).

3. Build ingest/sync job
   - Batch import from Databricks table(s) to transcript parquet DB.
   - Add incremental mode (`from_time`/`to_time` or change-data-feed pattern).

4. Configure Scout View runtime
   - Launch with `scout view -T <transcripts-db> --scans <scans-dir>`.
   - Configure authorization token header if exposure is shared.

5. Validate end-to-end
   - Verify transcript browsing, filtering, scanner execution, and persisted scan outputs.

## Questions we need from you

### Table design
- What are the exact Unity Catalog table names (catalog.schema.table) for:
  - transcript-level rows,
  - message-level rows,
  - event/tool-call rows (if separate),
  - existing scanner results (if any)?
- Are messages/events stored as JSON blobs, arrays/structs, or normalized child tables?
- What is the primary key (or stable ID) for a transcript?
- Which columns contain timestamp(s) for incremental sync?
- Approximate volume (rows/day, total transcripts, average messages/transcript)?

### Databricks runtime and access
- How should the app connect: SQL Warehouse, cluster compute, or serverless?
- Auth mechanism available to app runtime: PAT, OAuth service principal, or workspace identity?
- Network/path constraints for storage targets (DBFS volume, Unity external location, S3/ADLS/GCS)?
- Can we create/write parquet artifacts in object storage from the runtime?

### App serving and security
- Which serving surface do you want: Databricks Apps or another proxy endpoint?
- Expected base URL/path (needed for `UVICORN_ROOT_PATH` if proxied).
- Do you require auth at Scout API layer (Authorization header), workspace-only isolation, or both?

### Scanner execution model
- Should scans run in-process with the web app, or be submitted as background jobs?
- Which model providers will scanners use (OpenAI/Azure/OpenRouter/etc.)?
- Where should scan outputs be persisted and retained?

### Operational constraints
- Latency expectations for transcript browsing and scan runs.
- SLA/availability requirements.
- Cost or quota constraints for compute/storage/model API usage.
