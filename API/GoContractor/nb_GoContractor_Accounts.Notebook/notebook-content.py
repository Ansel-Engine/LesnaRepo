# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cde07ee3-3b2d-4924-a2da-d9ac5970b70e",
# META       "default_lakehouse_name": "LS_Lakehouse",
# META       "default_lakehouse_workspace_id": "8bc6ea08-fd30-4ca7-be7f-dc76aee17419",
# META       "known_lakehouses": [
# META         {
# META           "id": "cde07ee3-3b2d-4924-a2da-d9ac5970b70e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Create the table LS_Lakehouse.stg_GoContractor_Workers (one column: raw_record STRING)

TARGET_DB  = "LS_Lakehouse"
TARGET_TBL = "stg_gocontractor_accounts"

# Ensure the LS_Lakehouse is attached to the notebook
dbs = [db.name for db in spark.catalog.listDatabases()]
if TARGET_DB not in dbs:
    raise RuntimeError(
        f"Lakehouse '{TARGET_DB}' is not attached. "
        "Attach it to this notebook (as Primary or Secondary) and re-run."
    )

spark.sql(f"USE {TARGET_DB}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TBL} (
  raw_record STRING,
  LoadedDate TIMESTAMP
)
USING DELTA
""")

print(f"Table ready: {TARGET_DB}.{TARGET_TBL}")
spark.sql(f"DESCRIBE EXTENDED {TARGET_DB}.{TARGET_TBL}").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Fabric notebook cell (Python)
# Reads paginated Accounts and appends to LS_LakeHouse.stg_GoContractor_Accounts
# Columns: raw_record (<= 4000 chars), LoadedDate (UTC timestamp)

import json
import requests
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timezone

# ---------------- Config ----------------
BASE_URL    = "https://integrations.gocontractor.com"
API_URL     = f"{BASE_URL}/api/v1/integration/Account"
API_KEY     = "Bearer gc_vl3cOW16XE2t1mOarHNkfwIVpV1p2DMkiPPGK7wr12uw"  # <-- consider moving this to a secret store (e.g., Key Vault) in production
TIMEOUT     = 60
START_PAGE  = 0                      # zero-based
PAGE_SIZE   = 200                    # try larger if supported
TARGET_DB   = "LS_Lakehouse"
TARGET_TBL  = "stg_gocontractor_accounts"
WRITE_MODE  = "append"
VERBOSE     = True

# Optional test limiter (e.g., 5 pages); set to None for full run
MAX_PAGES: int | None = None

# Match NVARCHAR(4000)
MAX_CHARS = 4000

# ---------------- HTTP setup ----------------
session = requests.Session()
headers = {
    "x-api-key": API_KEY,
    "Authorization": f"{API_KEY}",
    "Accept": "application/json",
    "User-Agent": "fabric-notebook-gocontractor/accounts/2.0",
}

def build_start_url() -> str:
    q = {"PageNumber": str(START_PAGE)}
    q["PageSize"] = str(PAGE_SIZE)   # try both casings; API will ignore unsupported
    q["pageSize"] = str(PAGE_SIZE)
    return f"{API_URL}?{urlencode(q)}"

def next_url_from_pagination(current_url: str, pagination: dict) -> str | None:
    nxt_rel = pagination.get("nextPage")
    if nxt_rel:
        return urljoin(BASE_URL, nxt_rel)
    cp = pagination.get("currentPage")
    np = pagination.get("numPages")
    if isinstance(cp, int) and isinstance(np, int) and cp < (np - 1):
        nxt = cp + 1
        parsed = urlparse(current_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["PageNumber"] = str(nxt)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(query)}"
    return None

def compact_and_bound(obj) -> str:
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return s[:MAX_CHARS] if len(s) > MAX_CHARS else s

# ---------------- Lakehouse checks ----------------
dbs = [db.name for db in spark.catalog.listDatabases()]
if TARGET_DB not in dbs:
    raise RuntimeError(
        f"Lakehouse '{TARGET_DB}' is not attached. Attach it and re-run. "
        f"Attached: {dbs}"
    )

tbl_names = [t.name if hasattr(t, "name") else t.tableName for t in spark.catalog.listTables(TARGET_DB)]
if TARGET_TBL not in tbl_names:
    raise RuntimeError(
        f"Table '{TARGET_DB}.{TARGET_TBL}' not found. "
        f"Create it in the SQL analytics endpoint with:\n"
        f"CREATE TABLE {TARGET_TBL} (raw_record NVARCHAR(4000) NOT NULL, LoadedDate DATETIME2(3) NOT NULL);"
    )

spark.sql(f"TRUNCATE TABLE {TARGET_DB}.{TARGET_TBL}")  ######LS remove this

spark.sql(f"USE {TARGET_DB}")
full_name = f"{TARGET_DB}.{TARGET_TBL}"

# ---------------- Fetch & write per page ----------------
current_url = build_start_url()
seen_urls = set()
pages_seen = 0
total_rows = 0
truncated_rows = 0

#MAX_PAGES = 5  ##LS

print("Starting fetch…", flush=True)

while True:
    if current_url in seen_urls:
        print(f"Stopping: detected repeated nextPage URL (cycle): {current_url}", flush=True)
        break
    seen_urls.add(current_url)

    resp = session.get(current_url, headers=headers, timeout=TIMEOUT)
    if resp.status_code == 401:
        raise RuntimeError("Unauthorized (401). Check API key / auth headers.")
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}

    items = payload.get("data", []) or []
    pagination = payload.get("pagination") or {}

    # Prepare this page's rows (compact JSON, bounded to 4000 chars)
    page_json = []
    for acc in items:
        s = json.dumps(acc, ensure_ascii=False, separators=(",", ":"))
        if len(s) > MAX_CHARS:
            truncated_rows += 1
            s = s[:MAX_CHARS]
        page_json.append((s,))  # tuple for single col

    # Convert to Spark DF: raw_record STRING, then add LoadedDate
    if page_json:
        schema = StructType([StructField("raw_record", StringType(), False)])
        df_page = spark.createDataFrame(page_json, schema).withColumn("LoadedDate", F.current_timestamp())
        # Append to existing table
        df_page.write.mode(WRITE_MODE).saveAsTable(full_name)
        total_rows += df_page.count()

    pages_seen += 1
    if VERBOSE:
        cp = pagination.get("currentPage")
        np = pagination.get("numPages")
        print(f"Page {cp if cp is not None else pages_seen-1}"
              + (f"/{(np-1)}" if isinstance(np, int) else "")
              + f": items={len(items)}, total_rows={total_rows}", flush=True)

    if MAX_PAGES is not None and pages_seen >= MAX_PAGES:
        print(f"Reached test limit of {MAX_PAGES} pages; stopping.", flush=True)
        break

    nxt = next_url_from_pagination(current_url, pagination)
    if not nxt:
        print("No next page. Finished.", flush=True)
        break
    current_url = nxt

    ##LS - Added this section
    #pages_seen += 1
    #print(f"pages_seen:{pages_seen}")

    # --- TEST LIMIT: stop after N pages ------------------------------------------------------------------------------------
    #if pages_seen >= MAX_PAGES:
    #    print(f"Reached test limit of {MAX_PAGES} pages; stopping.", flush=True)
    #    break
    ##

print(f"Done. Pages fetched: {pages_seen}, rows written: {total_rows}", flush=True)
if truncated_rows:
    print(f"NOTE: {truncated_rows} row(s) exceeded {MAX_CHARS} chars and were truncated.", flush=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
