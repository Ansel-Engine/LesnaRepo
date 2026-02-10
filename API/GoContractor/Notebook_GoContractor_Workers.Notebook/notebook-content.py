# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Fabric notebook cell (Python)
# Robust, talkative pagination for GoContractor Worker endpoint
import requests
import pandas as pd
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode

BASE_URL = "https://integrations.gocontractor.com"
API_URL = f"{BASE_URL}/api/v1/integration/Worker"
API_KEY = "Bearer gc_vl3cOW16XE2t1mOarHNkfwIVpV1p2DMkiPPGK7wr12uw"  # <-- consider moving this to a secret store (e.g., Key Vault) in production
TIMEOUT = 60
START_PAGE = 0
# If the API supports it, bump this to reduce calls; if not supported, the server will ignore it.
PAGE_SIZE_PARAM = 200

def extract_home_postcode(user_data: Optional[List[Dict[str, Any]]]) -> Optional[str]:
    if not user_data:
        return None
    for item in user_data:
        name = (item or {}).get("name", "")
        if isinstance(name, str) and name.strip().lower() == "home postcode":
            data = (item or {}).get("data")
            if isinstance(data, dict):
                v = data.get("value")
                if isinstance(v, str) and v.strip():
                    return v.strip()
                opts = data.get("selectedOptions")
                if isinstance(opts, list) and opts:
                    return str(opts[0]).strip() or None
            if isinstance(data, str) and data.strip():
                return data.strip()
    return None

def extract_worker_group_guids(worker_groups: Optional[List[Dict[str, Any]]]) -> List[Optional[str]]:
    if not worker_groups:
        return [None]
    guids = []
    for wg in worker_groups:
        guid = (wg or {}).get("guid")
        guids.append(guid if isinstance(guid, str) else None)
    return guids or [None]

session = requests.Session()
headers = {
    "x-api-key": API_KEY,
    "Authorization": f"{API_KEY}",
    "Accept": "application/json",
    "User-Agent": "fabric-notebook-gocontractor/1.3",
}

rows: List[Dict[str, Any]] = []

# Start at PageNumber=0 and include an optional PageSize
start_query = {"PageNumber": str(START_PAGE)}
if PAGE_SIZE_PARAM:
    # Many APIs use PageSize, pageSize or limit; this one shows 'pageSize' in pagination
    # but nextPage uses 'PageNumber'. We'll still try 'PageSize' and also 'pageSize'.
    start_query["PageSize"] = str(PAGE_SIZE_PARAM)
    start_query["pageSize"] = str(PAGE_SIZE_PARAM)
current_url = f"{API_URL}?{urlencode(start_query)}"

pages_seen = 0
expected_pages = None  # from pagination.numPages
seen_urls = set()      # to prevent cycles
HARD_PAGE_CAP = 20000  # safety cap to avoid runaway loops

MAX_PAGES = 5  ##LS


print("Starting fetch…", flush=True)

while True:

    if current_url in seen_urls:
        print(f"Stopping: detected repeated nextPage URL (cycle): {current_url}", flush=True)
        break
    seen_urls.add(current_url)

    try:
        resp = session.get(current_url, headers=headers, timeout=TIMEOUT)
        if resp.status_code == 401:
            raise RuntimeError("Unauthorized (401). Check API key / auth headers.")
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}
    except Exception as e:
        print(f"Request failed on URL: {current_url}\n{e}", flush=True)
        break

    items = payload.get("data", []) or []
    pagination = payload.get("pagination") or {}

    # Capture total pages (zero-based last index is numPages-1)
    if expected_pages is None and isinstance(pagination.get("numPages"), int):
        expected_pages = pagination["numPages"]

    # Extract requested fields
    added = 0
    for worker in items:
        home_postcode = extract_home_postcode(worker.get("userData"))
        for group_guid in extract_worker_group_guids(worker.get("workerGroups")):
            rows.append({
                "worker_group_guid": group_guid,
                "home_postcode": home_postcode,
            })
            added += 1

    pages_seen += 1
    total_rows = len(rows)
    cp = pagination.get("currentPage")
    npages = pagination.get("numPages")
    print(
        f"Page {cp if cp is not None else pages_seen-1}"
        + (f"/{npages-1}" if isinstance(npages, int) else "")
        + f": items={len(items)}, rows_added={added}, total_rows={total_rows}",
        flush=True
    )

    # Determine next page

    next_page_rel = pagination.get("nextPage")


    if next_page_rel:
        # nextPage is a relative path; make it absolute
        current_url = urljoin(BASE_URL, next_page_rel)
    else:
        # Fallback using zero-based currentPage / numPages
        if isinstance(cp, int) and isinstance(npages, int) and cp < (npages - 1):
            next_number = cp + 1
            parsed = urlparse(current_url)
            query = dict(parse_qsl(parsed.query, keep_blank_values=True))
            query["PageNumber"] = str(next_number)
            current_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(query)}"
        else:
            print("No next page. Finished.", flush=True)
            break

    # Safety: bail out if page count looks unreasonable
    if pages_seen >= HARD_PAGE_CAP:
        print(f"Stopping: hit HARD_PAGE_CAP={HARD_PAGE_CAP}.", flush=True)
        break

    ##LS - Added this section
    pages_seen += 1
    print(f"pages_seen:{pages_seen}")

    # --- TEST LIMIT: stop after N pages ---
    if pages_seen >= MAX_PAGES:
        print(f"Reached test limit of {MAX_PAGES} pages; stopping.", flush=True)
        break
    ##

# Build the DataFrame and show first rows right away
df = pd.DataFrame(rows)
print(f"\nDone. Pages fetched: {pages_seen}" + (f" / {expected_pages}" if expected_pages is not None else ""))
print(f"Total rows extracted: {len(df):,}")
display(df.head(20))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Fabric notebook cell (Python)
# Robust, talkative pagination for GoContractor Worker endpoint
import requests
import pandas as pd
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode

BASE_URL = "https://integrations.gocontractor.com"
API_URL = f"{BASE_URL}/api/v1/integration/Worker"
API_KEY = "Bearer gc_vl3cOW16XE2t1mOarHNkfwIVpV1p2DMkiPPGK7wr12uw"  # TODO: move to a secret store in production
TIMEOUT = 60
START_PAGE = 0
PAGE_SIZE_PARAM = 200

# --- Helpers ---
def extract_course_entries(user_data):
    """Return a list of flattened dicts for userData items with type == 'Course'."""
    out = []
    if not isinstance(user_data, list):
        return out
    for ud in user_data:
        if (ud or {}).get("type") == "Course":
            d = (ud or {}).get("data") or {}
            out.append({
                "course_name": (ud or {}).get("name"),
                "requirement_guid": (ud or {}).get("requirementGuid"),
                "expiry_utc": (ud or {}).get("expiryDate"),
                "course_guid": d.get("courseGuid"),
                "test_attempts": d.get("testAttempts"),
                "test_score": d.get("testScore"),
                "test_date_utc": d.get("testDate"),
                "test_time": d.get("testTime"),
                "pass_score": d.get("passScore"),
                "is_completed": d.get("isCompleted"),
                "is_passed": d.get("isPassed"),
                "certificate_url": d.get("generateCertificateUrl"),
            })
    return out

# --- HTTP session ---
session = requests.Session()
headers = {
    "x-api-key": API_KEY,
    "Authorization": f"{API_KEY}",
    "Accept": "application/json",
    "User-Agent": "fabric-notebook-gocontractor/1.3",
}

# --- Accumulators ---
rows: List[Dict[str, Any]] = []          # minimal per-worker rows (no postcode / no worker group)
course_rows: List[Dict[str, Any]] = []   # per worker x course

# Start at PageNumber=0 and include an optional PageSize
start_query = {"PageNumber": str(START_PAGE)}
if PAGE_SIZE_PARAM:
    start_query["PageSize"] = str(PAGE_SIZE_PARAM)
    start_query["pageSize"] = str(PAGE_SIZE_PARAM)
current_url = f"{API_URL}?{urlencode(start_query)}"

pages_seen = 0
expected_pages = None  # from pagination.numPages
seen_urls = set()      # to prevent cycles
HARD_PAGE_CAP = 20000  # safety cap to avoid runaway loops

MAX_PAGES = 5  # test limiter

print("Starting fetch…", flush=True)

while True:

    if current_url in seen_urls:
        print(f"Stopping: detected repeated nextPage URL (cycle): {current_url}", flush=True)
        break
    seen_urls.add(current_url)

    try:
        resp = session.get(current_url, headers=headers, timeout=TIMEOUT)
        if resp.status_code == 401:
            raise RuntimeError("Unauthorized (401). Check API key / auth headers.")
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}
    except Exception as e:
        print(f"Request failed on URL: {current_url}\n{e}", flush=True)
        break

    items = payload.get("data", []) or []
    pagination = payload.get("pagination") or {}

    # Capture total pages (zero-based last index is numPages-1)
    if expected_pages is None and isinstance(pagination.get("numPages"), int):
        expected_pages = pagination["numPages"]

    # Extract requested fields (without home_postcode or worker_group_guid)
    added = 0
    for worker in items:
        user = (worker or {}).get("user") or {}

        # Minimal per-worker row
        rows.append({
            "worker_guid": (worker or {}).get("guid"),
            "user_guid": user.get("guid"),
            "first_name": user.get("firstName"),
            "last_name": user.get("lastName"),
        })
        added += 1

        # Per-course rows from userData where type == 'Course'
        for c in extract_course_entries(worker.get("userData")):
            course_rows.append({
                "worker_guid": (worker or {}).get("guid"),
                "user_guid": user.get("guid"),
                "first_name": user.get("firstName"),
                "last_name": user.get("lastName"),
                **c,  # flatten course fields
            })

    pages_seen += 1
    total_rows = len(rows)
    cp = pagination.get("currentPage")
    npages = pagination.get("numPages")
    print(
        f"Page {cp if cp is not None else pages_seen-1}"
        + (f"/{npages-1}" if isinstance(npages, int) else "")
        + f": items={len(items)}, rows_added={added}, total_rows={total_rows}",
        flush=True
    )

    # Determine next page
    next_page_rel = pagination.get("nextPage")

    if next_page_rel:
        # nextPage is a relative path; make it absolute
        current_url = urljoin(BASE_URL, next_page_rel)
    else:
        # Fallback using zero-based currentPage / numPages
        if isinstance(cp, int) and isinstance(npages, int) and cp < (npages - 1):
            next_number = cp + 1
            parsed = urlparse(current_url)
            query = dict(parse_qsl(parsed.query, keep_blank_values=True))
            query["PageNumber"] = str(next_number)
            current_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(query)}"
        else:
            print("No next page. Finished.", flush=True)
            break

    # Safety: bail out if page count looks unreasonable
    if pages_seen >= HARD_PAGE_CAP:
        print(f"Stopping: hit HARD_PAGE_CAP={HARD_PAGE_CAP}.", flush=True)
        break

    # --- TEST LIMIT: stop after N pages ---
    pages_seen += 1
    print(f"pages_seen:{pages_seen}")
    if pages_seen >= MAX_PAGES:
        print(f"Reached test limit of {MAX_PAGES} pages; stopping.", flush=True)
        break

# Build the per-worker DataFrame and show first rows
df = pd.DataFrame(rows)
print(f"\nDone. Pages fetched: {pages_seen}" + (f" / {expected_pages}" if expected_pages is not None else ""))
print(f"Total workers extracted: {len(df):,}")
display(df.head(20))

# Build the Courses dataframe and show first rows
df_courses = pd.DataFrame(course_rows)

# Optional: parse timestamps for convenience
for col in ("test_date_utc", "expiry_utc"):
    if col in df_courses.columns:
        df_courses[col] = pd.to_datetime(df_courses[col], errors="coerce", utc=True)

print(f"Total course rows extracted: {len(df_courses):,}")
display(df_courses.head(20))

# Optional persistence examples:
# df.to_parquet("workers.parquet")
# df_courses.to_parquet("courses.parquet")
# spark.createDataFrame(df_courses).write.mode("overwrite").saveAsTable("catalog.schema.WorkerCourses")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
