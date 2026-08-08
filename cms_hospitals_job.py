"""
CMS Provider Data - download all "Hospitals" datasets, incrementally, in parallel.
=================================================================================

A NOTE ON MY BACKGROUND
-----------------------
My background is SQL. I am new to Python. So I designed this
job the way I would design an ETL job in a warehouse, and deliberately kept the
logic in SQL wherever SQL could do the work. Python is used only for the three
things SQL genuinely cannot do: make HTTP calls, write files, and run work in
parallel. The SQL engine here is DuckDB - an embedded, file-based analytical
database (think "SQLite for analytics"), so no database server is required.

I first prototyped the catalog load in SQL Server using OPENROWSET + OPENJSON to
understand the JSON structure, and validated that the Python version returns the
same 73 Hospitals datasets that my T-SQL query returned.

HOW THE JOB MAPS TO THE REQUIREMENTS
------------------------------------
1. "Download all data sets related to the theme Hospitals"
      -> Section 3 loads the metastore catalog into a staging table and filters
         with:  WHERE list_contains(theme, 'Hospitals')
         Note the catalog's "theme" is an ARRAY, so this checks the whole array
         rather than just the first element.

2. "Convert all column names to snake_case"
      -> to_snake_case() in Section 1, applied to the header row of every CSV in
         Section 2. Any run of non-alphanumeric characters collapses to a single
         underscore, so "Patients' rating of the facility linear mean score"
         becomes patients_rating_of_the_facility_linear_mean_score.

3. "Downloaded and processed in parallel"
      -> Section 5 runs 8 downloads at a time via a thread pool (the Python
         equivalent of MAXDOP 8).

4. "Run every day, but only download files modified since the previous run
    (need to track runs/metadata)"
      -> Section 4 is a classic incremental-load query: LEFT JOIN the staging
         catalog against a run_history table and keep rows that are new or whose
         "modified" date advanced. Section 6 upserts run_history (MERGE) so the
         next run only picks up changes. run_history lives in run_metadata.duckdb,
         a single file next to this script.

5. "Must run on a regular Windows or Linux computer"
      -> Pure Python plus one package (duckdb). No server, no cloud services.

SETUP
-----
    pip install duckdb          (or: pip install -r requirements.txt)
    python cms_hospitals_job.py

Run it twice: the first run downloads all 73 files, the second should report
"to download: 0 | skipped: 73", which demonstrates the incremental logic.

OUTPUT
------
    output/                 the 73 processed CSVs (snake_case headers)
    run_metadata.duckdb     the metadata database (run_history table)
    catalog.json            the raw catalog pulled from the API this run

To force a full re-download, delete run_metadata.duckdb (that clears the
"watermark"), or run:  DELETE FROM run_history;
"""

# =============================================================================
# SECTION 0 - IMPORTS AND CONFIG
# =============================================================================
# In SQL you might reference a linked server or enable a feature before using
# it. Python is similar: the core language is small, so you "import" the
# libraries you need. Each of these does exactly one job for us:
#
#   csv                - parses a CSV line correctly (handles quoted commas)
#   re                 - regular expressions, used for the snake_case rename
#   urllib.request     - makes HTTP calls (SQL Server can't; this is why the
#                        job needs Python at all)
#   concurrent.futures - runs work in parallel (the thread pool / MAXDOP part)
#   pathlib.Path       - builds file paths that work on Windows AND Linux
#   duckdb             - the SQL engine. NOT part of Python by default:
#                        run "pip install duckdb" first.
import csv
import re
import urllib.request
import concurrent.futures
from pathlib import Path
import duckdb

# Constants. Python has no DECLARE - assigning a value creates the variable.
# ALL_CAPS is just a naming convention meaning "this is a config value".
CATALOG_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
OUTPUT_DIR = Path(r"C:\Users\siris\Downloads\assessment\output")  # TODO: hardcoded to this machine — change to a relative path before running elsewhere
OVERWRITE_ALL = False  # ****NOTE:IMPORTANT****: only set this to True when you intentionally want to re-download everything

# Create the output folder if it isn't there. exist_ok=True means
# "don't error if it already exists" - like IF NOT EXISTS ... CREATE.
OUTPUT_DIR.mkdir(exist_ok=True)


# =============================================================================
# SECTION 1 - HELPER: snake_case a column name  [REQUIREMENT 2]
# =============================================================================
# "def" defines a function. Think scalar user-defined function: it takes a
# value in and RETURNs a value out. Everything indented below the def line is
# the body of the function (Python uses indentation where T-SQL uses BEGIN/END).
#
# The T-SQL equivalent on SQL Server 2025 would be:
#     LOWER(TRIM('_' FROM REGEXP_REPLACE(@name, '[^0-9a-zA-Z]+', '_')))
#
# Reading the pattern [^0-9a-zA-Z]+ :
#     [ ]  = a set of characters
#     ^    = NOT (so: not a digit, not a letter)
#     +    = one or more in a row
# So every run of spaces, apostrophes, slashes, %, etc. becomes ONE underscore.
# .strip("_") trims leading/trailing underscores; .lower() is LOWER().
def to_snake_case(name):
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


# =============================================================================
# SECTION 2 - HELPER: download one CSV and fix its header  [REQUIREMENTS 2 & 3]
# =============================================================================
# This is the stored procedure that Section 5 will run 8 copies of at once.
# It takes ONE row from the delta query, downloads that dataset's CSV, rewrites
# the header row, saves the file, and RETURNs a row shaped for run_history.
#
# Note it returns a status instead of raising an error: one bad URL should not
# kill the whole batch, the same way you'd wrap a step in TRY/CATCH and log the
# failure so the rest of the job continues.
def download_one(row):
    # "row" arrives as a tuple - one record with 4 columns, in the order the
    # SELECT in Section 4 listed them. This line unpacks those 4 columns into
    # 4 named variables at once (there's no SQL equivalent; it's just shorthand).
    dataset_id, title, modified, url = row

    # Build the output path. The / operator on a Path joins folders/filenames
    # and picks the right slash for the OS. The f"..." is an f-string: anything
    # inside {curly braces} is substituted in, like building a dynamic SQL
    # string. The dataset_id is appended so two datasets with similar titles
    # can never overwrite each other (it's the primary key, after all).
    out_file = OUTPUT_DIR / f"{to_snake_case(title)}__{dataset_id}.csv"

    # try/except is TRY/CATCH. Everything under "try" is attempted; if anything
    # fails, control jumps to the "except" block at the bottom.
    try:
        # Make the HTTP request. The User-Agent header just identifies the job
        # politely; some servers reject requests that don't send one.
        req = urllib.request.Request(url, headers={"User-Agent": "cms-job/1.0"})

        # "with" opens the connection and guarantees it's closed afterwards -
        # the same discipline as closing a cursor or connection in SQL.
        with urllib.request.urlopen(req, timeout=120) as resp:
            # Read the whole file and turn bytes into text.
            # "utf-8-sig" strips the BOM (a few invisible marker bytes some CMS
            # files start with). Without this, the first column name would come
            # out looking like  i_facility_id  instead of  facility_id.
            content = resp.read().decode("utf-8-sig")

        # Split the file into "first line" and "everything else".
        # .partition("\n") returns 3 pieces: before the newline, the newline
        # itself, and after it. The middle piece is discarded by naming it "_",
        # which is the Python convention for "a value I don't need".
        # Only the header is rewritten; the data rows are copied through
        # untouched, so no data is altered by this job.
        header, _, body = content.partition("\n")

        # Parse the header line into a list of column names.
        # This is done with the csv module rather than a plain split on commas
        # because a column NAME can itself contain a quoted comma, e.g.
        #     "Measure Name, Full"
        # A naive split would turn that one column into two.
        columns = next(csv.reader([header]))

        # Apply to_snake_case to every column, then join them back with commas.
        # This is a list comprehension, which reads almost exactly like SQL:
        #     ",".join(  to_snake_case(c)   for c in columns  )
        #                ^ SELECT             ^ FROM
        new_header = ",".join(to_snake_case(c) for c in columns)

        # Write the fixed header plus the original body back out to disk.
        out_file.write_text(new_header + "\n" + body, encoding="utf-8")

        print(f"  [SUCCESS] {out_file.name}")

        # RETURN one row for run_history: 6 values in the same order as the
        # table's columns, ready for the INSERT in Section 6.
        return (dataset_id, title, modified, url, str(out_file), "success")

    except Exception as err:
        # CATCH block: log which dataset failed and why, and return a "failed"
        # row. Because Section 4's join filters on status = 'success', anything
        # recorded as failed here will automatically be retried tomorrow.
        print(f"  [FAILED] {title} -> {err}")
        return (dataset_id, title, modified, url, None, "failed")


# =============================================================================
# SECTION 3 - SETUP THE METADATA DATABASE, THEN STAGING LOAD  [REQUIREMENT 1]
# =============================================================================
# duckdb.connect() opens a database file, creating it if it doesn't exist.
# One file on disk holding tables = roughly an .mdf, except the engine runs
# inside this Python process instead of as a service.
con = duckdb.connect("run_metadata.duckdb")

# The metadata table required by requirement 4: what we've downloaded, and the
# "modified" date it had at the time. CREATE TABLE IF NOT EXISTS means this is
# safe to run every day - it only actually creates the table on the first run.
con.execute("""
    CREATE TABLE IF NOT EXISTS run_history (
        dataset_id VARCHAR PRIMARY KEY, title VARCHAR, modified VARCHAR,
        download_url VARCHAR, local_file VARCHAR, status VARCHAR,
        downloaded_at TIMESTAMP)
""")

# --- Fetch the catalog ------------------------------------------------------
# The URL returns ONE JSON file, but it's a catalog: 234 rows, each describing
# a published dataset and pointing at that dataset's CSV. So this is a table of
# contents (like querying sys.tables), not the data itself.
#
# SQL can't make HTTP calls, so Python downloads the JSON to disk here and
# DuckDB reads it from there in the next statement.
print("Fetching CMS catalog...")
req = urllib.request.Request(CATALOG_URL, headers={"User-Agent": "cms-job/1.0"})
with urllib.request.urlopen(req, timeout=120) as resp:
    Path("catalog.json").write_bytes(resp.read())

# --- Staging load: truncate-and-load the catalog into a table ---------------
# From here on it's ordinary SQL. read_json() shreds the JSON into rows and
# columns - it is DuckDB's equivalent of OPENJSON, and it infers the structure
# so no WITH (...) column list is needed.
#
# Three things worth noting in this query:
#   * distribution[1].downloadURL  - reaches into the nested structure to get
#     the file URL. DuckDB arrays are 1-based like SQL (in T-SQL this was
#     '$.distribution[0].downloadURL', which is 0-based - easy to trip on).
#   * list_contains(theme, 'Hospitals') - "theme" is an array, so this asks
#     "does the array contain Hospitals?" rather than comparing one value.
#   * CAST(modified AS VARCHAR) - read_json infers "modified" as DATE, but
#     run_history stores it as VARCHAR; DuckDB is strict about types and will
#     refuse to compare DATE to VARCHAR in the join below, so cast it here.
#     Comparing ISO dates as text is safe because YYYY-MM-DD sorts correctly.
con.execute("""
    CREATE OR REPLACE TABLE staging_catalog AS
    SELECT identifier AS dataset_id, title,
           CAST(modified AS VARCHAR) AS modified,
           distribution[1].downloadURL AS download_url
    FROM read_json('catalog.json')
    WHERE list_contains(theme, 'Hospitals')          -- 'Hospitals' IN theme[]
      AND distribution[1].downloadURL IS NOT NULL    -- skip entries with no file
""")


# =============================================================================
# SECTION 4 - DELTA DETECTION  [REQUIREMENT 4]
# =============================================================================
# The heart of the job, and it's pure SQL: a standard incremental-load query.
# Compare today's catalog against what we've already got:
#     h.dataset_id IS NULL   -> a dataset we've never downloaded
#     s.modified > h.modified -> CMS republished it since our last good run
# Anything else is skipped, which is what "only download files that have been
# modified since the previous run" asks for.
#
# The "AND h.status = 'success'" sits in the JOIN (not the WHERE) on purpose:
# a dataset whose last attempt FAILED finds no matching history row, so it
# comes back as NULL and gets retried on the next run.
delta = con.execute("""
    SELECT s.dataset_id, s.title, s.modified, s.download_url
    FROM staging_catalog s
    LEFT JOIN run_history h
           ON h.dataset_id = s.dataset_id AND h.status = 'success'
    WHERE h.dataset_id IS NULL          -- brand new dataset
       OR s.modified > h.modified       -- changed since last successful run
""").fetchall()
# .fetchall() pulls the result set out of the database and into Python as a
# list of rows (each row a tuple). Up to this point nothing has left SQL.

# .fetchone()[0] takes the first row of the result, then its first column -
# i.e. the scalar out of a COUNT(*).
total = con.execute("SELECT COUNT(*) FROM staging_catalog").fetchone()[0]

# len(delta) is the row count of the delta list - COUNT(*) on that result set.
if OVERWRITE_ALL:
    print("IMPORTANT: OVERWRITE_ALL is enabled. This will re-download all datasets.")
    delta = con.execute("SELECT dataset_id, title, modified, download_url FROM staging_catalog").fetchall()

print(f"{total} hospital datasets | to download: {len(delta)} | skipped: {total - len(delta)}\n")
if delta:
    if OVERWRITE_ALL:
        print("Overwriting all datasets because OVERWRITE_ALL=True")
    else:
        print("Changed datasets since last successful run:")
    for dataset_id, title, _, _ in delta:
        print(f"  - {title} ({dataset_id})")
    print()
else:
    print("No datasets changed since the last successful run.\n")


# =============================================================================
# SECTION 5 - PARALLEL DOWNLOAD  [REQUIREMENT 3]
# =============================================================================
# A thread pool is a set of workers that process rows at the same time.
# max_workers=8 means 8 downloads are in flight at once - the same idea as
# MAXDOP 8. Downloads spend most of their time waiting on the network, so
# running 8 of them together is dramatically faster than one at a time.
#
# pool.map(download_one, delta) reads as:
#     SELECT download_one(row) FROM delta      -- with MAXDOP 8
# It applies the function to every row of delta and collects the returned rows.
# list(...) just materializes those results, and "with" shuts the pool down
# cleanly once every worker has finished.
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    results = list(pool.map(download_one, delta))


# =============================================================================
# SECTION 6 - MERGE THE RESULTS INTO run_history  [REQUIREMENT 4]
# =============================================================================
# Record what happened so tomorrow's delta query knows what to skip.
# "INSERT ... ON CONFLICT (key) DO UPDATE" is DuckDB's spelling of MERGE:
# insert the row, but if that primary key already exists, update it instead
# (WHEN NOT MATCHED THEN INSERT / WHEN MATCHED THEN UPDATE).
# "excluded" refers to the row we tried to insert - i.e. the SOURCE side of
# the merge. current_timestamp is generated by SQL, not by Python.
#
# The "if results:" guard is here because executemany rejects an empty batch,
# and on a day when nothing changed there is genuinely nothing to write.
if results:
    con.executemany("""
        INSERT INTO run_history VALUES (?, ?, ?, ?, ?, ?, current_timestamp)
        ON CONFLICT (dataset_id) DO UPDATE SET
            modified = excluded.modified, local_file = excluded.local_file,
            status = excluded.status, downloaded_at = excluded.downloaded_at
    """, results)
# executemany runs the statement once per row in "results" - a batched INSERT.
# The ? placeholders are bound parameters (like @p1, @p2). Values are passed
# separately rather than concatenated into the SQL string, which is the same
# reason you'd avoid building dynamic SQL by hand.

con.close()

# Count the successes. This is another list comprehension, and it reads as:
#     SELECT COUNT(*) FROM results WHERE status = 'success'
# (r[5] is the 6th column of the returned row - Python counts from 0.)
ok = sum(1 for r in results if r[5] == "success")
print(f"\nDone. success = {ok} | failed = {len(results) - ok}")
