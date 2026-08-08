# CMS Hospitals ETL Job

Downloads every CMS dataset tagged with the "Hospitals" theme, converts headers to
snake_case, and runs incrementally (only re-downloading datasets that are new or
changed since the last run). Built with DuckDB — no database server required. Also option to redownload all with a change in a flag in the code.

I'm new to Python, and my core skills are around SQL so I used Claude and Gemini to help write the Python code, and
leaned on SQL Server 2025 to import, explore, and verify the CMS data first — to make
sure I understood the JSON structure and the numbers before automating it. Since I come from mostly sql background, I am using SQL query using duckdb, which is also new to me and installed it today.

## Setup

```
pip install -r requirements.txt
python cms_hospitals_job.py
```

## Output

73 CSVs land in `output/`, one per Hospitals dataset, with snake_case column headers.



## SQL Server import and data profiling

Before writing the Python job, I used SQL Server 2025 thats on my local machine  to import the raw CMS catalog JSON, profile its columns, and confirm the Hospitals theme resolves to the same 73 datasets that the Python version downloads.Please refer to Images folder.
