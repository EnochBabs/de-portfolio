# Healthcare Readmission Pipeline

A medallion architecture pipeline built on Azure Databricks that ingests, 
cleans, and models US hospital readmission data published by the Centers 
for Medicare and Medicaid Services (CMS).

---

## The Business Problem

Hospitals that readmit too many patients within 30 days of discharge are 
financially penalised by CMS through the Hospital Readmissions Reduction 
Program (HRRP).

This pipeline answers three questions:

- Which hospitals have the highest 30-day readmission rates nationally?
- Which US states perform worst on readmission metrics?
- Which medical conditions drive the most excess readmissions?

---

## Architecture

This pipeline follows the medallion architecture pattern — the standard 
approach used by most modern data engineering teams.

![Pipeline Architecture](assets/architecture.png)

Each layer has a strict contract. Bronze never reads from Silver. 
Silver never reads from Gold. The flow is always one direction.

---

## Azure Stack

| Service | Name | Purpose |
|---|---|---|
| ADLS Gen2 | theportfoliostorage | Data lake storage |
| Azure Databricks | de-portfolio-dbx | Pipeline compute |
| Azure Key Vault | deportfolio-kv | Secret management |
| Unity Catalog | healthcare catalog | Data governance |
| Azure Data Factory | de-portfolio-adf-enoch | Orchestration |

All resources were provisioned from scratch using the Azure CLI, 
not the portal. The commands are documented in the setup notes 
at the bottom of this README.

---

## Dataset

Source: CMS Hospital Readmissions Reduction Program
URL: https://data.cms.gov/provider-data/dataset/9n3s-kdb3
Size: 18,330 rows, 12 columns, updated annually
Scope: approximately 3,000 US hospitals across 6 medical conditions

The six conditions tracked are heart failure, pneumonia, hip and knee 
replacement, COPD, acute myocardial infarction, and coronary artery 
bypass graft surgery.

---

## Data Quality Issues

This dataset has several real-world data quality problems that the 
Silver layer fixes. Understanding these issues is as important as 
the pipeline code itself.

### Suppressed values

CMS suppresses readmission data for hospitals with fewer than 25 cases 
to protect patient privacy under HIPAA. These suppressed values appear 
as strings inside numeric columns:

- "Too Few to Report"
- "N/A"

56% of rows in this dataset contain suppressed values. This is not a 
data error — it is an intentional privacy protection that a pipeline 
must handle explicitly.

The fix has a specific order that matters:

1. Flag suppressed rows with an is_suppressed boolean column
2. Replace suppressed strings with null
3. Cast columns to numeric types

Reversing steps 1 and 2, or casting before nulling, causes the 
pipeline to fail or lose information.

### Mixed date formats

The start_date column was ingested as a string while end_date was 
correctly parsed as a date. Both represent the same concept — the 
measurement period boundary — but Spark inferred them differently 
because the formats were inconsistent in the source file.

### Inconsistent hospital names

The same hospital appears with different name casing and spacing 
across rows. This is why the pipeline always joins on facility_id, 
never on facility_name. IDs are reliable. Names are not.

### Column names with spaces

Delta Lake rejects column names containing spaces. All 12 source 
columns were renamed to snake_case at the Bronze layer.

---

## Pipeline Notebooks

### 01_bronze_ingest

Reads the raw CMS CSV from a Unity Catalog volume and writes it to 
a Bronze Delta table with no transformations applied.

Adds three metadata columns to every row:
- _ingested_at: timestamp when the row was loaded
- _source_file: name of the source file
- _source_year: the data year

These columns do not exist in the source data. They are pipeline 
metadata that make debugging and auditing possible months later.

Key output: healthcare.bronze.cms_hrrp_raw — 18,330 rows

### 02_silver_transform

Handles all data quality issues identified in Bronze.

- Flags suppressed rows before touching numeric columns
- Replaces suppression strings with null
- Casts all numeric columns to their correct types
- Parses start_date from string to date
- Trims whitespace from string columns
- Deduplicates on facility_id and measure_name

Key output: healthcare.silver.cms_hrrp_clean — 18,330 rows, 
correct types, is_suppressed flag added

### 03_gold_serve

Builds three aggregate tables from the Silver layer using only 
unsuppressed rows for calculations.

hospital_readmission_summary — one row per hospital with national 
rank, average excess ratio, risk tier classification (High, Medium, 
Low), and penalty flag. 2,477 hospitals.

state_readmission_summary — one row per state with average excess 
ratio, state rank, and penalty flag. 51 states.

condition_readmission_summary — one row per medical condition with 
average excess ratio and condition rank. 6 conditions.

---

## Key Findings

### Hospital performance

The worst performing hospital nationally is Surgical Institute of 
Reading in Pennsylvania with an excess readmission ratio of 1.31, 
meaning it readmits 31% more patients than expected for its patient mix.

### State performance

Massachusetts ranks as the worst performing state with an average 
excess ratio of 1.044, followed by New Jersey and Florida. This is 
notable given Massachusetts is home to some of the most prestigious 
academic medical centres in the world.

### Condition performance

Hip and knee replacement has the highest excess ratio at 1.036 despite 
having the lowest predicted readmission rate of all conditions at 5.4%. 
This suggests patients are being discharged too quickly after joint 
replacement surgery.

### Small hospital insight

Suppressed hospitals — those too small to have reportable data — 
averaged an excess ratio of 0.986 compared to 1.009 for larger 
reportable hospitals. Smaller facilities appear to perform better 
on readmission outcomes despite having fewer resources. The data 
raises the question but does not answer it.

---

## Unity Catalog Setup

Setting up Unity Catalog on a personal Azure subscription requires 
three things to be created in sequence before any catalog can be 
registered:

An Access Connector — an Azure resource that gives Databricks a 
managed identity to present to Azure Storage.

A Storage Credential — registered inside Unity Catalog, referencing 
the Access Connector, proving Databricks is authorised to access storage.

An External Location — also registered inside Unity Catalog, 
mapping a specific ADLS Gen2 path to the Storage Credential.

Only after all three exist can you create a catalog pointing at 
that storage location. Skipping any step produces a different 
error that does not clearly explain what is missing. This took 
significant debugging to resolve and is worth documenting for 
anyone building a similar setup.

---

## Security Notes

Secrets — service principal client ID, client secret, and tenant ID — 
are stored in Azure Key Vault and accessed in notebooks exclusively 
through dbutils.secrets.get(). No credentials appear in notebook code.

During development, a service principal secret was accidentally 
committed to a notebook that was then pushed to GitHub. GitHub's 
secret scanning detected and blocked the push. The credential was 
immediately rotated. This is documented here because it is a real 
mistake that happens and the correct response is to rotate immediately, 
not to hope nobody noticed.
