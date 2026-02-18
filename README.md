# Mutual Fund Portfolio ETL Pipeline

Extract equity holdings data from mutual fund monthly portfolio Excel files into structured JSON and load into PostgreSQL.

## Supported AMCs

- **SBI Mutual Fund** - 68 unique funds
- **Nippon India Mutual Fund** - 71 unique funds  
- **Kotak Mahindra Mutual Fund** - 83 unique funds
- **Axis Mutual Fund** - 50 unique funds

## Quick Start

### 1. Setup

```bash
# Install dependencies
pip install openpyxl psycopg2-binary

# Verify PostgreSQL is running
psql -U postgres -d mutual_fund_db -c "SELECT COUNT(*) FROM amc_master;"
```

### 2. Process Excel Files

```bash
# Single file ETL
python src/sbi_etl.py path/to/January.xlsx
python src/nippon_etl.py path/to/Nippon-January.xlsx
python src/kotak_etl.py path/to/Kotak-January.xlsx
python src/axis_etl.py path/to/Axis-January.xlsx

# Batch process entire year
python scripts/batch_sbi.py SBI-Mutual-Fund/2025
python scripts/batch_nippon.py Nippon-India-Mutual-Fund/2025
python scripts/batch_kotak.py Kotak-Mahindra-Mutual-Fund/2025
python scripts/batch_axis.py Axis-Mutual-Fund/2025
```

### 3. Load into Database

```bash
# Load single file
python scripts/load_to_postgres.py \
  data/processed/sbi/sbi_equity_holdings_202601.json \
  --dbname mutual_fund_db \
  --user postgres \
  --password your_password

# Load all files for an AMC (using wildcard)
python scripts/load_to_postgres.py \
  data/processed/axis/axis_equity_holdings_2025*.json \
  --dbname mutual_fund_db \
  --user postgres \
  --password your_password
```

## What It Does

Each ETL script:
- Parses mutual fund portfolio Excel files (Index sheet + scheme sheets)
- Extracts **equity-only** holdings from "Listed/awaiting listing" sections
- Outputs structured JSON matching the database schema
- Skips debt-only schemes and non-equity assets  
- Deduplicates securities by ISIN
- Aggregates duplicate holdings within same scheme

## Output Structure

JSON files contain:
```json
{
  "metadata": {
    "source_file": "January.xlsx",
    "report_date": "2026-01-31",
    "total_schemes": 80,
    "schemes_with_equity": 44,
    "total_unique_securities": 563,
    "total_holdings_records": 3064
  },
  "amc_master": {
    "amc_name": "Axis Mutual Fund",
    "short_code": "AXIS"
  },
  "fund_master": [...],
  "security_master": [...],
  "portfolio_holdings": [...]
}
```

## Database Schema

- `amc_master` — AMC information (amc_id, amc_name, short_code)
- `fund_master` — Fund/scheme details (fund_id, amc_id, scheme_name)
- `security_master` — Unique securities (security_id, isin, security_name, industry)
- `portfolio_holdings` — Holdings data (fund_id, security_id, report_date, quantity, market_value, %)

## Project Structure

```
NewMF-analytics/
├── SBI-Mutual-Fund/2025/        # SBI raw Excel files
├── Nippon-India-Mutual-Fund/2025/
├── Kotak-Mahindra-Mutual-Fund/2025/
├── Axis-Mutual-Fund/2025/
├── data/processed/
│   ├── sbi/                     # SBI JSON outputs
│   ├── nippon/
│   ├── kotak/
│   └── axis/
├── src/
│   ├── sbi_etl.py              # SBI ETL script
│   ├── nippon_etl.py           # Nippon ETL script
│   ├── kotak_etl.py            # Kotak ETL script
│   └── axis_etl.py             # Axis ETL script
├── scripts/
│   ├── batch_sbi.py            # SBI batch processor
│   ├── batch_nippon.py         # Nippon batch processor
│   ├── batch_kotak.py          # Kotak batch processor
│   ├── batch_axis.py           # Axis batch processor
│   └── load_to_postgres.py     # Database loader
└── README.md
```

## Database Stats (as of Feb 2026)

- **Total Holdings**: ~165,000 records
- **Total Securities**: 873 unique ISINs
- **Report Dates**: 13 months (Jan 2025 - Jan 2026)
- **Total Funds**: 272 unique schemes across 4 AMCs

## Example Queries

### Top 10 holdings by value (latest date)
```sql
SELECT sm.security_name, 
       SUM(ph.market_value_lakhs) as total_value
FROM portfolio_holdings ph
JOIN security_master sm ON ph.security_id = sm.security_id
WHERE ph.report_date = '2026-01-31'
GROUP BY sm.security_name
ORDER BY total_value DESC
LIMIT 10;
```

### Holdings per AMC (latest date)
```sql
SELECT am.amc_name, 
       COUNT(*) as holdings_count,
       COUNT(DISTINCT fm.fund_id) as fund_count
FROM portfolio_holdings ph
JOIN fund_master fm ON ph.fund_id = fm.fund_id
JOIN amc_master am ON fm.amc_id = am.amc_id
WHERE ph.report_date = '2026-01-31'
GROUP BY am.amc_name;
```

### Top securities across all funds
```sql
SELECT sm.security_name,
       COUNT(DISTINCT ph.fund_id) as fund_count,
       SUM(ph.market_value_lakhs) as total_value
FROM portfolio_holdings ph
JOIN security_master sm ON ph.security_id = sm.security_id
WHERE ph.report_date = '2026-01-31'
GROUP BY sm.security_name
ORDER BY fund_count DESC
LIMIT 10;
```

## AMC-Specific Notes

### SBI Mutual Fund
- Uses "Index" sheet for scheme mapping
- Equity data in column A (index 0)
- Handles 13-column BRSR format (skips ESG data)

### Nippon India Mutual Fund  
- Uses "INDEX" or "Index" sheet
- Equity data in column A (index 0)
- Special handling for segregated portfolios

### Kotak Mahindra Mutual Fund
- Uses "Scheme" sheet for abbreviation mapping
- Equity data in column B (index 1)
- 117 total sheets per file

### Axis Mutual Fund
- Uses "Index" sheet for short name mapping
- **Equity data in column B (index 1)** ⚠️
- Stricter end-condition checks for companies with "Total" in name
- Aggregates duplicate ISINs within schemes

### Motilal Oswal Mutual Fund
- Uses "Index" sheet with mapping in columns D & E
- **Equity data in Index 3 (ISIN) & Index 1 (Name)**
- Requires specific end-of-data detection (Total marker in Name column)
- Handles both .xls and .xlsx formats (requires conversion)

## Technical Details

- **Language**: Python 3.x
- **Dependencies**: `openpyxl`, `psycopg2-binary`
- **Data Columns**: Name, ISIN, Industry/Sector, Quantity, Market Value, % to AUM/NAV
- **Idempotent Loads**: Database loader deletes existing records before insert

## Troubleshooting

### Common Issues

1. **File Not Found**: Ensure Excel files are in correct directory structure
2. **Empty Output**: Check equity section markers match AMC format
3. **Duplicate Errors**: Run cleanup scripts or use idempotent loader
4. **Date Format**: Ensure dates are parsed as YYYY-MM-DD

### Verification Scripts

Check data integrity:
```bash
# Verify latest report dates per AMC
python -c "import psycopg2; ..."

# Check specific fund holdings count
python verify_axis500.py
```

## Future Enhancements

- [ ] Add HDFC Mutual Fund support
- [ ] Add ICICI Mutual Fund support
- [ ] Refactor common ETL logic into base class
- [ ] Create unified batch processor for all AMCs
- [ ] Add data validation and anomaly detection
- [ ] Generate portfolio analytics and reports
