# SBI Mutual Fund ETL Pipeline

Extract equity holdings data from SBI Mutual Fund monthly portfolio Excel files into structured JSON.

## Quick Start

```bash
# Single file
python src/sbi_etl.py January.xlsx

# Batch process all files in a folder
python scripts/batch_process.py 2025 --output-dir data/processed/2025
```

## What It Does

- Parses SBI MF portfolio Excel files (Index sheet + scheme sheets)
- Extracts **equity-only** holdings from "Listed/awaiting listing" section
- Outputs structured JSON matching the database schema
- Skips debt-only schemes and non-equity assets
- Deduplicates securities by ISIN

## Output Structure

JSON files contain:
- `metadata` — extraction statistics
- `amc_master` — AMC info (SBI Mutual Fund)
- `fund_master` — scheme details (code, name, holdings count)
- `security_master` — unique securities (ISIN, name, sector)
- `portfolio_holdings` — detailed holdings (scheme, ISIN, quantity, market value, % AUM)

## Results Summary

### 2025 Data (12 months)
- **12/12 files** processed successfully (0 errors)
- **774 total scheme extractions**
- **30,701 total holdings** extracted
- Output: `data/processed/2025/`

### January 2026
- **67 schemes** with equity
- **456 unique securities**
- **2,571 holdings**
- Output: `data/processed/sbi_equity_holdings_202601.json`

## Project Structure

```
NewMF-analytics/
├── 2025/                    # 2025 monthly Excel files
├── data/
│   └── processed/
│       ├── 2025/            # 2025 JSON outputs
│       └── sbi_equity_holdings_202601.json
├── src/
│   └── sbi_etl.py          # Main ETL script
├── scripts/
│   └── batch_process.py     # Batch processing script
└── README.md
```

## Technical Details

- **Language**: Python 3.x
- **Dependencies**: `openpyxl`
- **Data Columns Extracted**: Name, ISIN, Industry, Quantity, Market Value, % to AUM
- **BRSR/ESG Data**: Skipped (only 1/67 schemes has this data)
