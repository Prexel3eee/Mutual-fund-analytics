# Contributing to Mutual Fund ETL Pipeline

## Adding a New AMC

Follow this pattern to add support for a new Asset Management Company:

### 1. Analysis Phase

Create an analysis script to understand the Excel file structure:

```python
"""analyze_<amc>.py"""
import openpyxl

wb = openpyxl.load_workbook('path/to/file.xlsx', read_only=True)

# 1. Check total sheets and identify index sheet
print(f"Total sheets: {len(wb.sheetnames)}")
print(f"First 5: {wb.sheetnames[:5]}")
print(f"Last 5: {wb.sheetnames[-5:]}")

# 2. Find equity data markers
ws = wb['SomeSchemeSheet']
rows = list(ws.iter_rows(max_row=50, values_only=True))
for i, r in enumerate(rows):
    for j, cell in enumerate(r):
        if cell and 'equity' in str(cell).lower():
            print(f"Row {i+1}, Col {j}: {cell}")

# 3. Identify column positions for data extraction
```

**Key Questions:**
- Which sheet contains scheme name mappings?
- Which column has equity section markers?
- What are the column indices for: Name, ISIN, Industry, Quantity, Market Value, % NAV?
- What triggers the end of equity data section?

### 2. ETL Script Development

Create `src/<amc>_etl.py` following this template:

```python
"""ETL script for <AMC> Mutual Fund."""
import argparse
import json
import logging
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import openpyxl

# Define column indices (0-indexed)
COL_NAME = 1
COL_ISIN = 2
COL_INDUSTRY = 3
COL_QUANTITY = 4
COL_MARKET_VALUE = 5
COL_PCT_NAV = 6

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("<amc>_etl")


def safe_float(val: Any) -> float | None:
    """Convert value to float, return None if invalid."""
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "NIL", "N/A", "#", "-", "Total", "Sub Total"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def clean_str(val: Any) -> str | None:
    """Clean string value, return None if empty."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s not in ("", "NIL", "N/A", "#", "-") else None


def parse_index_sheet(wb: openpyxl.Workbook) -> dict[str, str]:
    """Parse index sheet for scheme mappings."""
    # Implement AMC-specific logic
    pass


def extract_equity_holdings(ws_rows: list[tuple], scheme_info: dict) -> dict | None:
    """Extract listed equity holdings from a scheme sheet."""
    # Find equity section markers
    # Extract data rows
    # Aggregate duplicates if needed
    # Return structured dict
    pass


def run_etl(excel_path: str) -> dict:
    """Run ETL process."""
    # Load workbook
    # Parse index sheet
    # Extract report date
    # Process each scheme sheet
    # Build output structure
    pass


def main():
    parser = argparse.ArgumentParser(
        description="Extract equity holdings from <AMC> portfolio Excel"
    )
    parser.add_argument("excel_file", help="Path to Excel file")
    parser.add_argument("--output", "-o", help="Output JSON file path")
    parser.add_argument("--verbose", "-v", action="store_true")
    
    args = parser.parse_args()
    
    # Run ETL and save output
    pass


if __name__ == "__main__":
    main()
```

### 3. Batch Processing Script

Create `scripts/batch_<amc>.py`:

```python
"""Batch process <AMC> Excel files."""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from <amc>_etl import run_etl

# Similar structure to existing batch scripts
```

### 4. Testing & Validation

1. **Single File Test**: Process one month's data
   ```bash
   python src/<amc>_etl.py path/to/January.xlsx
   ```

2. **Validate Output**: Check JSON structure matches schema
   ```python
   import json
   with open('output.json') as f:
       data = json.load(f)
       assert 'metadata' in data
       assert 'fund_master' in data
       # etc.
   ```

3. **Batch Test**: Process full year
   ```bash
   python scripts/batch_<amc>.py <AMC>-Mutual-Fund/2025
   ```

4. **Database Load Test**: Load into PostgreSQL
   ```bash
   python scripts/load_to_postgres.py data/processed/<amc>/*.json \
     --dbname mutual_fund_db --user postgres --password pwd
   ```

5. **Verification**: Run sanity checks
   ```sql
   SELECT COUNT(*) FROM portfolio_holdings ph
   JOIN fund_master fm ON ph.fund_id = fm.fund_id
   JOIN amc_master am ON fm.amc_id = am.amc_id
   WHERE am.short_code = '<AMC_CODE>';
   ```

## Common Patterns & Best Practices

### Handling Different Excel Formats

**Pattern 1: Equity markers in Column 0** (SBI, Nippon, Kotak)
```python
cell = clean_str(r[0])
if cell and "EQUITY" in cell.upper() and "RELATED" in cell.upper():
    equity_section_row = i
```

**Pattern 2: Equity markers in Column 1** (Axis)
```python
cell = clean_str(r[1])
if cell and "EQUITY" in cell.upper() and "RELATED" in cell.upper():
    equity_section_row = i
```

### Detecting End of Equity Data

**Strict checks** to avoid false positives (e.g., company name = "Adani Total Gas"):
```python
cell_col1 = clean_str(r[1])
if cell_col1:
    c_lower = cell_col1.lower().strip()
    is_end_marker = (
        c_lower == "total" or 
        c_lower == "sub total" or 
        c_lower.startswith("(b)")
    )
    if is_end_marker and (not isin or len(isin) != 12):
        break
```

### Aggregating Duplicate ISINs

Always aggregate duplicates within the same scheme:
```python
aggregated_holdings = {}
for h in holdings:
    isin = h["isin"]
    if isin in aggregated_holdings:
        existing = aggregated_holdings[isin]
        existing["quantity"] = (existing["quantity"] or 0) + (h["quantity"] or 0)
        existing["market_value_lakhs"] = (existing["market_value_lakhs"] or 0) + (h["market_value_lakhs"] or 0)
    else:
        aggregated_holdings[isin] = h

holdings = list(aggregated_holdings.values())
```

### Date Parsing

Handle multiple formats and case sensitivity:
```python
import re

def parse_date(val: Any) -> str | None:
    """Parse date to YYYY-MM-DD format."""
    if val is None:
        return None
    s = str(val).strip()
    
    # Clean up
    s = s.replace(",", ", ")
    s = re.sub(r'\s+', ' ', s)
    
    # Try common formats
    formats = (
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%B %d, %Y",  # January 31, 2026
    )
    
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    # Try with title case
    s_title = s.title()
    for fmt in formats:
        try:
            return datetime.strptime(s_title, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    return None
```

## Code Style

- Use type hints for function signatures
- Add docstrings to all functions
- Use `logging` for status messages, not `print()`
- Handle errors gracefully with try/except
- Keep functions focused and single-purpose
- Use meaningful variable names

## Testing Checklist

- [ ] Single file ETL produces valid JSON
- [ ] Batch processing handles all files without errors
- [ ] Database loading is idempotent (can re-run safely)
- [ ] Holdings count matches source Excel totals
- [ ] No duplicate holdings within same scheme
- [ ] All ISINs are 12 characters
- [ ] Market values sum correctly per scheme
- [ ] Report dates are in YYYY-MM-DD format

## Documentation

After adding a new AMC, update:
- [ ] `README.md` — Add to supported AMCs list
- [ ] `README.md` — Add AMC-specific notes section
- [ ] `task.md` — Add new phase for the AMC
- [ ] `walkthrough.md` — Document the ETL development process

## Getting Help

If you encounter issues:
1. Check existing ETL scripts for similar patterns
2. Run analysis script to understand Excel structure
3. Add debug logging to identify where extraction fails
4. Verify column indices match actual data positions
5. Test with multiple Excel files to ensure consistency
