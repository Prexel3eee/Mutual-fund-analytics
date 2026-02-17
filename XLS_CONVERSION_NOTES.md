# XLS to XLSX Conversion - Documentation

## Purpose
Convert all `.xls` (old Excel 97-2003 format) files to `.xlsx` (modern Excel format) without any data or formatting loss.

## Why Convert?
1. **Modern Format**: `.xlsx` is the modern standard (since Excel 2007)
2. **Better Compatibility**: Works better with modern tools and libraries
3. **Smaller File Size**: `.xlsx` uses compression, typically 50-75% smaller
4. **More Features**: Supports more rows, columns, and modern Excel features

## How It Works

Uses **Excel COM Automation** via `pywin32`:
- Opens Excel in the background (invisible)
- Opens each `.xls` file
- Saves as `.xlsx` with full fidelity
- Closes and deletes the original `.xls` file

## Installation

The script auto-installs `pywin32` if needed:
```bash
pip install pywin32
```

## Process Flow

**Phase 3** (runs after download and extraction):
1. Scans for all `.xls` files
2. Launches Excel (hidden)
3. For each file:
   - Opens the `.xls` file
   - Saves as `.xlsx`
   - Deletes original `.xls`
4. Quits Excel

## Example Output

```
======================================================================
Converting XLS to XLSX Format
======================================================================

Found 13 .xls files to convert

📄 Converting: January.xls
   ✓ Saved as: January.xlsx
   🗑 Removed: January.xls

📄 Converting: February.xls
   ✓ Saved as: February.xlsx
   🗑 Removed: February.xls

...

======================================================================
XLS to XLSX Conversion Complete!
  Files found: 13
  Successfully converted: 13
======================================================================
```

## Benefits

✅ **No Data Loss**: Excel's native conversion ensures 100% accuracy
✅ **Preserves Formatting**: All styles, colors, formulas, charts preserved
✅ **Automatic**: Runs automatically at the end of the download script
✅ **Clean**: Removes old `.xls` files after conversion

## Requirements

- **Windows OS** ✅ (you have this)
- **Microsoft Excel installed** ✅ (you have this)
- **pywin32 library** (auto-installed)

## When It Runs

Automatically runs as **Phase 3** after:
- Phase 1: Download all files ✓
- Phase 2: Extract all ZIPs ✓
- Phase 3: Convert .xls → .xlsx ✓

## Files Affected

Currently affects **Trust Mutual Fund** files (13 files):
- Before: `Trust-Mutual-Fund/2025/January.xls`
- After: `Trust-Mutual-Fund/2025/January.xlsx`

Any future `.xls` downloads will also be auto-converted.
