"""Batch process all Bajaj Finserv Mutual Fund Excel files."""

import logging
import os
import re
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("batch_bajaj")

MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}

def get_date_from_filename(filepath: Path) -> str | None:
    """Derive YYYY-MM-DD report date from path like .../2025/April.xlsx."""
    parts = filepath.parts
    month_name = filepath.stem.lower()
    month_num = MONTH_MAP.get(month_name)
    if not month_num:
        return None
    # Find year in path
    for part in reversed(parts):
        if re.match(r'^\d{4}$', part):
            year = part
            # Last day of month
            import calendar
            last_day = calendar.monthrange(int(year), int(month_num))[1]
            return f"{year}-{month_num}-{last_day:02d}"
    return None


def main():
    if len(sys.argv) < 2:
        print("Usage: python batch_bajaj.py <directory>")
        print("Example: python batch_bajaj.py Bajaj-Finserv-Mutual-Fund/2025")
        sys.exit(1)

    input_dir = Path(sys.argv[1])
    if not input_dir.exists():
        log.error(f"Directory not found: {input_dir}")
        sys.exit(1)

    files = sorted(input_dir.glob("*.xlsx"))
    if not files:
        log.error(f"No .xlsx files found in {input_dir}")
        sys.exit(1)

    log.info(f"Found {len(files)} Excel files to process")
    log.info("=" * 60)

    success, errors = 0, 0
    total_schemes, total_holdings = 0, 0

    for f in files:
        log.info(f"\n📁 Processing: {f.name}")

        date_from_filename = get_date_from_filename(f)
        out_dir = Path("data/processed/bajaj")
        out_dir.mkdir(parents=True, exist_ok=True)

        # Determine output filename
        if date_from_filename:
            yyyymm = date_from_filename[:7].replace("-", "")
            out_file = out_dir / f"bajaj_equity_holdings_{yyyymm}.json"
        else:
            out_file = out_dir / f"bajaj_equity_holdings_{f.stem.lower()}.json"

        cmd = [
            sys.executable, "src/bajaj_etl.py",
            str(f),
            "-o", str(out_file),
        ]
        if date_from_filename:
            cmd += ["--date", date_from_filename]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            # Parse summary from stderr
            schemes = holdings = 0
            for line in result.stderr.splitlines():
                if "Schemes:" in line:
                    m = re.search(r'Schemes:\s*(\d+)', line)
                    if m:
                        schemes = int(m.group(1))
                if "Total holdings:" in line:
                    m = re.search(r'Total holdings:\s*(\d+)', line)
                    if m:
                        holdings = int(m.group(1))
            log.info(f"  ✓ {out_file.name}  Schemes: {schemes}, Holdings: {holdings}")
            success += 1
            total_schemes += schemes
            total_holdings += holdings
        else:
            log.error(f"  ✗ FAILED: {f.name}")
            log.error(result.stderr[-500:])
            errors += 1

    log.info("\n" + "=" * 60)
    log.info("BATCH PROCESSING COMPLETE")
    log.info("=" * 60)
    log.info(f"Total files: {len(files)}")
    log.info(f"  Success:   {success}")
    log.info(f"  Errors:    {errors}")
    log.info(f"  Total schemes extracted: {total_schemes}")
    log.info(f"  Total holdings extracted: {total_holdings}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
