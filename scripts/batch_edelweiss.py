"""Batch process Edelweiss Mutual Fund Excel files."""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from edelweiss_etl import run_etl
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("batch_edelweiss")

MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Batch process Edelweiss Excel files")
    parser.add_argument("input_dir", help="Directory containing Excel files")
    parser.add_argument("--output-dir", default="data/processed/edelweiss", help="Output directory")

    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    excel_files = sorted(input_dir.glob("*.xlsx"))

    if not excel_files:
        log.error(f"No Excel files found in {input_dir}")
        return

    log.info(f"Found {len(excel_files)} Excel files to process")
    log.info("=" * 40)

    success_count = 0
    error_count = 0
    total_schemes = 0
    total_holdings = 0

    for excel_file in excel_files:
        if excel_file.name.startswith("~$"):
            continue  # Skip temp files

        try:
            log.info(f"\n📁 Processing: {excel_file.name}")

            data = run_etl(str(excel_file))

            month_name = excel_file.stem.lower()
            report_date = data["metadata"]["report_date"]

            # Derive year from parent directory
            year = input_dir.name  # e.g., "2025" or "2026"

            # If report date is today or wrong year, fix from filename
            today = datetime.now().strftime("%Y-%m-%d")
            if report_date == today or not report_date.startswith(year):
                month_num = MONTH_MAP.get(month_name)
                if month_num:
                    import calendar
                    y = int(year)
                    m = int(month_num)
                    last_day = calendar.monthrange(y, m)[1]
                    report_date = f"{year}-{month_num}-{last_day:02d}"
                    data["metadata"]["report_date"] = report_date
                    log.info(f"  📅 Corrected date from filename: {report_date}")

            date_obj = datetime.strptime(report_date, "%Y-%m-%d")
            date_suffix = date_obj.strftime("%Y%m")

            output_file = output_dir / f"edelweiss_equity_holdings_{date_suffix}.json"

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            schemes = data["metadata"]["schemes_with_equity"]
            holdings = data["metadata"]["total_holdings_records"]

            total_schemes += schemes
            total_holdings += holdings
            success_count += 1

            log.info(f"  ✓ Written to: {output_file}")
            log.info(f"    Schemes: {schemes}, Holdings: {holdings}")

        except Exception as e:
            log.error(f"  ✗ Failed: {e}")
            error_count += 1

    # Summary
    log.info("\n" + "=" * 40)
    log.info("BATCH PROCESSING COMPLETE")
    log.info("=" * 40)
    log.info(f"Total files: {len(excel_files)}")
    log.info(f"  Success:   {success_count}")
    log.info(f"  Errors:    {error_count}")
    log.info(f"  Total schemes extracted: {total_schemes}")
    log.info(f"  Total holdings extracted: {total_holdings}")
    log.info("=" * 40)


if __name__ == "__main__":
    main()
