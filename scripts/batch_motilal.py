"""Batch process Motilal Oswal Mutual Fund Excel files."""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from motilal_etl import run_etl
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("batch_motilal")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Batch process Motilal Oswal Excel files")
    parser.add_argument("input_dir", help="Directory containing Excel files")
    parser.add_argument("--output-dir", default="data/processed/motilal", help="Output directory")
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all Excel files
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
        try:
            log.info(f"\n📁 Processing: {excel_file.name}")
            
            # Run ETL
            data = run_etl(str(excel_file))
            
            # Extract month from filename
            month_name = excel_file.stem  # e.g., "January", "February"
            report_date = data["metadata"]["report_date"]
            
            # If report date is current date (ETL couldn't find it), use filename
            from datetime import datetime
            today = datetime.now().strftime("%Y-%m-%d")
            
            if report_date == today or report_date.startswith("2026"):
                # Use filename to determine correct 2025 date
                month_map = {
                    "january": "2025-01-31",
                    "february": "2025-02-28",
                    "march": "2025-03-31",
                    "april": "2025-04-30",
                    "may": "2025-05-31",
                    "june": "2025-06-30",
                    "july": "2025-07-31",
                    "august": "2025-08-31",
                    "september": "2025-09-30",
                    "october": "2025-10-31",
                    "november": "2025-11-30",
                    "december": "2025-12-31",
                }
                report_date_override = month_map.get(month_name.lower())
                if report_date_override:
                    report_date = report_date_override
                    data["metadata"]["report_date"] = report_date
                    log.info(f"  📅 Corrected date from filename: {report_date}")
            
            # Convert report date to YYYYMM format for output filename
            date_obj = datetime.strptime(report_date, "%Y-%m-%d")
            date_suffix = date_obj.strftime("%Y%m")
            
            # Output filename
            output_file = output_dir / f"motilal_equity_holdings_{date_suffix}.json"
            
            # Write output
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
