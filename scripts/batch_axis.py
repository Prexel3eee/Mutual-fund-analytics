"""Batch process Axis Mutual Fund portfolio Excel files."""
import argparse
import json
import logging
import os
import sys
from pathlib import Path

# Import the ETL runner
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.axis_etl import run_etl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("batch_axis")


def main():
    parser = argparse.ArgumentParser(
        description="Batch process Axis Mutual Fund portfolio files"
    )
    parser.add_argument("input_dir", help="Directory containing Excel files")
    parser.add_argument("--output-dir", "-o", default="data/processed/axis", 
                       help="Output directory for JSON files")
    
    args = parser.parse_args()
    
    input_path = Path(args.input_dir)
    output_path = Path(args.output_dir)
    
    if not input_path.exists():
        log.error(f"Input directory not found: {input_path}")
        sys.exit(1)
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all Excel files
    excel_files = sorted(input_path.glob("*.xlsx"))
    excel_files = [f for f in excel_files if not f.name.startswith("~$")]
    
    if not excel_files:
        log.error(f"No Excel files found in {input_path}")
        sys.exit(1)
    
    log.info(f"Found {len(excel_files)} Excel files to process")
    log.info("=" * 60)
    
    results = []
    
    for idx, excel_file in enumerate(excel_files, 1):
        log.info(f"\n[{idx}/{len(excel_files)}] Processing: {excel_file.name}")
        
        try:
            result = run_etl(str(excel_file))
            
            # Determine output filename
            report_date = result["metadata"]["report_date"]
            if report_date:
                # Handle spaces in report date string
                date_obj = None
                try:
                    # Check if it's already YYYY-MM-DD
                    if "-" in report_date and len(report_date.split("-")) == 3:
                         date_suffix = report_date.replace("-", "")[:6]
                    else:
                        date_suffix = excel_file.stem.lower()
                except:
                    date_suffix = excel_file.stem.lower()
            else:
                date_suffix = excel_file.stem.lower()
            
            # Ensure safe filename
            date_suffix = date_suffix.replace(" ", "")
            output_file = output_path / f"axis_equity_holdings_{date_suffix}.json"
            
            # Write JSON
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            log.info(f"    ✓ Written to: {output_file}")
            
            results.append({
                "file": excel_file.name,
                "status": "success",
                "output": str(output_file),
                "schemes": result["metadata"]["schemes_with_equity"],
                "securities": result["metadata"]["total_unique_securities"],
                "holdings": result["metadata"]["total_holdings_records"],
            })
            
        except Exception as e:
            log.error(f"    ✗ Error: {e}")
            results.append({
                "file": excel_file.name,
                "status": "error",
                "error": str(e),
            })
    
    # Summary
    log.info("\n" + "=" * 60)
    log.info("BATCH PROCESSING COMPLETE")
    log.info("=" * 60)
    
    success_count = sum(1 for r in results if r["status"] == "success")
    error_count = len(results) - success_count
    
    log.info(f"Total files: {len(results)}")
    log.info(f"  Success:   {success_count}")
    log.info(f"  Errors:    {error_count}")
    
    if success_count > 0:
        total_schemes = sum(r.get("schemes", 0) for r in results if r["status"] == "success")
        total_holdings = sum(r.get("holdings", 0) for r in results if r["status"] == "success")
        log.info(f"\nTotal schemes extracted: {total_schemes}")
        log.info(f"Total holdings extracted: {total_holdings}")
    
    # Write summary
    summary_file = output_path / "batch_summary.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump({
            "batch_date": __import__("datetime").datetime.now().isoformat(),
            "input_directory": str(input_path),
            "files_processed": len(results),
            "success_count": success_count,
            "error_count": error_count,
            "results": results,
        }, f, indent=2)
    
    log.info(f"\nBatch summary: {summary_file}")
    
    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
