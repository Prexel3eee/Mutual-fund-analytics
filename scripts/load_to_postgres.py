"""
Load SBI MF ETL JSON output into PostgreSQL database.

Maps JSON structure to database tables:
- amc_master
- fund_master  
- security_master
- portfolio_holdings (or initial_portfolio_staging)
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("db_loader")


class DatabaseLoader:
    """Load JSON data into PostgreSQL."""
    
    def __init__(self, connection_params: dict):
        """Initialize with database connection parameters."""
        self.conn_params = connection_params
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cursor = self.conn.cursor()
            log.info(f"Connected to database: {self.conn_params['dbname']}")
        except Exception as e:
            log.error(f"Failed to connect to database: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            log.info("Database connection closed")
    
    def load_json_file(self, json_path: Path) -> dict:
        """Load JSON file."""
        log.info(f"Loading: {json_path.name}")
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    def insert_amc_master(self, amc_data: dict) -> int:
        """
        Insert or get AMC record.
        Returns amc_id.
        """
        amc_name = amc_data["amc_name"]
        short_code = amc_data["short_code"]
        
        # Check if exists
        self.cursor.execute(
            "SELECT amc_id FROM amc_master WHERE amc_name = %s",
            (amc_name,)
        )
        result = self.cursor.fetchone()
        
        if result:
            amc_id = result[0]
            log.debug(f"  AMC exists: {amc_name} (ID: {amc_id})")
            return amc_id
        
        # Insert new AMC
        self.cursor.execute(
            """
            INSERT INTO amc_master (amc_name, short_code)
            VALUES (%s, %s)
            RETURNING amc_id
            """,
            (amc_name, short_code)
        )
        amc_id = self.cursor.fetchone()[0]
        log.info(f"  Inserted AMC: {amc_name} (ID: {amc_id})")
        return amc_id
    
    def insert_fund_master(self, fund_data: dict, amc_id: int) -> int:
        """
        Insert or update fund record.
        Returns fund_id.
        """
        scheme_code = fund_data["scheme_code"]
        scheme_name = fund_data["scheme_name"]
        
        # Check if exists by scheme_code and amc_id
        self.cursor.execute(
            "SELECT fund_id FROM fund_master WHERE amc_id = %s AND fund_id::text = %s",
            (amc_id, scheme_code)
        )
        result = self.cursor.fetchone()
        
        if result:
            fund_id = result[0]
            # Update scheme name if changed
            self.cursor.execute(
                "UPDATE fund_master SET scheme_name = %s WHERE fund_id = %s",
                (scheme_name, fund_id)
            )
            return fund_id
        
        # Insert new fund - use scheme_code as fund_id (integer)
        try:
            fund_id = int(scheme_code)
        except ValueError:
            # If scheme_code is not numeric, generate an ID
            self.cursor.execute("SELECT COALESCE(MAX(fund_id), 0) + 1 FROM fund_master")
            fund_id = self.cursor.fetchone()[0]
        
        self.cursor.execute(
            """
            INSERT INTO fund_master (fund_id, amc_id, scheme_name, is_active)
            VALUES (%s, %s, %s, true)
            ON CONFLICT (fund_id) DO UPDATE SET scheme_name = EXCLUDED.scheme_name
            RETURNING fund_id
            """,
            (fund_id, amc_id, scheme_name)
        )
        fund_id = self.cursor.fetchone()[0]
        log.debug(f"  Inserted fund: {scheme_name} (ID: {fund_id})")
        return fund_id
    
    def insert_securities(self, securities: list[dict]) -> dict[str, int]:
        """
        Insert or update securities.
        Returns dict mapping ISIN -> security_id.
        """
        isin_to_id = {}
        
        for sec in securities:
            isin = sec["isin"]
            name = sec["security_name"]
            sector = sec.get("current_sector")
            industry = sec.get("current_industry")
            
            # Check if exists
            self.cursor.execute(
                "SELECT security_id FROM security_master WHERE isin = %s",
                (isin,)
            )
            result = self.cursor.fetchone()
            
            if result:
                security_id = result[0]
                # Update name/sector if needed
                self.cursor.execute(
                    """
                    UPDATE security_master 
                    SET security_name = %s, current_sector = %s, current_industry = %s
                    WHERE security_id = %s
                    """,
                    (name, sector, industry, security_id)
                )
            else:
                # Insert new security
                self.cursor.execute(
                    """
                    INSERT INTO security_master (isin, security_name, asset_class, current_sector, current_industry)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING security_id
                    """,
                    (isin, name, "Equity", sector, industry)
                )
                security_id = self.cursor.fetchone()[0]
            
            isin_to_id[isin] = security_id
        
        log.info(f"  Processed {len(securities)} securities")
        return isin_to_id
    
    def insert_holdings(self, holdings: list[dict], fund_id_map: dict[str, int], 
                       isin_to_security_id: dict[str, int], report_date: str):
        """Insert portfolio holdings."""
        
        # Delete existing holdings for this fund and date (idempotent)
        fund_ids = list(fund_id_map.values())
        if fund_ids:
            self.cursor.execute(
                "DELETE FROM portfolio_holdings WHERE fund_id = ANY(%s) AND report_date = %s",
                (fund_ids, report_date)
            )
        
        # Prepare data for batch insert
        rows = []
        for h in holdings:
            fund_id = fund_id_map.get(h["scheme_short_code"])
            security_id = isin_to_security_id.get(h["isin"])
            
            if not fund_id or not security_id:
                log.warning(f"  Skipping holding: fund_id={fund_id}, security_id={security_id}")
                continue
            
            rows.append((
                fund_id,
                security_id,
                report_date,
                h.get("quantity"),
                h.get("market_value_lakhs"),
                h.get("pct_to_aum"),
                h.get("industry"),  # sector_at_time
            ))
        
        if rows:
            execute_values(
                self.cursor,
                """
                INSERT INTO portfolio_holdings 
                (fund_id, security_id, report_date, quantity, market_value_lakhs, pct_portfolio, sector_at_time)
                VALUES %s
                """,
                rows
            )
            log.info(f"  Inserted {len(rows)} holdings for {report_date}")
    
    def load_json_to_db(self, json_path: Path):
        """Load a single JSON file into the database."""
        data = self.load_json_file(json_path)
        
        # Extract metadata
        report_date = data["metadata"]["report_date"]
        log.info(f"Report date: {report_date}")
        
        # Insert AMC
        amc_id = self.insert_amc_master(data["amc_master"])
        
        # Insert securities
        isin_to_security_id = self.insert_securities(data["security_master"])
        
        # Insert funds and build mapping
        fund_id_map = {}
        for fund in data["fund_master"]:
            fund_id = self.insert_fund_master(fund, amc_id)
            fund_id_map[fund["scheme_short_code"]] = fund_id
        
        log.info(f"  Processed {len(fund_id_map)} funds")
        
        # Insert holdings
        self.insert_holdings(
            data["portfolio_holdings"],
            fund_id_map,
            isin_to_security_id,
            report_date
        )
        
        # Commit transaction
        self.conn.commit()
        log.info(f"✓ Committed {json_path.name}")


def main():
    parser = argparse.ArgumentParser(description="Load SBI ETL JSON into PostgreSQL")
    parser.add_argument("json_files", nargs="+", help="JSON file(s) to load")
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    parser.add_argument("--dbname", required=True, help="Database name")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        log.setLevel(logging.DEBUG)
    
    # Connection parameters
    conn_params = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.user,
        "password": args.password,
    }
    
    # Expand glob patterns
    json_paths = []
    for pattern in args.json_files:
        if "*" in pattern:
            json_paths.extend(Path(".").glob(pattern))
        else:
            json_paths.append(Path(pattern))
    
    if not json_paths:
        log.error("No JSON files found")
        sys.exit(1)
    
    log.info(f"Loading {len(json_paths)} JSON file(s) into database")
    log.info("=" * 60)
    
    loader = DatabaseLoader(conn_params)
    
    try:
        loader.connect()
        
        for json_path in sorted(json_paths):
            if not json_path.exists():
                log.warning(f"File not found: {json_path}")
                continue
            
            try:
                loader.load_json_to_db(json_path)
            except Exception as e:
                log.error(f"Error loading {json_path.name}: {e}")
                loader.conn.rollback()
                raise
        
        log.info("\n" + "=" * 60)
        log.info("DATABASE LOAD COMPLETE")
        log.info("=" * 60)
        
    except Exception as e:
        log.error(f"Database loading failed: {e}")
        sys.exit(1)
    finally:
        loader.close()


if __name__ == "__main__":
    main()
