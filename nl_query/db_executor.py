"""
Safe SQL Executor
Validates and executes SQL queries with safety checks and timeouts.
Only allows read-only (SELECT / WITH) queries.
"""

import psycopg2
import psycopg2.extras
import re
import time
from typing import Optional


class SafeExecutor:
    """Execute SQL queries with safety validation."""
    
    # Dangerous keywords that indicate write operations
    _BLOCKED_KEYWORDS = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'TRUNCATE',
        'CREATE', 'GRANT', 'REVOKE', 'COPY', 'VACUUM', 'REINDEX',
        'LOCK', 'COMMENT', 'SECURITY', 'OWNER',
    ]
    
    # Max rows to return
    MAX_ROWS = 500
    
    # Query timeout in milliseconds
    QUERY_TIMEOUT_MS = 30_000
    
    def __init__(self, conn_params: dict):
        self.conn_params = conn_params
    
    def validate_query(self, sql: str) -> tuple[bool, str]:
        """
        Validate a SQL query for safety.
        Returns (is_valid, message).
        """
        if not sql or not sql.strip():
            return False, "Empty query"
        
        cleaned = sql.strip().rstrip(';')
        
        # Remove comments
        cleaned_no_comments = re.sub(r'--.*$', '', cleaned, flags=re.MULTILINE)
        cleaned_no_comments = re.sub(r'/\*.*?\*/', '', cleaned_no_comments, flags=re.DOTALL)
        
        upper = cleaned_no_comments.upper().strip()
        
        # Must start with SELECT or WITH (CTE)
        if not (upper.startswith('SELECT') or upper.startswith('WITH')):
            return False, "Only SELECT and WITH (CTE) queries are allowed"
        
        # Check for blocked keywords (word boundary match)
        for kw in self._BLOCKED_KEYWORDS:
            pattern = rf'\b{kw}\b'
            if re.search(pattern, upper):
                return False, f"Blocked keyword detected: {kw}. Only read-only queries allowed."
        
        # Check for multiple statements (semicolons in middle)
        # Allow trailing semicolon but not multiple statements
        statements = [s.strip() for s in cleaned_no_comments.split(';') if s.strip()]
        if len(statements) > 1:
            return False, "Multiple statements not allowed. Please use a single query."
        
        return True, "Query is valid"
    
    def execute(self, sql: str) -> dict:
        """
        Execute a validated SQL query and return results.
        
        Returns dict with:
        - success: bool
        - columns: list of column names
        - rows: list of tuples
        - row_count: int
        - truncated: bool (if rows exceeded MAX_ROWS)
        - execution_time_ms: float
        - error: str or None
        """
        # Validate first
        is_valid, message = self.validate_query(sql)
        if not is_valid:
            return {
                "success": False,
                "columns": [],
                "rows": [],
                "row_count": 0,
                "truncated": False,
                "execution_time_ms": 0,
                "error": f"⛔ {message}",
            }
        
        conn = None
        try:
            conn = psycopg2.connect(**self.conn_params)
            cur = conn.cursor()
            
            # Set statement timeout
            cur.execute(f"SET statement_timeout = {self.QUERY_TIMEOUT_MS}")
            
            # Execute
            start = time.perf_counter()
            cur.execute(sql)
            elapsed_ms = (time.perf_counter() - start) * 1000
            
            # Fetch results
            if cur.description is None:
                return {
                    "success": True,
                    "columns": [],
                    "rows": [],
                    "row_count": 0,
                    "truncated": False,
                    "execution_time_ms": round(elapsed_ms, 1),
                    "error": None,
                }
            
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchmany(self.MAX_ROWS + 1)
            
            truncated = len(rows) > self.MAX_ROWS
            if truncated:
                rows = rows[:self.MAX_ROWS]
            
            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "truncated": truncated,
                "execution_time_ms": round(elapsed_ms, 1),
                "error": None,
            }
        
        except psycopg2.extensions.QueryCanceledError:
            return {
                "success": False,
                "columns": [],
                "rows": [],
                "row_count": 0,
                "truncated": False,
                "execution_time_ms": self.QUERY_TIMEOUT_MS,
                "error": f"⏱️ Query timed out after {self.QUERY_TIMEOUT_MS / 1000:.0f}s. Try a simpler query.",
            }
        except Exception as e:
            return {
                "success": False,
                "columns": [],
                "rows": [],
                "row_count": 0,
                "truncated": False,
                "execution_time_ms": 0,
                "error": f"❌ Database error: {e}",
            }
        finally:
            if conn:
                conn.close()


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    
    load_dotenv()
    
    params = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "dbname": os.getenv("DB_NAME", "mutual_fund_db"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "vivek"),
    }
    
    executor = SafeExecutor(params)
    
    # Test valid query
    print("Test 1 - Valid SELECT:")
    ok, msg = executor.validate_query("SELECT COUNT(*) FROM amc_master")
    print(f"  Valid: {ok}, Message: {msg}")
    
    # Test invalid query
    print("\nTest 2 - Blocked DROP:")
    ok, msg = executor.validate_query("DROP TABLE amc_master")
    print(f"  Valid: {ok}, Message: {msg}")
    
    # Test execution
    print("\nTest 3 - Execute query:")
    result = executor.execute("SELECT amc_id, amc_name FROM amc_master LIMIT 5")
    print(f"  Success: {result['success']}")
    print(f"  Columns: {result['columns']}")
    print(f"  Rows: {result['rows']}")
    print(f"  Time: {result['execution_time_ms']}ms")
