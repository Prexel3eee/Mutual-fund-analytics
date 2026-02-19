"""
Schema Introspection Module
Reads PostgreSQL database schema dynamically and produces a compressed
text representation for LLM context injection.
"""

import psycopg2
from typing import Optional


def get_db_connection(conn_params: dict):
    """Create a database connection."""
    return psycopg2.connect(**conn_params)


def get_schema_context(conn_params: dict, include_samples: bool = True) -> str:
    """
    Read the full database schema and produce a text representation
    suitable for injecting into an LLM system prompt.
    
    Returns a formatted string with:
    - Table names, columns, types, constraints
    - Foreign key relationships
    - Row counts
    - Sample data (optional)
    """
    conn = get_db_connection(conn_params)
    cur = conn.cursor()
    
    lines = []
    lines.append("=== POSTGRESQL DATABASE SCHEMA ===")
    lines.append(f"Database: {conn_params['dbname']}\n")
    
    # ── Get all tables ──────────────────────────────────────────
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]
    
    lines.append(f"Tables ({len(tables)}): {', '.join(tables)}\n")
    
    for table in tables:
        # Row count
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        row_count = cur.fetchone()[0]
        
        lines.append(f"── TABLE: {table} ({row_count:,} rows) ──")
        
        # ── Columns ────────────────────────────────────────────
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default,
                   character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        
        columns = cur.fetchall()
        for col in columns:
            col_name, dtype, nullable, default, max_len, num_prec, num_scale = col
            type_str = dtype
            if max_len:
                type_str += f"({max_len})"
            elif num_prec and num_scale:
                type_str += f"({num_prec},{num_scale})"
            
            extras = []
            if nullable == 'NO':
                extras.append("NOT NULL")
            if default:
                extras.append(f"DEFAULT {default}")
            
            extra_str = f"  [{', '.join(extras)}]" if extras else ""
            lines.append(f"  {col_name}: {type_str}{extra_str}")
        
        # ── Primary key ───────────────────────────────────────
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = 'public' 
              AND tc.table_name = %s 
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """, (table,))
        pk_cols = [row[0] for row in cur.fetchall()]
        if pk_cols:
            lines.append(f"  🔑 PK: ({', '.join(pk_cols)})")
        
        # ── Foreign keys ──────────────────────────────────────
        cur.execute("""
            SELECT
                kcu.column_name,
                ccu.table_name AS ref_table,
                ccu.column_name AS ref_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_name = ccu.constraint_name
            WHERE tc.table_schema = 'public'
              AND tc.table_name = %s
              AND tc.constraint_type = 'FOREIGN KEY'
        """, (table,))
        fks = cur.fetchall()
        for fk in fks:
            lines.append(f"  🔗 FK: {fk[0]} → {fk[1]}.{fk[2]}")
        
        # ── Unique constraints ────────────────────────────────
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = 'public'
              AND tc.table_name = %s
              AND tc.constraint_type = 'UNIQUE'
            ORDER BY tc.constraint_name, kcu.ordinal_position
        """, (table,))
        uniq_cols = [row[0] for row in cur.fetchall()]
        if uniq_cols:
            lines.append(f"  🔒 UNIQUE: ({', '.join(uniq_cols)})")
        
        # ── Sample data ───────────────────────────────────────
        if include_samples and row_count > 0:
            col_names = [c[0] for c in columns]
            cur.execute(f'SELECT * FROM "{table}" LIMIT 3')
            samples = cur.fetchall()
            if samples:
                lines.append(f"  📋 Sample ({min(3, row_count)} rows):")
                for sample in samples:
                    row_dict = dict(zip(col_names, sample))
                    # Truncate long values
                    truncated = {k: (str(v)[:60] + '...' if len(str(v)) > 60 else str(v)) 
                                 for k, v in row_dict.items() if v is not None}
                    lines.append(f"    {truncated}")
        
        lines.append("")  # blank line between tables
    
    # ── Relationship summary ──────────────────────────────────
    cur.execute("""
        SELECT
            tc.table_name AS from_table,
            kcu.column_name AS from_col,
            ccu.table_name AS to_table,
            ccu.column_name AS to_col
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
        ORDER BY tc.table_name
    """)
    relationships = cur.fetchall()
    
    if relationships:
        lines.append("=== FOREIGN KEY RELATIONSHIPS ===")
        for rel in relationships:
            lines.append(f"  {rel[0]}.{rel[1]} → {rel[2]}.{rel[3]}")
        lines.append("")
    
    # ── Common join patterns ──────────────────────────────────
    lines.append("=== COMMON JOIN PATTERNS ===")
    lines.append("  fund_master JOIN amc_master ON fund_master.amc_id = amc_master.amc_id")
    lines.append("  portfolio_holdings JOIN fund_master ON portfolio_holdings.fund_id = fund_master.fund_id")
    lines.append("  portfolio_holdings JOIN security_master ON portfolio_holdings.security_id = security_master.security_id")
    lines.append("  fund_sector_exposure JOIN fund_master ON fund_sector_exposure.fund_id = fund_master.fund_id")
    lines.append("  fund_monthly_metrics JOIN fund_master ON fund_monthly_metrics.fund_id = fund_master.fund_id")
    lines.append("  fund_manager_mapping JOIN fund_master ON fund_manager_mapping.fund_id = fund_master.fund_id")
    lines.append("  fund_manager_mapping JOIN fund_manager_master ON fund_manager_mapping.manager_id = fund_manager_master.manager_id")
    lines.append("")
    
    cur.close()
    conn.close()
    
    return "\n".join(lines)


def get_table_summary(conn_params: dict) -> str:
    """Get a quick summary of all tables and their row counts."""
    conn = get_db_connection(conn_params)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]
    
    lines = [f"{'Table':<40} {'Rows':>10}",  "─" * 52]
    total = 0
    for table in tables:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = cur.fetchone()[0]
        total += count
        lines.append(f"{table:<40} {count:>10,}")
    
    lines.append("─" * 52)
    lines.append(f"{'TOTAL':<40} {total:>10,}")
    
    cur.close()
    conn.close()
    
    return "\n".join(lines)


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
    
    print(get_table_summary(params))
    print("\n\n")
    schema = get_schema_context(params)
    print(f"Schema context length: {len(schema):,} characters")
    print(schema[:2000])
