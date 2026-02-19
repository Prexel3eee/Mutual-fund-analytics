"""
Rich Output Formatter
Formats query results into beautiful terminal tables, handles
number formatting, currency symbols, and CSV export.
"""

import csv
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

console = Console()


def format_value(val, col_name: str = "") -> str:
    """Format a single value for display."""
    if val is None:
        return "—"
    
    col_lower = col_name.lower()
    
    # Currency / monetary values
    if any(kw in col_lower for kw in ['aum', 'nav', 'market_value', 'value']):
        if isinstance(val, (int, float, Decimal)):
            if 'crore' in col_lower or 'aum' in col_lower:
                return f"₹{float(val):,.2f} Cr"
            elif 'lakh' in col_lower:
                return f"₹{float(val):,.2f} L"
            else:
                return f"₹{float(val):,.2f}"
    
    # Percentages
    if any(kw in col_lower for kw in ['pct', 'percent', 'ratio', 'return']):
        if isinstance(val, (int, float, Decimal)):
            return f"{float(val):.2f}%"
    
    # Numbers
    if isinstance(val, (int,)):
        return f"{val:,}"
    
    if isinstance(val, (float, Decimal)):
        fval = float(val)
        if fval == int(fval) and abs(fval) < 1e15:
            return f"{int(fval):,}"
        return f"{fval:,.2f}"
    
    # Dates
    if isinstance(val, (date, datetime)):
        return val.strftime("%Y-%m-%d")
    
    return str(val)


def display_results(result: dict, title: str = "Query Results"):
    """
    Display query results in a rich formatted table.
    
    Args:
        result: dict from SafeExecutor.execute()
        title: Optional title for the table
    """
    if not result["success"]:
        console.print(Panel(
            f"[red]{result['error']}[/red]",
            title="Error",
            border_style="red",
        ))
        return
    
    columns = result["columns"]
    rows = result["rows"]
    
    if not rows:
        console.print(Panel(
            "[yellow]No results found.[/yellow]",
            title="Empty Result",
            border_style="yellow",
        ))
        return
    
    # Build rich table
    table = Table(
        title=title,
        box=box.ROUNDED,
        show_lines=False,
        header_style="bold cyan",
        title_style="bold white",
        row_styles=["", "dim"],  # Alternating row colors
        padding=(0, 1),
    )
    
    # Add columns
    for col in columns:
        # Right-align numeric columns
        justify = "right" if any(kw in col.lower() for kw in [
            'count', 'sum', 'avg', 'total', 'pct', 'aum', 'nav', 'value',
            'quantity', 'id', 'rows', 'num', 'amount', 'ratio', 'return',
        ]) else "left"
        
        table.add_column(col, justify=justify, no_wrap=False, max_width=50)
    
    # Add rows
    for row in rows:
        formatted = []
        for i, val in enumerate(row):
            col_name = columns[i] if i < len(columns) else ""
            fv = format_value(val, col_name)
            
            # Color-code percentages
            if any(kw in col_name.lower() for kw in ['pct', 'return', 'delta']):
                if isinstance(val, (int, float, Decimal)):
                    fval = float(val)
                    if fval > 0:
                        fv = f"[green]+{fv}[/green]" if 'delta' in col_name.lower() else f"[green]{fv}[/green]"
                    elif fval < 0:
                        fv = f"[red]{fv}[/red]"
            
            formatted.append(fv)
        
        table.add_row(*formatted)
    
    console.print()
    console.print(table)
    
    # Show metadata
    meta_parts = [f"[dim]{result['row_count']} rows[/dim]"]
    meta_parts.append(f"[dim]{result['execution_time_ms']}ms[/dim]")
    
    if result["truncated"]:
        meta_parts.append("[yellow]⚠ Results truncated (500 row limit)[/yellow]")
    
    console.print("  " + "  •  ".join(meta_parts))
    console.print()


def export_csv(result: dict, filepath: Optional[str] = None) -> str:
    """
    Export query results to a CSV file.
    
    Returns the filepath of the exported CSV.
    """
    if not result["success"] or not result["rows"]:
        return "No data to export."
    
    # Default filepath
    if not filepath:
        exports_dir = Path(__file__).parent / "exports"
        exports_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = str(exports_dir / f"query_result_{timestamp}.csv")
    
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(result["columns"])
        for row in result["rows"]:
            writer.writerow([str(v) if v is not None else "" for v in row])
    
    return filepath


def display_sql(sql: str):
    """Display the generated SQL in a formatted panel."""
    console.print()
    console.print(Panel(
        f"[green]{sql}[/green]",
        title="[bold]Generated SQL[/bold]",
        border_style="green",
        padding=(1, 2),
    ))


def display_welcome(db_summary: str, active_model: str):
    """Display a welcome banner."""
    welcome_text = Text()
    welcome_text.append("🔍 Mutual Fund Database Assistant\n", style="bold cyan")
    welcome_text.append("Ask questions about your database in plain English.\n\n", style="dim")
    welcome_text.append(f"🤖 {active_model}\n", style="yellow")
    welcome_text.append("─" * 50 + "\n", style="dim")
    welcome_text.append(db_summary, style="white")
    welcome_text.append("\n─" * 0, style="dim")
    welcome_text.append("\n\nType ", style="dim")
    welcome_text.append("/help", style="bold green")
    welcome_text.append(" for commands, or just type your question!", style="dim")
    
    console.print(Panel(
        welcome_text,
        border_style="cyan",
        padding=(1, 2),
    ))


def display_help():
    """Display help information."""
    help_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    help_table.add_column("Command", style="bold green", min_width=16)
    help_table.add_column("Description", style="white")
    
    commands = [
        ("/help", "Show this help message"),
        ("/schema", "Show database table summary"),
        ("/models", "List available AI models"),
        ("/model 1|2", "Switch between AI models"),
        ("/history", "Show conversation history"),
        ("/export", "Export last result to CSV"),
        ("/clear", "Clear conversation history"),
        ("/quit or /exit", "Exit the assistant"),
    ]
    
    for cmd, desc in commands:
        help_table.add_row(cmd, desc)
    
    console.print(Panel(
        help_table,
        title="[bold]Available Commands[/bold]",
        border_style="green",
    ))
