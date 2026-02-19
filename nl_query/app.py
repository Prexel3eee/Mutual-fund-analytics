"""
Mutual Fund Database Assistant — Interactive CLI
Talk to your PostgreSQL database in natural language.

Usage:
    cd nl_query
    python app.py

Supports dual AI models (switch with /model command):
  1. MiniMax M2     — fast, general purpose
  2. GLM-4.7        — reasoning/thinking model
"""

import os
import sys
from pathlib import Path

# Ensure the nl_query directory is in the path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

# Load .env from the nl_query directory
load_dotenv(Path(__file__).parent / ".env")

from schema_introspect import get_schema_context, get_table_summary
from nl_engine import NLEngine
from db_executor import SafeExecutor
from formatter import (
    display_results, display_sql, display_welcome,
    display_help, export_csv, console,
)


def get_db_params() -> dict:
    """Get database connection parameters from environment."""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME", "mutual_fund_db"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "vivek"),
    }


def get_env_config() -> dict:
    """Get all environment variables as a dict."""
    keys = [
        "AI_PROVIDER", "NVIDIA_NIM_API_KEY", "NVIDIA_NIM_API_URL",
        "NVIDIA_NIM_MODEL", "NVIDIA_NIM_API_KEY_2", "NVIDIA_NIM_MODEL_2",
        "DEFAULT_MODEL",
    ]
    return {k: os.getenv(k, "") for k in keys}


def main():
    """Main interactive loop."""
    db_params = get_db_params()
    env_config = get_env_config()
    
    # ── Initialize components ─────────────────────────────────
    console.print("\n[dim]Connecting to database...[/dim]")
    
    try:
        db_summary = get_table_summary(db_params)
    except Exception as e:
        console.print(f"[red]❌ Cannot connect to database: {e}[/red]")
        console.print("[dim]Check your .env file: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD[/dim]")
        sys.exit(1)
    
    console.print("[dim]Loading schema context...[/dim]")
    schema_context = get_schema_context(db_params, include_samples=True)
    console.print(f"[dim]Schema loaded: {len(schema_context):,} chars[/dim]")
    
    # Initialize AI engine
    engine = NLEngine(env_config, schema_context)
    
    # Initialize safe executor
    executor = SafeExecutor(db_params)
    
    # Last result for export
    last_result = None
    
    # ── Welcome ───────────────────────────────────────────────
    display_welcome(db_summary, engine.get_active_model_info())
    
    # ── Interactive loop ──────────────────────────────────────
    while True:
        try:
            console.print()
            user_input = input("📝 You: ").strip()
        except (KeyboardInterrupt, EOFError):
            console.print("\n[dim]Goodbye! 👋[/dim]")
            break
        
        if not user_input:
            continue
        
        # ── Handle commands ───────────────────────────────────
        if user_input.startswith("/"):
            cmd = user_input.lower().split()
            command = cmd[0]
            
            if command in ("/quit", "/exit", "/q"):
                console.print("[dim]Goodbye! 👋[/dim]")
                break
            
            elif command == "/help":
                display_help()
                continue
            
            elif command == "/schema":
                console.print(f"\n[cyan]{db_summary}[/cyan]\n")
                continue
            
            elif command == "/models":
                console.print(f"\n[yellow]Available Models:[/yellow]")
                console.print(engine.list_models())
                console.print()
                continue
            
            elif command == "/model":
                if len(cmd) < 2:
                    console.print("[yellow]Usage: /model 1 or /model 2[/yellow]")
                else:
                    try:
                        model_id = int(cmd[1])
                        msg = engine.switch_model(model_id)
                        console.print(f"[green]{msg}[/green]")
                    except ValueError:
                        console.print("[red]Please specify model number: /model 1 or /model 2[/red]")
                continue
            
            elif command == "/history":
                if not engine.conversation_history:
                    console.print("[dim]No conversation history yet.[/dim]")
                else:
                    for i, turn in enumerate(engine.conversation_history):
                        role = "👤 You" if turn["role"] == "user" else "🤖 AI"
                        content = turn["content"][:150] + "..." if len(turn["content"]) > 150 else turn["content"]
                        console.print(f"[dim]{i+1}.[/dim] {role}: {content}")
                continue
            
            elif command == "/export":
                if last_result and last_result["success"] and last_result["rows"]:
                    filepath = export_csv(last_result)
                    console.print(f"[green]✅ Exported to: {filepath}[/green]")
                else:
                    console.print("[yellow]No results to export. Run a query first.[/yellow]")
                continue
            
            elif command == "/clear":
                engine.clear_history()
                console.print("[green]✅ Conversation history cleared.[/green]")
                continue
            
            elif command == "/test":
                console.print("[dim]Testing API connection...[/dim]")
                result = engine.test_connection()
                console.print(result)
                continue
            
            else:
                console.print(f"[yellow]Unknown command: {command}. Type /help for available commands.[/yellow]")
                continue
        
        # ── Process natural language query ────────────────────
        console.print(f"\n[dim]🤖 {engine.get_active_model_info()}[/dim]")
        
        # Get LLM response
        response = engine.ask(user_input)
        
        # Extract SQL from response
        sql = engine.extract_sql(response)
        
        if not sql:
            # No SQL found — the LLM gave a text-only answer
            continue
        
        # Display the extracted SQL
        display_sql(sql)
        
        # Ask for confirmation
        try:
            confirm = input("  ▶ Execute this query? (Y/n/edit): ").strip().lower()
        except (KeyboardInterrupt, EOFError):
            console.print("\n[dim]Query cancelled.[/dim]")
            continue
        
        if confirm in ("n", "no"):
            console.print("[dim]Query skipped.[/dim]")
            continue
        
        if confirm in ("e", "edit"):
            console.print("[dim]Enter your modified SQL (end with an empty line):[/dim]")
            lines = []
            while True:
                try:
                    line = input("  ")
                    if not line:
                        break
                    lines.append(line)
                except (KeyboardInterrupt, EOFError):
                    break
            if lines:
                sql = "\n".join(lines)
                display_sql(sql)
            else:
                console.print("[dim]No changes. Using original SQL.[/dim]")
        
        # Execute the query
        console.print("[dim]Executing...[/dim]")
        result = executor.execute(sql)
        last_result = result
        
        # Display results
        display_results(result, title=user_input[:80])


if __name__ == "__main__":
    main()
