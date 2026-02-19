"""
Natural Language to SQL Engine
Uses NVIDIA NIM API (OpenAI-compatible) to translate natural language
questions into PostgreSQL queries. Supports dual-model switching.
"""

import re
import os
import sys
import time
from openai import OpenAI
from typing import Optional

_USE_COLOR = sys.stdout.isatty() and os.getenv("NO_COLOR") is None
_THINKING_COLOR = "\033[90m" if _USE_COLOR else ""
_RESET_COLOR = "\033[0m" if _USE_COLOR else ""


SYSTEM_PROMPT_TEMPLATE = """You are an expert PostgreSQL database analyst for a Mutual Fund Analytics database.
Your job is to translate natural language questions into accurate, efficient SQL queries.

IMPORTANT RULES:
1. ONLY output valid PostgreSQL SELECT queries. Never use INSERT, UPDATE, DELETE, DROP, or any DDL/DML.
2. Always wrap your SQL in a ```sql code block.
3. Use proper JOINs - never use implicit joins in WHERE clauses.
4. Use aliases for readability (e.g., fm for fund_master, am for amc_master).
5. Format numbers nicely: use ROUND() for decimals, TO_CHAR() for formatting when helpful.
6. Always add ORDER BY for meaningful results. Use LIMIT for large result sets.
7. If ambiguous, prefer the most useful interpretation and explain your reasoning.
8. After the SQL block, briefly explain what the query does in 1-2 sentences.
9. If you cannot answer with a SQL query, explain why and suggest what data might be needed.

DOMAIN KNOWLEDGE:
- AMC = Asset Management Company (e.g., SBI, HDFC, Axis, Kotak, Nippon, Motilal Oswal, Bajaj, Edelweiss)
- NAV = Net Asset Value (per unit price of a mutual fund)
- AUM = Assets Under Management (total value managed, in crores ₹)
- Holdings = Securities (stocks/bonds) held by a fund
- ISIN = International Securities Identification Number (unique stock ID)
- Sector exposure = % allocation of a fund across sectors like Banking, IT, Pharma
- pct_portfolio / pct_to_aum = percentage of total fund value in a holding
- market_value_lakhs = value in lakhs (1 lakh = ₹1,00,000)
- aum_crores = AUM in crores (1 crore = ₹1,00,00,000)
- report_date = monthly reporting date for holdings/metrics
- fund_master links to amc_master via amc_id
- portfolio_holdings links to fund_master via fund_id and security_master via security_id
- fund_sector_exposure, fund_monthly_metrics, fund_style_exposure_monthly link to fund_master via fund_id
- cap_bucket values: 'Large Cap', 'Mid Cap', 'Small Cap', 'Micro Cap'

DATABASE SCHEMA:
{schema}
"""


class NLEngine:
    """Natural Language to SQL translation engine with dual-model support."""
    
    def __init__(self, env_config: dict, schema_context: str = ""):
        """
        Initialize with environment configuration.
        
        env_config should contain:
        - NVIDIA_NIM_API_KEY, NVIDIA_NIM_API_URL, NVIDIA_NIM_MODEL (model 1)
        - NVIDIA_NIM_API_KEY_2, NVIDIA_NIM_MODEL_2 (model 2)
        - DEFAULT_MODEL (1 or 2)
        """
        self.config = env_config
        self.schema_context = schema_context
        
        # Model configurations
        self.models = {
            1: {
                "name": env_config.get("NVIDIA_NIM_MODEL", "minimaxai/minimax-m2"),
                "api_key": env_config.get("NVIDIA_NIM_API_KEY", ""),
                "api_url": env_config.get("NVIDIA_NIM_API_URL", "https://integrate.api.nvidia.com/v1"),
                "label": "MiniMax M2",
                "supports_thinking": False,
            },
            2: {
                "name": env_config.get("NVIDIA_NIM_MODEL_2", "z-ai/glm4.7"),
                "api_key": env_config.get("NVIDIA_NIM_API_KEY_2", ""),
                "api_url": env_config.get("NVIDIA_NIM_API_URL", "https://integrate.api.nvidia.com/v1"),
                "label": "GLM-4.7 (Reasoning)",
                "supports_thinking": True,
            },
        }
        
        # Active model
        default = int(env_config.get("DEFAULT_MODEL", "1"))
        self.active_model_id = default if default in self.models else 1
        
        # Conversation history (per session)
        self.conversation_history = []
        
        # Build clients lazily
        self._clients = {}
    
    def _get_client(self, model_id: int) -> OpenAI:
        """Get or create an OpenAI client for the given model."""
        if model_id not in self._clients:
            model_cfg = self.models[model_id]
            self._clients[model_id] = OpenAI(
                base_url=model_cfg["api_url"],
                api_key=model_cfg["api_key"],
            )
        return self._clients[model_id]
    
    def switch_model(self, model_id: int) -> str:
        """Switch the active model. Returns status message."""
        if model_id not in self.models:
            return f"Invalid model ID. Available: {list(self.models.keys())}"
        
        self.active_model_id = model_id
        m = self.models[model_id]
        return f"Switched to Model {model_id}: {m['label']} ({m['name']})"
    
    def get_active_model_info(self) -> str:
        """Get info about the currently active model."""
        m = self.models[self.active_model_id]
        return f"Model {self.active_model_id}: {m['label']} ({m['name']})"
    
    def list_models(self) -> str:
        """List all available models."""
        lines = []
        for mid, m in self.models.items():
            active = " ◀ ACTIVE" if mid == self.active_model_id else ""
            thinking = " [🧠 reasoning]" if m["supports_thinking"] else ""
            lines.append(f"  {mid}. {m['label']} ({m['name']}){thinking}{active}")
        return "\n".join(lines)
    
    def _build_system_prompt(self) -> str:
        """Build the system prompt with schema context."""
        return SYSTEM_PROMPT_TEMPLATE.format(schema=self.schema_context)
    
    def ask(self, question: str, stream: bool = True) -> str:
        """
        Send a natural language question to the LLM and get a response.
        Supports streaming output.
        
        Returns the full response text.
        """
        model_cfg = self.models[self.active_model_id]
        client = self._get_client(self.active_model_id)
        
        # Build messages
        messages = [{"role": "system", "content": self._build_system_prompt()}]
        
        # Add conversation history (last 6 turns for context)
        for turn in self.conversation_history[-6:]:
            messages.append(turn)
        
        # Add current question
        messages.append({"role": "user", "content": question})
        
        # Build API args
        api_kwargs = {
            "model": model_cfg["name"],
            "messages": messages,
            "temperature": 0.1,  # Low temperature for SQL accuracy
            "top_p": 0.95,
            "max_tokens": 4096,
            "stream": stream,
        }
        
        # GLM-4.7 supports thinking/reasoning
        if model_cfg["supports_thinking"]:
            api_kwargs["extra_body"] = {
                "chat_template_kwargs": {
                    "enable_thinking": True,
                    "clear_thinking": False,
                }
            }
        
        full_response = ""
        thinking_content = ""
        
        try:
            if stream:
                completion = client.chat.completions.create(**api_kwargs)
                
                in_thinking = False
                for chunk in completion:
                    if not getattr(chunk, "choices", None):
                        continue
                    if len(chunk.choices) == 0 or getattr(chunk.choices[0], "delta", None) is None:
                        continue
                    
                    delta = chunk.choices[0].delta
                    
                    # Handle reasoning/thinking content (GLM-4.7)
                    reasoning = getattr(delta, "reasoning_content", None)
                    if reasoning:
                        if not in_thinking:
                            print(f"\n{_THINKING_COLOR}💭 Thinking...", end="")
                            in_thinking = True
                        print(f"{_THINKING_COLOR}{reasoning}{_RESET_COLOR}", end="")
                        thinking_content += reasoning
                    
                    # Handle main content
                    content = getattr(delta, "content", None)
                    if content is not None:
                        if in_thinking:
                            print(f"{_RESET_COLOR}\n")  # End thinking block
                            in_thinking = False
                        print(content, end="")
                        full_response += content
                
                if in_thinking:
                    print(f"{_RESET_COLOR}")
                print()  # Final newline
            else:
                completion = client.chat.completions.create(**api_kwargs)
                full_response = completion.choices[0].message.content or ""
                print(full_response)
        
        except Exception as e:
            error_msg = f"❌ API Error: {e}"
            print(error_msg)
            return error_msg
        
        # Update conversation history
        self.conversation_history.append({"role": "user", "content": question})
        self.conversation_history.append({"role": "assistant", "content": full_response})
        
        return full_response
    
    def extract_sql(self, response: str) -> Optional[str]:
        """Extract SQL query from LLM response (from code blocks)."""
        # Try ```sql ... ``` first
        pattern = r'```sql\s*\n?(.*?)\n?\s*```'
        matches = re.findall(pattern, response, re.DOTALL | re.IGNORECASE)
        
        if matches:
            # Return the last SQL block (in case there are explanations with examples)
            return matches[-1].strip()
        
        # Try generic ``` ... ```
        pattern = r'```\s*\n?(.*?)\n?\s*```'
        matches = re.findall(pattern, response, re.DOTALL)
        
        if matches:
            for match in matches:
                cleaned = match.strip()
                upper = cleaned.upper()
                if upper.startswith('SELECT') or upper.startswith('WITH'):
                    return cleaned
        
        # Try to find standalone SQL
        lines = response.split('\n')
        sql_lines = []
        capturing = False
        for line in lines:
            stripped = line.strip().upper()
            if stripped.startswith('SELECT') or stripped.startswith('WITH'):
                capturing = True
            if capturing:
                sql_lines.append(line)
                if line.strip().endswith(';'):
                    break
        
        if sql_lines:
            return '\n'.join(sql_lines).strip().rstrip(';')
        
        return None
    
    def clear_history(self):
        """Clear conversation history."""
        self.conversation_history.clear()
    
    def test_connection(self) -> str:
        """Test API connectivity with a simple request."""
        model_cfg = self.models[self.active_model_id]
        client = self._get_client(self.active_model_id)
        
        try:
            start = time.perf_counter()
            completion = client.chat.completions.create(
                model=model_cfg["name"],
                messages=[{"role": "user", "content": "Say 'Hello, I am ready!' in exactly those words."}],
                max_tokens=20,
                stream=False,
            )
            elapsed = (time.perf_counter() - start) * 1000
            response = completion.choices[0].message.content
            return f"✅ {model_cfg['label']} responded in {elapsed:.0f}ms: {response}"
        except Exception as e:
            return f"❌ Connection failed: {e}"
