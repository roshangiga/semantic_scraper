#!/usr/bin/env python3
"""
Cost tracking system for semantic processing using tiktoken.
"""

import os
import json
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import tiktoken

# Import for Gemini token counting
try:
    import google.generativeai as genai
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False


class CostTracker:
    """Track and estimate costs for LLM API usage."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize cost tracker with pricing configuration."""
        self.config = self._load_config(config_path)
        self.cost_config = self.config.get('cost_tracking', {})
        self.pricing = self.cost_config.get('pricing', {})
        self.enabled = self.cost_config.get('enabled', False)
        self.output_file = self.cost_config.get('output_file', 'cost_log.txt')
        
        # Initialize tiktoken encoder (using GPT-4 encoding as standard)
        try:
            self.encoder = tiktoken.get_encoding("cl100k_base")
        except Exception as e:
            print(f"Warning: Could not initialize tiktoken encoder: {e}")
            self.encoder = None
            
        # Initialize Gemini client if available
        self.gemini_configured = False
        if GENAI_AVAILABLE:
            try:
                api_key = os.getenv('GEMINI_API_KEY')
                if api_key:
                    genai.configure(api_key=api_key)
                    self.gemini_configured = True
            except Exception as e:
                print(f"Warning: Could not configure Gemini: {e}")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Could not load config for cost tracking: {e}")
            return {}
    
    def count_tokens(self, text: str, provider: str = "openai", model: str = "") -> int:
        """Count tokens in text using provider-specific methods."""
        if not text:
            return 0
            
        # Use Gemini's official token counting for Gemini models
        if provider.lower() == "gemini" and self.gemini_configured:
            try:
                # Map our model names to Gemini API model names
                model_mapping = {
                    "gemini-2.5-flash": "gemini-2.0-flash-exp", 
                    "gemini-1.5-pro": "gemini-1.5-pro",
                    "gemini-2.0-flash": "gemini-2.0-flash-exp"
                }
                api_model = model_mapping.get(model, "gemini-2.0-flash-exp")
                
                # Use the newer API
                model = genai.GenerativeModel(api_model)
                response = model.count_tokens(text)
                return response.total_tokens
            except Exception as e:
                print(f"Warning: Gemini token counting failed: {e}, using fallback")
        
        # Use tiktoken for OpenAI/Azure models
        if self.encoder:
            try:
                return len(self.encoder.encode(text))
            except Exception as e:
                print(f"Warning: Could not count tokens with tiktoken: {e}")
        
        # Fallback estimation: ~4 characters per token
        return len(text) // 4
    
    def get_pricing(self, provider: str, model: str, tier: str = "paid_tier") -> Dict[str, float]:
        """Get pricing for a specific provider/model/tier."""
        provider_pricing = self.pricing.get(provider, {})
        model_pricing = provider_pricing.get(model, {})
        
        if tier in model_pricing:
            return model_pricing[tier]
        elif "paid_tier" in model_pricing:
            return model_pricing["paid_tier"]
        else:
            # Default fallback pricing (per 1M tokens)
            return {"input": 1.0, "output": 3.0}
    
    def calculate_cost(self, provider: str, model: str, input_tokens: int, output_tokens: int, tier: str = "paid_tier") -> Dict[str, Any]:
        """Calculate cost for token usage."""
        if not self.enabled:
            return {"total_cost": 0.0, "input_cost": 0.0, "output_cost": 0.0}
        
        pricing = self.get_pricing(provider, model, tier)
        
        # Calculate costs (pricing is per 1M tokens)
        input_cost = (input_tokens / 1_000_000) * pricing.get("input", 0.0)
        output_cost = (output_tokens / 1_000_000) * pricing.get("output", 0.0)
        total_cost = input_cost + output_cost
        
        return {
            "total_cost": total_cost,
            "input_cost": input_cost,
            "output_cost": output_cost,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "pricing_tier": tier
        }
    
    def log_usage(self, provider: str, model: str, input_text: str, output_text: str, 
                  source_file: str = "", tier: str = "paid_tier") -> Dict[str, Any]:
        """Log API usage and calculate costs."""
        if not self.enabled:
            return {"total_cost": 0.0}
        
        # Count tokens using provider-specific methods
        input_tokens = self.count_tokens(input_text, provider, model)
        output_tokens = self.count_tokens(output_text, provider, model)
        
        # Calculate costs
        cost_info = self.calculate_cost(provider, model, input_tokens, output_tokens, tier)
        
        # Create log entry
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "provider": provider,
            "model": model,
            "tier": tier,
            "source_file": source_file,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "input_cost_usd": cost_info["input_cost"],
            "output_cost_usd": cost_info["output_cost"],
            "total_cost_usd": cost_info["total_cost"]
        }
        
        # Write to log file
        try:
            with open(self.output_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            print(f"Warning: Could not write to cost log: {e}")
        
        return cost_info
    
    def get_total_costs(self) -> Dict[str, Any]:
        """Get total costs from log file."""
        if not os.path.exists(self.output_file):
            return {"total_cost": 0.0, "total_entries": 0, "by_model": {}}
        
        total_cost = 0.0
        total_entries = 0
        by_model = {}
        by_provider = {}
        
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        entry = json.loads(line.strip())
                        cost = entry.get('total_cost_usd', 0.0)
                        total_cost += cost
                        total_entries += 1
                        
                        # Track by model
                        model = entry.get('model', 'unknown')
                        provider = entry.get('provider', 'unknown')
                        model_key = f"{provider}/{model}"
                        
                        if model_key not in by_model:
                            by_model[model_key] = {
                                'cost': 0.0, 'tokens_in': 0, 'tokens_out': 0, 'calls': 0
                            }
                        
                        by_model[model_key]['cost'] += cost
                        by_model[model_key]['tokens_in'] += entry.get('input_tokens', 0)
                        by_model[model_key]['tokens_out'] += entry.get('output_tokens', 0)
                        by_model[model_key]['calls'] += 1
                        
                        # Track by provider
                        if provider not in by_provider:
                            by_provider[provider] = {'cost': 0.0, 'calls': 0}
                        by_provider[provider]['cost'] += cost
                        by_provider[provider]['calls'] += 1
                        
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"Warning: Could not read cost log: {e}")
        
        return {
            "total_cost_usd": total_cost,
            "total_entries": total_entries,
            "by_model": by_model,
            "by_provider": by_provider
        }
    
    def print_cost_summary(self):
        """Print a formatted cost summary."""
        if not self.enabled:
            print("Cost tracking is disabled")
            return
        
        costs = self.get_total_costs()
        
        print("\nCost Summary")
        print("=" * 50)
        print(f"Total Cost: ${costs.get('total_cost_usd', 0.0):.4f} USD")
        print(f"Total API Calls: {costs.get('total_entries', 0)}")
        
        if costs.get('by_provider'):
            print(f"\nBy Provider:")
            for provider, data in costs['by_provider'].items():
                print(f"  {provider}: ${data['cost']:.4f} ({data['calls']} calls)")
        
        if costs.get('by_model'):
            print(f"\nBy Model:")
            for model, data in costs['by_model'].items():
                tokens_total = data['tokens_in'] + data['tokens_out']
                avg_cost = data['cost'] / data['calls'] if data['calls'] > 0 else 0
                print(f"  {model}:")
                print(f"    Cost: ${data['cost']:.4f}")
                print(f"    Calls: {data['calls']}")
                print(f"    Tokens: {tokens_total:,} ({data['tokens_in']:,} in, {data['tokens_out']:,} out)")
                print(f"    Avg per call: ${avg_cost:.4f}")
        
        print("=" * 50)


def estimate_cost_for_text(text: str, provider: str = "gemini", model: str = "gemini-2.5-flash", tier: str = "paid_tier") -> Dict[str, Any]:
    """Quick cost estimation for a text without logging."""
    tracker = CostTracker()
    input_tokens = tracker.count_tokens(text, provider, model)
    # Assume output is ~20% of input for estimation
    output_tokens = int(input_tokens * 0.2)
    
    return tracker.calculate_cost(provider, model, input_tokens, output_tokens, tier)


if __name__ == "__main__":
    # Test/demo the cost tracker
    tracker = CostTracker()
    tracker.print_cost_summary()