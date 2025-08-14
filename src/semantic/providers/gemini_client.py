#!/usr/bin/env python3
"""
Gemini client for contextual chunking.
"""

import json
import logging
import os
import time
import random
import re
import google.generativeai as genai
from typing import List, Dict, Any
from ...prompts.contextual_chunking import ContextualChunking
from .base_client import BaseLLMClient


class GeminiClient(BaseLLMClient):
    """Client for interacting with Google Gemini API for contextual chunking."""
    
    def __init__(self, api_key: str = None, model_name: str = None):
        """
        Initialize the Gemini client.
        
        Args:
            api_key: Gemini API key (defaults to GEMINI_API_KEY environment variable)
            model_name: Gemini model name (defaults to gemini-1.5-flash)
        """
        api_key = api_key or os.getenv('GEMINI_API_KEY')
        model_name = model_name or 'gemini-2.5-flash'
        super().__init__(api_key, model_name)
        
        if not self.api_key:
            raise ValueError("Gemini API key not found. Set GEMINI_API_KEY environment variable.")
        
        # Configure the Gemini client
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(self.model_name)
        
    def _make_api_request_with_retry(self, prompt: str, max_retries: int = 3) -> str:
        """
        Make API request with exponential backoff retry for quota limits.
        
        Args:
            prompt: The prompt to send to Gemini
            max_retries: Maximum number of retry attempts
            
        Returns:
            Response text from Gemini
        """
        for attempt in range(max_retries + 1):
            try:
                # Add delay with exponential backoff for retries
                if attempt > 0:
                    delay = (2 ** attempt) + random.uniform(1.0, 3.0)  # Exponential backoff (same as OpenAI)
                    logging.debug(f"⏳ Waiting {delay:.1f}s before retry attempt {attempt}...")
                    time.sleep(delay)
                else:
                    # Add random delay before first request to prevent rate limiting
                    delay = random.uniform(0.5, 1.5)  # 0.5-1.5 second delay (same as OpenAI)
                    time.sleep(delay)
                
                response = self.model.generate_content(prompt)
                # Defensively handle possible None or empty response text
                resp_text = getattr(response, 'text', None)
                if not resp_text or not isinstance(resp_text, str) or not resp_text.strip():
                    raise Exception("Empty response from Gemini")
                return resp_text.strip()
                
            except Exception as e:
                error_str = str(e)
                
                # Check for quota exceeded errors (429)
                if "429" in error_str and "quota" in error_str.lower():
                    if attempt < max_retries:
                        logging.warning(f"⚠️ API quota exceeded, retrying (attempt {attempt + 1}/{max_retries + 1})...")
                        continue
                    else:
                        logging.error("❌ API quota exceeded and max retries reached.")
                        raise Exception("API quota exceeded - please wait before retrying")
                
                # Check for other rate limit errors
                elif "rate limit" in error_str.lower():
                    if attempt < max_retries:
                        logging.warning(f"⚠️ Rate limited, retrying (attempt {attempt + 1}/{max_retries + 1})...")
                        continue
                    else:
                        logging.error("❌ Rate limited and max retries reached.")
                        raise
                
                # For other errors, don't retry
                else:
                    raise
        
        raise Exception("Max retries reached")

    def _sanitize_json_response(self, response_text: str) -> str:
        """
        Sanitize JSON response by removing/replacing invalid control characters.
        
        Args:
            response_text: Raw JSON response text
            
        Returns:
            Sanitized JSON response text
        """
        # Remove or replace invalid control characters (except \n, \t, \r which are valid in JSON strings)
        # Control characters are 0x00-0x1F except for \t (0x09), \n (0x0A), \r (0x0D)
        sanitized = ""
        for char in response_text:
            char_code = ord(char)
            if char_code < 32:  # Control character
                if char_code in [9, 10, 13]:  # \t, \n, \r are allowed
                    sanitized += char
                else:
                    # Replace other control characters with space or remove them
                    sanitized += " " if char_code != 0 else ""
            else:
                sanitized += char
        
        # Clean up multiple spaces
        sanitized = re.sub(r'\s+', ' ', sanitized)
        
        return sanitized.strip()

    def _fix_common_json_issues(self, response_text: str) -> str:
        """
        Fix common JSON formatting issues in Gemini responses.
        
        Args:
            response_text: Raw JSON response text
            
        Returns:
            Fixed JSON response text
        """
        fixed = response_text
        
        # Fix trailing commas before closing brackets/braces
        fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
        
        # Fix missing commas between objects in arrays
        fixed = re.sub(r'}\s*{', r'},{', fixed)
        
        # Fix single quotes to double quotes (JSON requires double quotes)
        # Be careful not to replace apostrophes in content
        fixed = re.sub(r"'([^']*)':", r'"\1":', fixed)
        
        # Fix unescaped quotes in string values by finding and escaping them
        # This handles cases where quotes appear in the middle of string values
        try:
            # Try to identify the problematic section and fix it
            lines = fixed.split('\n')
            for i, line in enumerate(lines):
                # Look for lines with unescaped quotes in string values
                if '"' in line and line.count('"') % 2 != 0:
                    # Try to fix unescaped quotes in the middle of strings
                    # Find patterns like: "text with "quote" in middle"
                    line = re.sub(r'(?<!\\)"(?=[^"]*"[^"]*$)', r'\\"', line)
                    lines[i] = line
            fixed = '\n'.join(lines)
        except Exception as e:
            logging.debug(f"Warning: Could not fix unescaped quotes: {e}")
        
        # Try to truncate at the last valid JSON structure if parsing fails
        # Find the last complete object or array
        try:
            # Find the last complete closing brace or bracket
            last_closing = max(
                fixed.rfind('}'),
                fixed.rfind(']')
            )
            if last_closing > 0:
                # Check if this creates valid JSON by trying different truncation points
                for end_pos in range(last_closing + 1, len(fixed) + 1):
                    test_json = fixed[:end_pos].rstrip()
                    try:
                        json.loads(test_json)
                        fixed = test_json
                        break
                    except:
                        continue
        except Exception as e:
            logging.debug(f"Warning: Could not truncate to valid JSON: {e}")
        
        return fixed

    def process_document_for_chunking(self, document_content: str) -> List[Dict[str, Any]]:
        """
        Process a markdown document for contextual chunking using Gemini.
        
        Args:
            document_content: The markdown content to process
            
        Returns:
            List of chunks with content and keywords
        """
        try:
            # Create contextual chunking prompt
            chunking = ContextualChunking("", document_content)
            prompt = chunking.get_full_prompt()
            
            # Generate response from Gemini with retry logic
            logging.info("🧠 Processing document with Gemini for contextual chunking...")
            response_text = self._make_api_request_with_retry(prompt)
            
            # Clean up the response (remove any markdown code blocks if present)
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            if response_text.startswith('```'):
                response_text = response_text[3:]
            
            response_text = response_text.strip()
            
            # Sanitize JSON response by removing/replacing invalid control characters
            response_text = self._sanitize_json_response(response_text)
            
            # Parse JSON
            chunks = json.loads(response_text)
            
            if not isinstance(chunks, list):
                raise ValueError("Response is not a list of chunks")
                
            logging.info(f"✅ Successfully processed document into {len(chunks)} chunks")
            return chunks
            
        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing failed: {e}")
            error_pos = getattr(e, 'pos', 0)
            
            # Try to fix common JSON issues and retry parsing
            try:
                fixed_response = self._fix_common_json_issues(response_text)
                chunks = json.loads(fixed_response)
                if isinstance(chunks, list):
                    logging.warning("✅ JSON recovered after fixing")
                    return chunks
                    
            except json.JSONDecodeError:
                # Last resort: try progressive truncation to find valid JSON
                try:
                    for truncate_pos in range(error_pos, max(0, error_pos - 1000), -10):
                        try:
                            truncated = response_text[:truncate_pos].rstrip()
                            # Try to close any open structures
                            if truncated.count('[') > truncated.count(']'):
                                truncated += ']' * (truncated.count('[') - truncated.count(']'))
                            if truncated.count('{') > truncated.count('}'):
                                truncated += '}' * (truncated.count('{') - truncated.count('}'))
                            
                            chunks = json.loads(truncated)
                            if isinstance(chunks, list) and len(chunks) > 0:
                                logging.warning(f"⚠️ JSON truncated, recovered {len(chunks)} chunks")
                                return chunks
                        except:
                            continue
                except:
                    pass
            except:
                pass
            
            raise
        except Exception as e:
            logging.error(f"Error processing document with Gemini: {e}")
            raise
    
