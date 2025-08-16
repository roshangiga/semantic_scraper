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
from google import genai
from pydantic import BaseModel
from typing import List, Dict, Any
from ...prompts.contextual_chunking import ContextualChunking
from .base_client import BaseLLMClient

# Pydantic schema for structured output
class Chunk(BaseModel):
    content: str
    keywords: List[str]

try:
    from ...console import print_error, print_success, print_warning, print_info, print_processing
except ImportError:
    # Fallback to regular logging if rich not available
    def print_error(msg): logging.error(f"❌ {msg}")
    def print_success(msg): logging.info(f"✅ {msg}")
    def print_warning(msg): logging.warning(f"⚠️ {msg}")
    def print_info(msg): logging.info(f"ℹ️ {msg}")
    def print_processing(msg): logging.info(f"🔄 {msg}")


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
        
        # Configure the Gemini client with structured output support
        self.client = genai.Client(api_key=self.api_key)
        
    def _make_structured_request_with_retry(self, prompt: str, max_retries: int = 3) -> List[Chunk]:
        """
        Make structured API request with exponential backoff retry for quota limits.
        
        Args:
            prompt: The prompt to send to Gemini
            max_retries: Maximum number of retry attempts
            
        Returns:
            List of Chunk objects with structured output
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
                
                # Use structured output with proper schema
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config={
                        "response_mime_type": "application/json",
                        "response_schema": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "content": {"type": "string"},
                                    "keywords": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    }
                                },
                                "required": ["content", "keywords"]
                            }
                        },
                        "temperature": 0.1,  # Low temperature for consistent output
                        "max_output_tokens": 8192,
                    }
                )
                
                # Parse the JSON response
                if hasattr(response, 'text') and response.text:
                    try:
                        chunks_data = json.loads(response.text)
                        chunks = []
                        for chunk_data in chunks_data:
                            chunks.append(Chunk(
                                content=chunk_data.get('content', ''),
                                keywords=chunk_data.get('keywords', [])
                            ))
                        return chunks
                    except json.JSONDecodeError as e:
                        raise Exception(f"Failed to parse JSON response: {e}")
                else:
                    raise Exception("No response text returned from Gemini")
                
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


    def process_document_for_chunking(self, document_content: str) -> List[Dict[str, Any]]:
        """
        Process a markdown document for contextual chunking using Gemini with structured output.
        
        Args:
            document_content: The markdown content to process
            
        Returns:
            List of chunks with content and keywords
        """
        try:
            # Create contextual chunking prompt
            chunking = ContextualChunking("", document_content)
            prompt = chunking.get_full_prompt()
            
            # Generate structured response from Gemini with retry logic
            logging.info("🧠 Processing document with Gemini for contextual chunking...")
            chunks = self._make_structured_request_with_retry(prompt)
            
            # Convert Pydantic objects to dictionaries for compatibility
            chunk_dicts = []
            for chunk in chunks:
                chunk_dicts.append({
                    "content": chunk.content,
                    "keywords": chunk.keywords
                })
                
            print_success(f"Successfully processed document into {len(chunk_dicts)} chunks")
            return chunk_dicts
            
        except Exception as e:
            logging.error(f"Error processing document with Gemini: {e}")
            raise
    
