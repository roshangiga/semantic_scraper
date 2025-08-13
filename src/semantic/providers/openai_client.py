#!/usr/bin/env python3
"""
Standard OpenAI client for contextual chunking using regular OpenAI API.
"""

import json
import logging
import os
import time
import random
from typing import List, Dict, Any
from openai import OpenAI, AzureOpenAI
from ...prompts.contextual_chunking import ContextualChunking
from .base_client import BaseLLMClient


class OpenAIStandardClient(BaseLLMClient):
    """Client for interacting with standard OpenAI API for contextual chunking."""
    
    def __init__(self, api_key: str = None, model_name: str = None, azure_endpoint: str = None, azure_api_version: str = None, azure_deployment: str = None):
        """
        Initialize the OpenAI client (Standard or Azure).
        
        Args:
            api_key: OpenAI API key (defaults to OPENAI_API_KEY environment variable)
            model_name: OpenAI model name (e.g., 'gpt-4o', 'gpt-4o-mini', 'gpt-5-mini')
            azure_endpoint: Azure OpenAI endpoint URL (optional)
            azure_api_version: Azure API version (optional)
            azure_deployment: Azure deployment name (optional)
        """
        api_key = api_key or os.getenv('OPENAI_API_KEY')
        model_name = model_name or 'gpt-4o-mini'  # Default model
        
        super().__init__(api_key, model_name)
        
        if not self.api_key:
            raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
        
        # Store Azure settings
        self.azure_endpoint = azure_endpoint
        self.azure_api_version = azure_api_version
        self.azure_deployment = azure_deployment
        
        # Initialize appropriate OpenAI client
        if azure_endpoint and azure_api_version:
            print(f"   🔷 Initializing Azure OpenAI client: {azure_endpoint}")
            print(f"   🎯 Using deployment: {azure_deployment or model_name}")
            self.client = AzureOpenAI(
                api_key=self.api_key,
                api_version=azure_api_version,
                azure_endpoint=azure_endpoint
            )
            self.is_azure = True
            # For Azure, use deployment name as model
            self.model_name = azure_deployment or model_name
        else:
            print(f"   🔷 Initializing standard OpenAI client")
            self.client = OpenAI(api_key=self.api_key)
            self.is_azure = False
        
    def _make_api_request_with_retry(self, messages: List[Dict], max_retries: int = 3) -> str:
        """
        Make API request with retry logic for rate limits.
        
        Args:
            messages: Chat messages for the API
            max_retries: Maximum number of retry attempts
            
        Returns:
            Response content from OpenAI
        """
        for attempt in range(max_retries + 1):
            try:
                # Add small delay between requests to be respectful
                if attempt > 0:
                    delay = (2 ** attempt) + random.uniform(1.0, 3.0)  # Exponential backoff
                    logging.info(f"⏳ Waiting {delay:.1f}s before retry attempt {attempt}...")
                    time.sleep(delay)
                else:
                    delay = random.uniform(0.5, 1.5)  # Small random delay
                    time.sleep(delay)
                
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    temperature=0.1,  # Low temperature for consistency
                    max_completion_tokens=16384,  # Set high token limit for detailed responses
                    response_format={"type": "json_object"}  # Ensure JSON response
                )
                # Handle possible None content from API
                content = response.choices[0].message.content if response and response.choices else None
                if not content or not isinstance(content, str) or not content.strip():
                    raise Exception("Empty response from OpenAI")
                return content.strip()
                
            except Exception as e:
                error_str = str(e)
                
                # Check for rate limit and quota errors
                if "rate limit" in error_str.lower() or "429" in error_str or "quota" in error_str.lower():
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * 2  # Exponential backoff: 2s, 4s, 8s
                        logging.warning(f"⚠️ API quota exceeded, retrying (attempt {attempt + 1}/{max_retries + 1})...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logging.error("❌ API quota exceeded and max retries reached.")
                        raise Exception("API quota exceeded - please wait before retrying")
                
                # Check for quota exceeded errors
                elif "quota" in error_str.lower() and "exceeded" in error_str.lower():
                    logging.error("❌ OpenAI API quota exceeded.")
                    raise Exception("API quota exceeded - please check your OpenAI billing")
                
                # For other errors, don't retry
                else:
                    logging.error(f"❌ API request failed: {e}")
                    raise
        
        raise Exception("Max retries reached")
        
    def process_document_for_chunking(self, document_content: str) -> List[Dict[str, Any]]:
        """
        Process a markdown document for contextual chunking using OpenAI.
        
        Args:
            document_content: The markdown content to process
            
        Returns:
            List of chunks with content and keywords
        """
        try:
            # Create contextual chunking prompt
            chunking = ContextualChunking("", document_content)
            prompt = chunking.get_full_prompt()
            
            # Prepare messages
            messages = [
                {
                    "role": "system",
                    "content": "You are an expert at semantic chunking of documents. Follow the instructions precisely and return valid JSON only. The response must be a JSON array of chunk objects."
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ]
            
            # Generate response from OpenAI with retry logic
            logging.info(f"🧠 Processing document with OpenAI {self.model_name} for contextual chunking...")
            response_text = self._make_api_request_with_retry(messages)
            
            # Clean up the response (remove any markdown code blocks if present)
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            if response_text.startswith('```'):
                response_text = response_text[3:]
            
            response_text = response_text.strip()
            
            # Parse JSON
            parsed_response = json.loads(response_text)
            
            # Handle different response formats
            if isinstance(parsed_response, dict):
                # If response is wrapped in an object, look for common keys
                if 'chunks' in parsed_response:
                    chunks = parsed_response['chunks']
                elif 'results' in parsed_response:
                    chunks = parsed_response['results']
                elif 'data' in parsed_response:
                    chunks = parsed_response['data']
                else:
                    # Check if it's a single chunk wrapped in a dict
                    if 'content' in parsed_response and 'keywords' in parsed_response:
                        chunks = [parsed_response]
                    else:
                        raise ValueError("Response format not recognized - no chunks array found")
            else:
                chunks = parsed_response
            
            if not isinstance(chunks, list):
                raise ValueError("Response does not contain a list of chunks")
                
            # Validate chunk structure
            validated_chunks = []
            for i, chunk in enumerate(chunks):
                if not isinstance(chunk, dict):
                    logging.warning(f"Chunk {i} is not a dictionary, skipping")
                    continue
                    
                if 'content' not in chunk:
                    logging.warning(f"Chunk {i} missing 'content' field, skipping")
                    continue
                    
                # Ensure keywords exist
                if 'keywords' not in chunk:
                    chunk['keywords'] = []
                elif not isinstance(chunk['keywords'], list):
                    chunk['keywords'] = []
                
                validated_chunks.append(chunk)
            
            if not validated_chunks:
                raise ValueError("No valid chunks found in response")
            
            logging.info(f"✅ Successfully processed document into {len(validated_chunks)} chunks")
            return validated_chunks
            
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response from OpenAI: {e}")
            logging.error(f"Response text: {response_text[:500]}...")
            raise
        except Exception as e:
            logging.error(f"Error processing document with OpenAI: {e}")
            raise