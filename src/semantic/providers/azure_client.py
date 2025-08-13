#!/usr/bin/env python3
"""
OpenAI and Azure OpenAI clients for contextual chunking.
"""

import json
import logging
import os
from typing import List, Dict, Any
from openai import OpenAI, AzureOpenAI
from ...prompts.contextual_chunking import ContextualChunking
from .base_client import BaseLLMClient


class OpenAIClient(BaseLLMClient):
    """Client for interacting with the standard OpenAI API for contextual chunking."""

    def __init__(self, api_key: str = None, model_name: str = None):
        api_key = api_key or os.getenv('OPENAI_API_KEY')
        model_name = model_name or 'gpt-4o-mini'
        super().__init__(api_key, model_name)

        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not found.")

        self.client = OpenAI(api_key=self.api_key)

    def process_document_for_chunking(self, document_content: str) -> List[Dict[str, Any]]:
        try:
            chunking = ContextualChunking("", document_content)
            prompt = chunking.get_full_prompt()

            logging.info("🧠 Processing document with OpenAI for contextual chunking...")
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "You are an expert at semantic chunking of documents. Return valid JSON only."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                response_format={"type": "json_object"},
            )

            response_text = response.choices[0].message.content.strip()
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            if response_text.startswith('```'):
                response_text = response_text[3:]
            response_text = response_text.strip()

            parsed_response = json.loads(response_text)
            chunks = parsed_response['chunks'] if isinstance(parsed_response, dict) and 'chunks' in parsed_response else parsed_response
            if not isinstance(chunks, list):
                raise ValueError("Response does not contain a list of chunks")

            validated = []
            for chunk in chunks:
                if isinstance(chunk, dict) and 'content' in chunk:
                    if 'keywords' not in chunk or not isinstance(chunk['keywords'], list):
                        chunk['keywords'] = []
                    validated.append(chunk)
            logging.info(f"✅ Successfully processed document into {len(validated)} chunks")
            return validated
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response from OpenAI: {e}")
            logging.error(f"Response text: {response_text[:500]}...")
            raise
        except Exception as e:
            logging.error(f"Error processing document with OpenAI: {e}")
            raise


class AzureOpenAIClient(BaseLLMClient):
    """Client for interacting with Azure OpenAI API for contextual chunking."""

    def __init__(self, api_key: str = None, model_name: str = None, endpoint: str = None, api_version: str = None):
        api_key = api_key or os.getenv('OPENAI_API_KEY') or os.getenv('AZURE_OPENAI_API_KEY_41')
        model_name = model_name or 'gpt-4.1'
        endpoint = endpoint or os.getenv('AZURE_OPENAI_ENDPOINT', 'https://doc-parsing.openai.azure.com/')
        api_version = api_version or os.getenv('AZURE_OPENAI_API_VERSION', '2024-02-01')

        super().__init__(api_key, model_name)

        if not self.api_key:
            raise ValueError("Azure OpenAI API key not found. Set OPENAI_API_KEY or AZURE_OPENAI_API_KEY_41.")

        self.client = AzureOpenAI(
            api_key=self.api_key,
            api_version=api_version,
            azure_endpoint=endpoint,
        )
        self.endpoint = endpoint
        self.api_version = api_version

    def process_document_for_chunking(self, document_content: str) -> List[Dict[str, Any]]:
        try:
            chunking = ContextualChunking("", document_content)
            prompt = chunking.get_full_prompt()

            logging.info("🧠 Processing document with Azure OpenAI for contextual chunking...")
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "You are an expert at semantic chunking of documents. Return valid JSON only."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                response_format={"type": "json_object"},
            )

            response_text = response.choices[0].message.content.strip()
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            if response_text.startswith('```'):
                response_text = response_text[3:]
            response_text = response_text.strip()

            parsed_response = json.loads(response_text)
            chunks = parsed_response['chunks'] if isinstance(parsed_response, dict) and 'chunks' in parsed_response else parsed_response
            if not isinstance(chunks, list):
                raise ValueError("Response does not contain a list of chunks")

            validated = []
            for chunk in chunks:
                if isinstance(chunk, dict) and 'content' in chunk:
                    if 'keywords' not in chunk or not isinstance(chunk['keywords'], list):
                        chunk['keywords'] = []
                    validated.append(chunk)
            logging.info(f"✅ Successfully processed document into {len(validated)} chunks")
            return validated
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response from Azure OpenAI: {e}")
            logging.error(f"Response text: {response_text[:500]}...")
            raise
        except Exception as e:
            logging.error(f"Error processing document with Azure OpenAI: {e}")
            raise