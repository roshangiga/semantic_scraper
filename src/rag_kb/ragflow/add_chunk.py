#!/usr/bin/env python3
"""
Process and upload JSON files as chunks to RAGFlow dataset.
"""

import json
import os
import requests
import urllib3
import logging
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration variables - edit these as needed
RAGFLOW_API_KEY = "ragflow-NiMTRkZWJhNTI5NzExZjBiYTY2MDI0Mm"
RAGFLOW_BASE_URL = "https://rag-chat.innov.mt"


class RAGFlowClient:
    def __init__(self, api_key: str, base_url: str = "https://rag-chat.innov.mt"):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def list_documents(self, dataset_id: str, keywords: str = None) -> dict:
        """List documents in a dataset."""
        url = f"{self.base_url}/api/v1/datasets/{dataset_id}/documents"
        params = {}
        if keywords:
            params['keywords'] = keywords
        
        try:
            response = requests.get(url, headers=self.headers, params=params, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as e:
            if "getaddrinfo failed" in str(e) or "NameResolutionError" in str(e):
                logging.error("❌ RAGFlow server unreachable - upload failed")
            else:
                logging.error("❌ RAGFlow connection failed - upload failed")
            raise Exception("RAGFlow connection failed") from None
        except Exception as e:
            logging.error(f"❌ RAGFlow API error: {e}")
            raise
    
    def find_document_by_name(self, dataset_id: str, document_name: str) -> Optional[dict]:
        """Find a document by name in the dataset."""
        docs_response = self.list_documents(dataset_id, keywords=document_name)
        docs = docs_response.get('data', {}).get('docs', [])
        
        for doc in docs:
            if doc.get('name') == document_name:
                return doc
        return None
    
    def add_chunk(self, dataset_id: str, document_id: str, content: str, 
                  important_keywords: list = None, questions: list = None) -> dict:
        """Add a chunk to a document."""
        url = f"{self.base_url}/api/v1/datasets/{dataset_id}/documents/{document_id}/chunks"
        
        data = {"content": content}
        if important_keywords:
            data["important_keywords"] = important_keywords
        if questions:
            data["questions"] = questions
        
        try:
            response = requests.post(url, headers=self.headers, json=data, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as e:
            if "getaddrinfo failed" in str(e) or "NameResolutionError" in str(e):
                logging.error("❌ RAGFlow server unreachable - upload failed")
            else:
                logging.error("❌ RAGFlow connection failed - upload failed")
            raise Exception("RAGFlow connection failed") from None
        except Exception as e:
            logging.error(f"❌ RAGFlow API error: {e}")
            raise


def process_and_upload_file(file_path: str, api_key: str, base_url: str, dataset_id: str, document_id: str) -> bool:
    """Process a single file (JSON or MD) and immediately upload to RAGFlow."""
    print(f"📄 Processing file: {file_path}")
    
    try:
        # Determine file type and read content
        if file_path.endswith('.json'):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # Use entire JSON as content
            full_content = json.dumps(data, indent=2, ensure_ascii=False)
            print(f"✅ Extracted JSON content: {len(full_content)} characters")
        elif file_path.endswith('.md'):
            with open(file_path, 'r', encoding='utf-8') as f:
                full_content = f.read()
            print(f"✅ Extracted MD content: {len(full_content)} characters")
        else:
            print(f"⚠️ Unsupported file type: {file_path}")
            return False
        
        if not full_content.strip():
            print(f"⚠️ No content found in {file_path}")
            return False
        
        # Use LLM to generate keywords and questions
        azure_api_key = os.getenv("AZURE_OPENAI_API_KEY-4o")
        if not azure_api_key:
            raise Exception("Azure OpenAI API key is required")
        
        from openai import AzureOpenAI
        llm_client = AzureOpenAI(
            api_key=azure_api_key,
            api_version="2024-12-01-preview",
            azure_endpoint="https://consumersupport-rag-instance.openai.azure.com/"
        )
        
        # Generate keywords
        content_type = "JSON" if file_path.endswith('.json') else "Markdown"
        keywords_prompt = f"""
        Based on this {content_type} content from MyT.mu website:
        
        {full_content}
        
        ---
        
        From the provided {content_type} content, extract 2 to 5 concise and specific keywords for search indexing or content categorization.
        
        1. The keywords must reflect core offerings, product names, or specific service features mentioned in the content.
        
        2. Each keyword should clearly refer to the exact product or app, not generic categories (e.g., use “my.t Traffic Watch app” instead of “app download support”).
        
        3. At least one keyword must indicate the audience type, using “Consumer-only” or “Enterprise-only”.
        
        4. Do not use vague or generic terms like “mobile app,” “support,” or “services.”
        
        5. Be product-specific and feature-aware (e.g., use “Drone View on my.t Traffic Watch app” instead of just “Drone View”).
        
        6. We know its for Mauritius. Not a keyword. However if it is a specific location or another country, you may include it.
        
        Return the keywords strictly as a JSON array of strings, with no explanations or extra text.
        """
        
        keywords_response = llm_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": keywords_prompt}],
            max_tokens=100,
            temperature=0.1
        )
        
        keywords_text = keywords_response.choices[0].message.content.strip()
        if keywords_text.startswith('```json'):
            keywords_text = keywords_text.replace('```json', '').replace('```', '').strip()
        elif keywords_text.startswith('```'):
            keywords_text = keywords_text.replace('```', '').strip()
        
        keywords = json.loads(keywords_text)
        print(f"✅ Generated keywords: {keywords}")
        
        # Generate questions
        questions_prompt = f"""
        Based on this {content_type} content from MyT.mu specific page:
        
        
        {full_content}
        
        ---
                
        From the provided {content_type} content below, extract all questions that are clearly and directly answerable using the content.
        
        1. Only include questions that have clear and direct answers in the text.
        
        2. Do not include questions where the answer must be inferred or is ambiguous.
                
        3. Be product-specific and feature-aware (e.g., use “How to check my validity period for mobile postpay” instead of just “"How to check my validity period?"”). Always include the product/service in the question even if repetitive.
                
        4. If no such questions exist, return an empty JSON array.
        
        5. Do not repeat same question.
        
        
        Return the result strictly as a JSON array of strings with no extra text.
        """
        
        # questions_response = llm_client.chat.completions.create(
        #     model="gpt-4o",
        #     messages=[{"role": "user", "content": questions_prompt}],
        #     max_tokens=300,
        #     temperature=0.1
        # )
        #
        # questions_text = questions_response.choices[0].message.content.strip()
        # if questions_text.startswith('```json'):
        #     questions_text = questions_text.replace('```json', '').replace('```', '').strip()
        # elif questions_text.startswith('```'):
        #     questions_text = questions_text.replace('```', '').strip()
        #
        # try:
        #     questions = json.loads(questions_text)
        # except json.JSONDecodeError as e:
        #     print(f"⚠️ Failed to parse questions, using empty array: {e}")
        #     questions = []
        #
        # print(f"✅ Generated {len(questions)} questions")
        
        # Immediately upload to RAGFlow
        print(f"🔄 Uploading to RAGFlow...")
        
        ragflow_client = RAGFlowClient(api_key, base_url)
        result = ragflow_client.add_chunk(
            dataset_id=dataset_id,
            document_id=document_id,
            content=full_content,
            important_keywords=keywords,
            # questions=questions
        )
        
        chunk_id = result.get('data', {}).get('id', 'Unknown')
        print(f"✅ Successfully uploaded chunk: {chunk_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error processing/uploading {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return False


def process_all_files_streaming(website: str, timestamp: str, api_key: str, base_url: str, dataset_id: str, document_id: str) -> int:
    """Process and upload files one by one immediately."""
    folder_path = f"generated/{timestamp}/{website}"
    
    if not os.path.exists(folder_path):
        print(f"❌ Folder not found: {folder_path}")
        return 0
    
    # Look for both JSON and MD files
    json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    md_files = [f for f in os.listdir(folder_path) if f.endswith('.md')]
    all_files = json_files + md_files
    
    if not all_files:
        print(f"❌ No JSON or MD files found in {folder_path}")
        return 0
    
    print(f"📁 Found {len(json_files)} JSON files and {len(md_files)} MD files to process and upload")
    print(f"📁 Total: {len(all_files)} files to process")
    
    success_count = 0
    
    for i, filename in enumerate(all_files, 1):
        print(f"\n[{i}/{len(all_files)}] Processing {filename}")
        file_path = os.path.join(folder_path, filename)
        
        if process_and_upload_file(file_path, api_key, base_url, dataset_id, document_id):
            success_count += 1
            print(f"✅ File {i}/{len(all_files)} completed successfully")
        else:
            print(f"❌ File {i}/{len(all_files)} failed")
        
        # Small delay between uploads
        import time
        time.sleep(1)
    
    print(f"\n🎉 Completed: {success_count}/{len(all_files)} files uploaded successfully")
    return success_count






