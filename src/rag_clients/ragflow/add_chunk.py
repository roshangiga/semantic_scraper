#!/usr/bin/env python3
"""
Upload semantic chunks from crawled_semantic directory to RAGFlow dataset.
"""

import json
import os
import requests
import urllib3
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path
from dotenv import load_dotenv
try:
    from ...console import console, print_error, print_success, print_warning, print_info, print_processing
except ImportError:
    # Fallback to regular print if rich not available
    def print_error(msg): print(f"❌ {msg}")
    def print_success(msg): print(f"✅ {msg}")
    def print_warning(msg): print(f"⚠️ {msg}")
    def print_info(msg): print(f"ℹ️ {msg}")
    def print_processing(msg): print(f"🔄 {msg}")


# Load environment variables from .env file
load_dotenv()

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration variables from environment
RAGFLOW_API_KEY = os.getenv("RAGFLOW_API_KEY")
RAGFLOW_BASE_URL = os.getenv("RAGFLOW_URL")

# Validate environment variables
if not RAGFLOW_API_KEY:
    raise ValueError("RAGFLOW_API_KEY environment variable is missing. Please set it in your .env file or environment.")
if not RAGFLOW_BASE_URL:
    raise ValueError("RAGFLOW_URL environment variable is missing. Please set it in your .env file or environment.")


class RAGFlowClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def _make_request_with_retry(self, method: str, url: str, max_retries: int = 3, **kwargs):
        """
        Make HTTP request with exponential backoff retry for timeouts and server errors.
        
        Args:
            method: HTTP method ('GET', 'POST', 'PUT', 'DELETE')
            url: Request URL
            max_retries: Maximum number of retry attempts
            **kwargs: Additional arguments passed to requests
            
        Returns:
            Response object
        """
        import time
        import random
        
        for attempt in range(max_retries + 1):
            try:
                # Add delay with exponential backoff for retries
                if attempt > 0:
                    delay = (2 ** attempt) + random.uniform(1.0, 3.0)  # 3-5s, 5-7s, 9-11s
                    print(f"    ⏳ RAG API retry {attempt}/{max_retries}, waiting {delay:.1f}s...")
                    time.sleep(delay)
                
                # Make the request
                if method.upper() == 'GET':
                    response = requests.get(url, **kwargs)
                elif method.upper() == 'POST':
                    response = requests.post(url, **kwargs)
                elif method.upper() == 'PUT':
                    response = requests.put(url, **kwargs)
                elif method.upper() == 'DELETE':
                    response = requests.delete(url, **kwargs)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.Timeout as e:
                if attempt < max_retries:
                    print(f"    ⚠️ RAG API timeout, retrying (attempt {attempt + 1}/{max_retries + 1})...")
                    continue
                else:
                    print(f"    ❌ RAG API timeout after {max_retries + 1} attempts")
                    raise
                    
            except requests.exceptions.ConnectionError as e:
                if "getaddrinfo failed" in str(e) or "NameResolutionError" in str(e):
                    # Don't print here - let the calling code handle it to avoid duplicates
                    raise Exception("RAGFlow server unreachable - upload failed") from None
                else:
                    # Don't print here - let the calling code handle it to avoid duplicates
                    raise Exception("RAGFlow connection failed - upload failed") from None
                    
            except requests.exceptions.RequestException as e:
                # Check for specific server errors that should be retried
                if hasattr(e, 'response') and e.response is not None:
                    status_code = e.response.status_code
                    if status_code in [500, 502, 503, 504]:  # Server errors
                        if attempt < max_retries:
                            print(f"    ⚠️ RAG API server error ({status_code}), retrying (attempt {attempt + 1}/{max_retries + 1})...")
                            continue
                        else:
                            print(f"    ❌ RAG API server error ({status_code}) after {max_retries + 1} attempts")
                            raise
                
                # For other errors (4xx client errors, etc), don't retry
                raise
        
        raise Exception("Max retries reached")
    
    def list_documents(self, dataset_id: str, keywords: str = None) -> dict:
        """List documents in a dataset."""
        url = f"{self.base_url}/api/v1/datasets/{dataset_id}/documents"
        params = {}
        if keywords:
            params['keywords'] = keywords
        
        response = self._make_request_with_retry('GET', url, headers=self.headers, params=params, verify=False)
        return response.json()
    
    def find_document_by_name(self, dataset_id: str, document_name: str) -> Optional[dict]:
        """Find a document by name in the dataset."""
        try:
            docs_response = self.list_documents(dataset_id, keywords=document_name)
            
            # Handle API response format {'code': 0, 'data': {...}}
            if isinstance(docs_response, dict) and 'data' in docs_response:
                data = docs_response['data']
                # Look for docs in the data structure
                if isinstance(data, dict):
                    docs = data.get('docs', [])
                elif isinstance(data, list):
                    docs = data
                else:
                    docs = []
            elif isinstance(docs_response, list):
                # Fallback: direct list
                docs = docs_response
            else:
                docs = []
            
            # Search for document by name
            for doc in docs:
                if isinstance(doc, dict) and doc.get('name') == document_name:
                    return doc
            
            return None
        except Exception as e:
            print(f"Error searching for document: {e}")
            return None
    
    def list_datasets(self) -> dict:
        """List all datasets."""
        url = f"{self.base_url}/api/v1/datasets"
        response = self._make_request_with_retry('GET', url, headers=self.headers, verify=False)
        return response.json()
    
    def create_dataset(self, name: str, description: str = None) -> dict:
        """Create a new dataset."""
        url = f"{self.base_url}/api/v1/datasets"
        data = {
            "name": name,
            "permission": "team"
        }
        if description:
            data["description"] = description
        
        response = self._make_request_with_retry('POST', url, headers=self.headers, json=data, verify=False)
        return response.json()
    
    def find_or_create_dataset(self, name: str, description: str = None) -> str:
        """Find existing dataset or create new one."""
        try:
            # Try to find existing dataset
            datasets_response = self.list_datasets()
            
            # API returns {'code': 0, 'data': [...]}
            if isinstance(datasets_response, dict) and 'data' in datasets_response:
                datasets = datasets_response['data']
            elif isinstance(datasets_response, list):
                # Fallback: direct list
                datasets = datasets_response
            else:
                datasets = []
            
            # Search for existing dataset
            for dataset in datasets:
                if isinstance(dataset, dict) and dataset.get('name') == name:
                    dataset_id = dataset.get('id')
                    return dataset_id
            
            # Create new dataset if not found
            create_response = self.create_dataset(name, description)
            
            # API returns {'code': 0, 'data': {...}}
            if isinstance(create_response, dict) and 'data' in create_response:
                dataset_data = create_response['data']
                if isinstance(dataset_data, dict):
                    return dataset_data.get('id')
            
            return None
        except Exception as e:
            # Don't print here - let the calling code handle display
            # This avoids duplicate ugly error messages
            raise
    
    def create_document(self, dataset_id: str, name: str) -> dict:
        """Create a new document in a dataset."""
        # NOTE: In RAGFlow, documents are created by uploading files, not by JSON API
        # This method is kept for backward compatibility but won't work
        url = f"{self.base_url}/api/v1/datasets/{dataset_id}/documents"
        data = {"name": name}
        
        response = self._make_request_with_retry('POST', url, headers=self.headers, json=data, verify=False)
        return response.json()
    
    def find_or_create_document(self, dataset_id: str, name: str) -> str:
        """Find existing document or create new one."""
        try:
            # Try to find existing document
            doc = self.find_document_by_name(dataset_id, name)
            if doc and isinstance(doc, dict):
                return doc.get('id')
            
            # Create new document if not found
            create_response = self.create_document(dataset_id, name)
            
            # Handle API response format {'code': 0, 'data': {...}}
            if isinstance(create_response, dict) and 'data' in create_response:
                doc_data = create_response['data']
                if isinstance(doc_data, dict):
                    return doc_data.get('id')
            
            return None
        except Exception as e:
            print(f"Error finding/creating document: {e}")
            raise
    
    def upload_file(self, dataset_id: str, file_path: str) -> dict:
        """Upload a file to a dataset (following working demo pattern)."""
        url = f"{self.base_url}/api/v1/datasets/{dataset_id}/documents"
        
        # Use file upload headers (remove Content-Type to let requests set it)
        upload_headers = {"Authorization": f"Bearer {self.api_key}"}
        
        with open(file_path, 'rb') as file:
            files = {'file': (os.path.basename(file_path), file, 'application/octet-stream')}
            response = self._make_request_with_retry('POST', url, headers=upload_headers, files=files, verify=False)
            return response.json()
    
    def set_document_metadata(self, dataset_id: str, document_id: str, source: str, timestamp: str) -> bool:
        """Set metadata for a document using RAGFlow API."""
        try:
            url = f"{self.base_url}/api/v1/datasets/{dataset_id}/documents/{document_id}"
            
            # Parse timestamp to readable date (format: 2025-08-14 12:24)
            try:
                from datetime import datetime
                dt = datetime.strptime(timestamp, '%Y%m%d_%H%M%S')
                date = dt.strftime('%Y-%m-%d %H:%M')
            except:
                date = timestamp
            
            # Prepare request body according to API spec - only date and source
            request_body = {
                "meta_fields": {
                    "source": source,
                    "date": date
                }
            }
            
            # Set metadata using PUT request
            response = self._make_request_with_retry('PUT', url, headers=self.headers, json=request_body, verify=False)
            
            result = response.json()
            # Metadata set silently
            return True
                
        except Exception as e:
            print(f"[WARN] Could not set document metadata: {e}")
            return False
    
    def add_chunk(self, dataset_id: str, document_id: str, content: str, 
                  important_keywords: list = None, questions: list = None) -> dict:
        """Add a chunk to a document."""
        url = f"{self.base_url}/api/v1/datasets/{dataset_id}/documents/{document_id}/chunks"
        
        data = {"content": content}
        if important_keywords:
            data["important_keywords"] = important_keywords
        if questions:
            data["questions"] = questions
        
        response = self._make_request_with_retry('POST', url, headers=self.headers, json=data, verify=False)
        return response.json()


def get_latest_timestamp_dir(base_dir: str = "crawled_semantic") -> Optional[str]:
    """Get the latest timestamp directory from crawled_semantic."""
    if not os.path.exists(base_dir):
        print(f"❌ Directory not found: {base_dir}")
        return None
    
    # Get all timestamp directories
    timestamp_dirs = []
    for item in os.listdir(base_dir):
        item_path = os.path.join(base_dir, item)
        if os.path.isdir(item_path):
            try:
                # Check if it matches timestamp format YYYYMMDD_HHMMSS
                from datetime import datetime
                datetime.strptime(item, '%Y%m%d_%H%M%S')
                timestamp_dirs.append(item)
            except ValueError:
                continue
    
    if not timestamp_dirs:
        print(f"❌ No timestamp directories found in {base_dir}")
        return None
    
    # Sort and get the latest
    timestamp_dirs.sort()
    latest = timestamp_dirs[-1]
    print_info(f"Using latest timestamp directory: {latest}")
    return os.path.join(base_dir, latest)


def process_semantic_json(json_path: str, dataset_id: str, document_id: str, 
                         ragflow_client: RAGFlowClient) -> int:
    """Process a single semantic JSON file and upload its chunks to RAGFlow."""
    print(f"\n📄 Processing: {json_path}")
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        source = data.get('source', 'Unknown')
        chunks = data.get('chunks', [])
        
        if not chunks:
            print(f"⚠️ No chunks found in {json_path}")
            return 0
        
        print(f"📍 Source: {source}")
        print_info(f"Found {len(chunks)} chunks to upload")
        
        success_count = 0
        
        for i, chunk in enumerate(chunks, 1):
            content = chunk.get('content', '')
            keywords = chunk.get('keywords', [])
            
            if not content:
                print_warning(f"Chunk {i}: Empty content, skipping")
                continue
            
            try:
                result = ragflow_client.add_chunk(
                    dataset_id=dataset_id,
                    document_id=document_id,
                    content=content,
                    important_keywords=keywords
                )
                
                chunk_id = result.get('data', {}).get('id', 'Unknown')
                print_success(f"Chunk {i}/{len(chunks)}: Uploaded (ID: {chunk_id})")
                success_count += 1
                
            except Exception as e:
                print_error(f"Chunk {i}/{len(chunks)}: Failed - {e}")
        
        print_success(f"Completed: {success_count}/{len(chunks)} chunks uploaded successfully")
        return success_count
        
    except Exception as e:
        print(f"❌ Error processing {json_path}: {e}")
        return 0


def upload_chunks_from_data(data: Dict[str, Any], timestamp: str, domain: str, original_filename: str = None) -> int:
    """Upload chunks from semantic data using the working ragflow-demo approach.
    
    Args:
        data: Semantic data with chunks
        timestamp: Timestamp for dataset naming
        domain: Domain for dataset naming
        original_filename: Original JSON filename to use for document name
        
    Returns:
        Number of chunks uploaded
    """
    # Create dataset name using timestamp_domain format
    dataset_name = f"{timestamp}_{domain}"
    
    # Import os module
    import os
    
    # Create document name from original filename (keep original extension)
    if original_filename:
        document_name = os.path.basename(original_filename)
    else:
        document_name = f"{domain}.json"
    
    try:
        # Initialize client
        ragflow_client = RAGFlowClient(RAGFLOW_API_KEY, RAGFLOW_BASE_URL)
        
        # Find or create dataset
        dataset_id = ragflow_client.find_or_create_dataset(
            name=dataset_name,
            description=f"Semantic chunks from {domain} crawled at {timestamp}"
        )
        
        if not dataset_id:
            return 0
        
        # Check if document already exists
        existing_doc = ragflow_client.find_document_by_name(dataset_id, document_name)
        if existing_doc and isinstance(existing_doc, dict):
            document_id = existing_doc.get('id')
            if document_id:
                # Use existing document
                return process_semantic_json_data(data, dataset_id, document_id, ragflow_client)
        
        # Create an empty document first (RAGFlow requirement) 
        import tempfile
        import os
        
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, document_name)
        
        # Create empty placeholder file
        with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
            temp_file.write(f"Document: {document_name}\n")
        
        try:
            # Upload file to create document
            upload_result = ragflow_client.upload_file(dataset_id, temp_file_path)
            
            if isinstance(upload_result, dict) and 'data' in upload_result:
                upload_data = upload_result['data']
                if isinstance(upload_data, list) and len(upload_data) > 0:
                    document_info = upload_data[0]
                    document_id = document_info.get('id')
                    
                    if document_id:
                        # Set metadata for the document - get source from semantic JSON
                        source = data.get('source', 'Unknown')
                        ragflow_client.set_document_metadata(
                            dataset_id=dataset_id,
                            document_id=document_id,
                            source=source,
                            timestamp=timestamp
                        )
                        
                        # Use the existing working function to process chunks
                        return process_semantic_json_data(data, dataset_id, document_id, ragflow_client)
                    
            print(f"[ERROR] Failed to create document")
            return 0
        
        finally:
            try:
                os.unlink(temp_file_path)
            except Exception:
                pass
    
    except Exception as e:
        # Don't print here - let the calling code handle display with proper panels
        # This avoids duplicate ugly error messages
        return 0


def process_semantic_json_data(data: Dict[str, Any], dataset_id: str, document_id: str, ragflow_client: RAGFlowClient) -> int:
    """Process semantic JSON data and upload chunks with keywords (using working ragflow-demo approach)."""
    source = data.get('source', 'Unknown')
    chunks = data.get('chunks', [])
    
    if not chunks:
        print(f"[WARN] No chunks found")
        return 0
    
    success_count = 0
    
    for i, chunk in enumerate(chunks, 1):
        content = chunk.get('content', '')
        keywords = chunk.get('keywords', [])
        
        if not content:
            continue
        
        try:
            # Use the exact working approach from ragflow-demo
            result = ragflow_client.add_chunk(
                dataset_id=dataset_id,
                document_id=document_id,
                content=content,
                important_keywords=keywords
            )
            success_count += 1
            
        except Exception as e:
            print(f"  [ERROR] RAG chunk {i}/{len(chunks)} failed: {e}")
    return success_count


def upload_from_semantic_dir(dataset_id: str, document_id: str, 
                           timestamp_dir: str = None, domain_filter: str = None) -> int:
    """
    Upload all semantic chunks from crawled_semantic directory to RAGFlow.
    
    Args:
        dataset_id: RAGFlow dataset ID
        document_id: RAGFlow document ID
        timestamp_dir: Specific timestamp directory to use (uses latest if None)
        domain_filter: Optional domain to filter (e.g., 'devices.myt.mu')
    
    Returns:
        Total number of chunks uploaded successfully
    """
    # Get the directory to process
    if timestamp_dir:
        semantic_dir = os.path.join("crawled_semantic", timestamp_dir)
        if not os.path.exists(semantic_dir):
            print(f"❌ Directory not found: {semantic_dir}")
            return 0
    else:
        semantic_dir = get_latest_timestamp_dir("crawled_semantic")
        if not semantic_dir:
            return 0
    
    print(f"📂 Processing semantic chunks from: {semantic_dir}")
    
    # Initialize RAGFlow client
    ragflow_client = RAGFlowClient(RAGFLOW_API_KEY, RAGFLOW_BASE_URL)
    
    # Find all JSON files
    json_files = []
    
    if domain_filter:
        # Process specific domain
        domain_dir = os.path.join(semantic_dir, domain_filter)
        if os.path.exists(domain_dir):
            for file in os.listdir(domain_dir):
                if file.endswith('.json'):
                    json_files.append(os.path.join(domain_dir, file))
        else:
            print(f"❌ Domain directory not found: {domain_dir}")
            return 0
    else:
        # Process all domains
        for domain in os.listdir(semantic_dir):
            domain_dir = os.path.join(semantic_dir, domain)
            if os.path.isdir(domain_dir):
                for file in os.listdir(domain_dir):
                    if file.endswith('.json'):
                        json_files.append(os.path.join(domain_dir, file))
    
    if not json_files:
        print(f"❌ No JSON files found")
        return 0
    
    print(f"📁 Found {len(json_files)} JSON files to process")
    
    total_chunks = 0
    
    for json_file in json_files:
        chunks_uploaded = process_semantic_json(json_file, dataset_id, document_id, ragflow_client)
        total_chunks += chunks_uploaded
        
        # Small delay between files
        import time
        time.sleep(1)
    
    print(f"\n🎉 Upload complete: {total_chunks} total chunks uploaded from {len(json_files)} files")
    return total_chunks


def upload_single_file_streaming(json_path: str) -> int:
    """Upload a single semantic JSON file for streaming mode.
    
    Args:
        json_path: Path to semantic JSON file
        
    Returns:
        Number of chunks uploaded
    """
    try:
        # Parse path to extract timestamp and domain
        path_parts = Path(json_path).parts
        
        # Expected format: .../crawled_semantic/TIMESTAMP/DOMAIN/file.json
        if len(path_parts) < 3:
            print(f"⚠️ Unexpected file path format: {json_path}")
            return 0
        
        timestamp = path_parts[-3]  # timestamp directory
        domain = path_parts[-2]     # domain directory
        
        # Load the JSON data
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Upload using the new method with original filename
        return upload_chunks_from_data(data, timestamp, domain, json_path)
        
    except Exception as e:
        print(f"[ERROR] Error in streaming upload for {json_path}: {e}")
        import traceback
        traceback.print_exc()
        return 0


def main():
    """Main function for command-line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload semantic chunks to RAGFlow with automatic dataset/document creation')
    parser.add_argument('--timestamp', help='Specific timestamp directory (uses latest if not specified)')
    parser.add_argument('--domain', help='Filter by specific domain (e.g., devices.myt.mu)')
    parser.add_argument('--file', help='Upload single file (for streaming mode)')
    
    args = parser.parse_args()
    
    try:
        if args.file:
            # Single file upload (streaming mode)
            total = upload_single_file_streaming(args.file)
        else:
            # Batch upload (legacy mode) - kept for backward compatibility
            # Note: This function signature needs updating for new API
            print("❌ Batch mode not implemented with new API - use streaming mode instead")
            return 1
        
        if total > 0:
            print(f"\n✅ Successfully uploaded {total} chunks to RAGFlow")
        else:
            print(f"\n❌ No chunks were uploaded")
            
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())