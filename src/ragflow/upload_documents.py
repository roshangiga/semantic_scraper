#!/usr/bin/env python3
"""
Upload documents to RAGFlow dataset using direct API calls.
"""

import os
import requests
import urllib3
from pathlib import Path
from typing import List

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def upload_document(api_key: str, base_url: str, dataset_id: str, file_path: str) -> dict:
    """
    Upload a single document to a RAGFlow dataset.
    
    Args:
        api_key: RAGFlow API key
        base_url: RAGFlow API base URL
        dataset_id: Dataset ID to upload to
        file_path: Path to the file to upload
        
    Returns:
        Response from the API
    """
    url = f"{base_url.rstrip('/')}/api/v1/datasets/{dataset_id}/documents"
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    with open(file_path, 'rb') as file:
        files = {'file': (os.path.basename(file_path), file, 'application/octet-stream')}
        response = requests.post(url, headers=headers, files=files, verify=False)
        response.raise_for_status()
        return response.json()


def upload_documents_from_folder(api_key: str, base_url: str, dataset_id: str, folder_path: str, 
                                file_pattern: str = "*.json") -> List[dict]:
    """
    Upload all files matching pattern from a folder to RAGFlow dataset.
    
    Args:
        api_key: RAGFlow API key
        base_url: RAGFlow API base URL
        dataset_id: Dataset ID to upload to
        folder_path: Path to folder containing files
        file_pattern: Glob pattern for files to upload (default: *.json)
        
    Returns:
        List of responses from successful uploads
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    
    files = list(folder.glob(file_pattern))
    if not files:
        print(f"No files matching '{file_pattern}' found in {folder_path}")
        return []
    
    print(f"Found {len(files)} files to upload")
    
    results = []
    for file_path in files:
        try:
            print(f"Uploading: {file_path.name}")
            result = upload_document(api_key, base_url, dataset_id, str(file_path))
            results.append(result)
            print(f"Successfully uploaded: {file_path.name}")
        except Exception as e:
            print(f"Failed to upload {file_path.name}: {e}")
    
    print(f"Successfully uploaded {len(results)} out of {len(files)} files")
    return results


def upload_empty_file(api_key: str, base_url: str, dataset_id: str, filename: str = "empty.txt") -> dict:
    """
    Upload a single empty file to a RAGFlow dataset.
    
    Args:
        api_key: RAGFlow API key
        base_url: RAGFlow API base URL
        dataset_id: Dataset ID to upload to
        filename: Name for the empty file (default: "empty.txt")
        
    Returns:
        Response from the API
    """
    url = f"{base_url.rstrip('/')}/api/v1/datasets/{dataset_id}/documents"
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    # Create empty file content
    empty_content = b""
    files = {'file': (filename, empty_content, 'application/octet-stream')}
    
    print(f"Uploading empty file: {filename}")
    response = requests.post(url, headers=headers, files=files, verify=False)
    response.raise_for_status()
    
    print(f"Successfully uploaded empty file: {filename}")
    return response.json()


def upload_generated_files(api_key: str, base_url: str, dataset_id: str, 
                          website: str, timestamp: str) -> List[dict]:
    """
    Upload JSON files from generated/{timestamp}/{website} folder.
    
    Args:
        api_key: RAGFlow API key
        base_url: RAGFlow API base URL
        dataset_id: Dataset ID to upload to
        website: Website name (e.g., "myt.mu")
        timestamp: Timestamp (e.g., "20250626_165204")
        
    Returns:
        List of responses from successful uploads
    """
    folder_path = f"generated/{timestamp}/{website}"
    
    print(f"Looking for JSON files in: {folder_path}")
    
    return upload_documents_from_folder(api_key, base_url, dataset_id, folder_path, "*.json")