#!/usr/bin/env python3
"""
Create dataset functionality for RAGFlow using direct API calls.
"""

import requests
import time
import urllib3
import logging
from typing import Optional

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class RAGFlowClient:
    def __init__(self, api_key: str, base_url: str = "https://rag-chat.innov.mt"):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def get_datasets(self) -> list:
        """Get all existing datasets."""
        try:
            response = requests.get(f"{self.base_url}/api/v1/datasets", headers=self.headers, verify=False)
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
    
    def delete_dataset(self, dataset_id: str) -> bool:
        """Delete a dataset by ID."""
        data = {"ids": [dataset_id]}
        response = requests.delete(f"{self.base_url}/api/v1/datasets", headers=self.headers, json=data, verify=False)
        return response.status_code == 200
    
    def create_dataset(self, name: str, description: str = "") -> dict:
        """Create a new dataset."""
        data = {
            "name": name,
            "description": description,
            "permission": "team"
        }
        try:
            response = requests.post(f"{self.base_url}/api/v1/datasets", headers=self.headers, json=data, verify=False)
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
    
    def find_dataset_by_name(self, name: str) -> Optional[dict]:
        """Find dataset by name."""
        datasets = self.get_datasets()
        for dataset in datasets.get('data', []):
            if dataset.get('name') == name:
                return dataset
        return None


def create_or_replace_dataset(api_key: str, base_url: str, dataset_name: str, description: str = "") -> dict:
    """
    Create a dataset if it doesn't exist, otherwise return existing dataset.
    
    Args:
        api_key: RAGFlow API key
        base_url: RAGFlow API base URL
        dataset_name: Name for the dataset
        description: Optional description
        
    Returns:
        Dataset information (existing or newly created)
    """
    client = RAGFlowClient(api_key, base_url)
    
    # Check if dataset already exists
    existing_dataset = client.find_dataset_by_name(dataset_name)
    if existing_dataset:
        print(f"Dataset '{dataset_name}' already exists. Using existing dataset...")
        dataset_id = existing_dataset.get('id')
        print(f"Using existing dataset: {dataset_id}")
        # Return in same format as create response
        return {
            "code": 0,
            "data": existing_dataset
        }
    
    # Create new dataset
    print(f"Creating new dataset '{dataset_name}'...")
    result = client.create_dataset(dataset_name, description)
    
    dataset_id = result.get('data', {}).get('id')
    print(f"Successfully created dataset: {dataset_id}")
    return result