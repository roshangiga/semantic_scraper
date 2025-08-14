"""
Generic RAG uploader interface for managing different RAG clients.
"""

import os
import json
from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod
from pathlib import Path


class RAGClient(ABC):
    """Abstract base class for RAG clients."""
    
    @abstractmethod
    def upload_chunks(self, chunks_data: Dict[str, Any], timestamp: str, domain: str, original_filename: str = None) -> int:
        """
        Upload chunks to the RAG system.
        
        Args:
            chunks_data: Dictionary containing source and chunks
            timestamp: Timestamp for this crawl session
            domain: Domain being crawled
            original_filename: Original filename for the document
            
        Returns:
            Number of chunks successfully uploaded
        """
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """
        Validate client configuration.
        
        Returns:
            True if configuration is valid
        """
        pass


class RAGFlowClient(RAGClient):
    """RAGFlow implementation of RAG client."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize RAGFlow client.
        
        Args:
            config: RAGFlow configuration from config.yaml
        """
        self.config = config
        # Remove hardcoded IDs - we'll generate them automatically
        self.dataset_id = None
        self.document_id = None
        
        # Import the actual RAGFlow client
        from src.rag_clients.ragflow.add_chunk import RAGFlowClient as RFClient
        
        # Get credentials from environment
        api_key = os.getenv("RAGFLOW_API_KEY")
        base_url = os.getenv("RAGFLOW_URL")
        
        if not api_key or not base_url:
            raise ValueError("RAGFLOW_API_KEY and RAGFLOW_URL must be set in environment")
        
        self.client = RFClient(api_key, base_url)
        
        # Set reference to parent uploader for caching
        self.client._parent_uploader = self
    
    def validate_config(self) -> bool:
        """Validate RAGFlow configuration."""
        # No need to validate dataset_id and document_id since we create them automatically
        return True
    
    def upload_chunks(self, chunks_data: Dict[str, Any], timestamp: str, domain: str, original_filename: str = None) -> int:
        """Upload chunks to RAGFlow using file upload (following working demo pattern)."""
        chunks = chunks_data.get('chunks', [])
        
        if not chunks:
            return 0
        
        try:
            # Use the new upload method from add_chunk.py
            from src.rag_clients.ragflow.add_chunk import upload_chunks_from_data
            return upload_chunks_from_data(chunks_data, timestamp, domain, original_filename)
            
        except Exception as e:
            print(f"    ❌ Failed to upload chunks: {e}")
            import traceback
            traceback.print_exc()
            return 0


class DefyClient(RAGClient):
    """Placeholder for future Defy RAG client implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Defy client."""
        self.config = config
        raise NotImplementedError("Defy client is not yet implemented")
    
    def validate_config(self) -> bool:
        """Validate Defy configuration."""
        return False
    
    def upload_chunks(self, chunks_data: Dict[str, Any], timestamp: str, domain: str) -> int:
        """Upload chunks to Defy."""
        raise NotImplementedError("Defy client is not yet implemented")


class RAGUploader:
    """Main RAG uploader that manages different RAG clients."""
    
    # Registry of available RAG clients
    CLIENTS = {
        'ragflow': RAGFlowClient,
        'defy': DefyClient,
        # Add more clients here as they become available
    }
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize RAG uploader.
        
        Args:
            config: RAG upload configuration from config.yaml
        """
        self.config = config
        self.enabled = config.get('enabled', False)
        self.client_name = config.get('client', 'ragflow')
        self.streaming = config.get('streaming', True)
        self.client = None
        
        # Cache for dataset/document IDs per domain to avoid recreating
        self._dataset_cache = {}
        self._document_cache = {}
        # Track uploaded files to prevent duplicates in streaming mode
        self._uploaded_files = set()
        
        if self.enabled:
            self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the selected RAG client."""
        if self.client_name not in self.CLIENTS:
            raise ValueError(f"Unknown RAG client: {self.client_name}. Available: {list(self.CLIENTS.keys())}")
        
        client_config = self.config.get(self.client_name, {})
        client_class = self.CLIENTS[self.client_name]
        
        try:
            self.client = client_class(client_config)
            if not self.client.validate_config():
                print(f"⚠️ RAG client '{self.client_name}' configuration is invalid")
                self.enabled = False
        except NotImplementedError as e:
            print(f"⚠️ RAG client '{self.client_name}' is not yet available: {e}")
            self.enabled = False
        except Exception as e:
            print(f"❌ Failed to initialize RAG client '{self.client_name}': {e}")
            self.enabled = False
    
    def upload_from_file(self, json_path: str, timestamp: str, domain: str) -> int:
        """
        Upload chunks from a semantic JSON file.
        
        Args:
            json_path: Path to semantic JSON file
            timestamp: Timestamp for this crawl session
            domain: Domain being crawled
            
        Returns:
            Number of chunks uploaded
        """
        if not self.enabled or not self.client:
            return 0
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return self.client.upload_chunks(data, timestamp, domain, json_path)
        except Exception as e:
            print(f"❌ Error uploading from {json_path}: {e}")
            return 0
    
    def upload_single_file_streaming(self, json_path: str) -> int:
        """
        Upload chunks from a semantic JSON file immediately (streaming mode).
        
        Args:
            json_path: Path to semantic JSON file
            
        Returns:
            Number of chunks uploaded
        """
        if not self.enabled or not self.client or not self.streaming:
            return 0
        
        # Check if file was already uploaded (prevent duplicates)
        json_path_normalized = os.path.normpath(json_path)
        if json_path_normalized in self._uploaded_files:
            return 0
        
        try:
            # Extract timestamp and domain from path
            # Path format: crawled_semantic/timestamp/domain/file.json
            path_parts = Path(json_path).parts
            if len(path_parts) >= 3:
                timestamp = path_parts[-3]  # timestamp directory
                domain = path_parts[-2]     # domain directory
                
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Pass the json_path as original_filename
                chunks_uploaded = self.client.upload_chunks(data, timestamp, domain, json_path)
                
                # Mark file as uploaded if successful
                if chunks_uploaded > 0:
                    self._uploaded_files.add(json_path_normalized)
                
                return chunks_uploaded
            else:
                print(f"⚠️ Invalid path format for streaming: {json_path}")
                return 0
        except Exception as e:
            print(f"❌ Error streaming from {json_path}: {e}")
            return 0
    
    def upload_from_directory(self, semantic_dir: str) -> int:
        """
        Upload all chunks from a semantic directory.
        
        Args:
            semantic_dir: Path to semantic directory with timestamp
            
        Returns:
            Total number of chunks uploaded
        """
        if not self.enabled or not self.client:
            return 0
        
        total_uploaded = 0
        json_files = []
        
        # Find all JSON files in the directory
        for domain in os.listdir(semantic_dir):
            domain_dir = os.path.join(semantic_dir, domain)
            if os.path.isdir(domain_dir):
                for file in os.listdir(domain_dir):
                    if file.endswith('.json'):
                        json_files.append(os.path.join(domain_dir, file))
        
        if not json_files:
            return 0
        
        print(f"\n📤 Uploading to {self.client_name.upper()} RAG system...")
        print(f"📁 Found {len(json_files)} files to upload")
        
        # Extract timestamp from directory path
        timestamp = os.path.basename(semantic_dir)
        
        for json_file in json_files:
            file_name = os.path.basename(json_file)
            # Extract domain from file path
            domain = os.path.basename(os.path.dirname(json_file))
            
            print(f"  📄 Uploading {file_name} ({domain})...", end=" ")
            
            chunks_uploaded = self.upload_from_file(json_file, timestamp, domain)
            if chunks_uploaded > 0:
                print(f"✅ {chunks_uploaded} chunks")
                total_uploaded += chunks_uploaded
            else:
                print(f"⚠️ skipped")
        
        if total_uploaded > 0:
            print(f"✅ Total uploaded to {self.client_name}: {total_uploaded} chunks")
        
        return total_uploaded
    
    def is_enabled(self) -> bool:
        """Check if RAG upload is enabled and properly configured."""
        return self.enabled and self.client is not None