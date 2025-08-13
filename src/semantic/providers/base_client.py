#!/usr/bin/env python3
"""
Base class for semantic chunking LLM clients.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseLLMClient(ABC):
    """Base class for LLM clients used for contextual chunking."""
    
    def __init__(self, api_key: str = None, model_name: str = None):
        """
        Initialize the LLM client.
        
        Args:
            api_key: API key for the LLM service
            model_name: Model name to use
        """
        self.api_key = api_key
        self.model_name = model_name
    
    @abstractmethod
    def process_document_for_chunking(self, document_content: str) -> List[Dict[str, Any]]:
        """
        Process a markdown document for contextual chunking.
        
        Args:
            document_content: The markdown content to process
            
        Returns:
            List of chunks with content and keywords
        """
        pass
    
    def save_chunks_to_markdown(self, chunks: List[Dict[str, Any]], output_path: str, source_url: str = None):
        """
        Save the processed chunks to a markdown file.
        
        Args:
            chunks: List of chunks with content and keywords
            output_path: Path to save the markdown file
            source_url: Optional source URL to include in the header
        """
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                # Write header with source URL if provided
                if source_url:
                    f.write(f"Source: {source_url}\n\n")
                
                # Write each chunk
                for i, chunk in enumerate(chunks, 1):
                    content = chunk.get('content', '')
                    keywords = chunk.get('keywords', [])
                    
                    # Add chunk separator
                    f.write(f"<!-- CHUNK {i} -->\n")
                    f.write(f"<!-- KEYWORDS: {', '.join(keywords)} -->\n\n")
                    
                    # Write chunk content
                    f.write(content)
                    f.write("\n\n")
                    
                    # Add chunk separator
                    f.write(f"<!-- END CHUNK {i} -->\n\n")
            
            import logging
            logging.info(f"💾 Saved {len(chunks)} chunks to {output_path}")
            
        except Exception as e:
            import logging
            logging.error(f"Error saving chunks to {output_path}: {e}")
            raise