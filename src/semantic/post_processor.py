#!/usr/bin/env python3
"""
Post-processor for semantic chunking that runs after docling conversion.
"""

import os
import asyncio
import multiprocessing as mp
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging

from .providers.gemini_client import GeminiClient


def process_file_for_chunking(args):
    """
    Process a single markdown file for semantic chunking.
    This function runs in a separate process.
    
    Args:
        args: Tuple of (file_path, output_path, source_url, api_key, model_name)
        
    Returns:
        Dict with processing results
    """
    file_path, output_path, source_url, api_key, model_name = args
    
    try:
        # Initialize Gemini client in this process
        client = GeminiClient(api_key=api_key, model_name=model_name)
        
        # Read the markdown file with encoding fallback
        encodings = ['utf-8', 'iso-8859-1', 'windows-1252', 'cp1252']
        content = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    content = f.read()
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            # If all encodings fail, use utf-8 with error replacement
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
        
        # Skip if file only contains source header (empty content)
        lines = content.strip().split('\n')
        if len(lines) <= 2 and lines[0].startswith('Source:'):
            return {
                'file_path': file_path,
                'status': 'skipped',
                'reason': 'Empty content (only source header)'
            }
        
        # Process for semantic chunks
        chunks = client.process_document_for_chunking(content)
        
        if chunks:
            # Create chunked content
            chunked_content = []
            for i, chunk in enumerate(chunks, 1):
                chunk_content = chunk.get('content', '')
                keywords = chunk.get('keywords', [])
                
                # Add chunk separator and metadata
                chunked_content.append(f"<!-- CHUNK {i} -->")
                chunked_content.append(f"<!-- KEYWORDS: {', '.join(keywords)} -->")
                chunked_content.append("")
                chunked_content.append(chunk_content)
                chunked_content.append("")
                chunked_content.append(f"<!-- END CHUNK {i} -->")
                chunked_content.append("")
            
            # Combine all chunks
            final_content = '\n'.join(chunked_content)
            
            # Add source header if provided
            if source_url:
                final_content = f"Source: {source_url}\n\n{final_content}"
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save to semantic directory
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(final_content)
            
            return {
                'file_path': file_path,
                'output_path': output_path,
                'status': 'success',
                'chunks_count': len(chunks)
            }
        else:
            return {
                'file_path': file_path,
                'status': 'failed',
                'reason': 'No chunks generated'
            }
            
    except Exception as e:
        return {
            'file_path': file_path,
            'status': 'error',
            'error': str(e)
        }


class SemanticPostProcessor:
    """Post-processor for semantic chunking using multiprocessing."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the semantic post-processor.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.chunking_config = config.get('contextual_chunking', {})
        self.file_config = config.get('crawler', {}).get('file_manager', {})
        
        # Get directories
        self.docling_dir = self.file_config.get('pages_output_dir', 'crawled_docling')
        self.semantic_dir = self.file_config.get('semantic_output_dir', 'crawled_semantic')
        
        # Get API settings
        self.api_key = os.getenv('GEMINI_API_KEY')
        self.model_name = self.chunking_config.get('gemini_model', 'gemini-1.5-pro')
        
    def is_enabled(self) -> bool:
        """Check if semantic chunking is enabled."""
        return self.chunking_config.get('enabled', False)
    
    def find_markdown_files(self) -> List[Path]:
        """
        Find all markdown files in the docling directory.
        
        Returns:
            List of markdown file paths
        """
        docling_path = Path(self.docling_dir)
        if not docling_path.exists():
            return []
        
        markdown_files = []
        for md_file in docling_path.rglob('*.md'):
            markdown_files.append(md_file)
        
        return markdown_files
    
    def get_output_path(self, input_path: Path) -> Path:
        """
        Get the corresponding output path in the semantic directory.
        
        Args:
            input_path: Input markdown file path
            
        Returns:
            Output path in semantic directory
        """
        # Get relative path from docling directory
        docling_path = Path(self.docling_dir)
        relative_path = input_path.relative_to(docling_path)
        
        # Create corresponding path in semantic directory
        semantic_path = Path(self.semantic_dir) / relative_path
        return semantic_path
    
    def extract_source_url(self, file_path: Path) -> Optional[str]:
        """
        Extract source URL from markdown file.
        
        Args:
            file_path: Path to markdown file
            
        Returns:
            Source URL if found, None otherwise
        """
        try:
            # Try multiple encodings for reading the first line
            encodings = ['utf-8', 'iso-8859-1', 'windows-1252', 'cp1252']
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        first_line = f.readline().strip()
                        if first_line.startswith('Source: '):
                            return first_line[8:]  # Remove 'Source: ' prefix
                    break
                except UnicodeDecodeError:
                    continue
        except Exception:
            pass
        return None
    
    async def process_all_files(self, max_workers: int = None) -> Dict[str, Any]:
        """
        Process all markdown files for semantic chunking using multiprocessing.
        
        Args:
            max_workers: Maximum number of worker processes
            
        Returns:
            Processing results
        """
        if not self.is_enabled():
            return {'status': 'disabled'}
        
        if not self.api_key:
            return {'status': 'error', 'error': 'GEMINI_API_KEY not found'}
        
        # Find all markdown files
        markdown_files = self.find_markdown_files()
        
        if not markdown_files:
            return {'status': 'no_files'}
        
        print(f"🧠 Starting semantic chunking post-processing...")
        print(f"   📁 Source directory: {self.docling_dir}")
        print(f"   📁 Output directory: {self.semantic_dir}")
        print(f"   📄 Found {len(markdown_files)} markdown files")
        
        # Prepare arguments for multiprocessing
        process_args = []
        for md_file in markdown_files:
            output_path = self.get_output_path(md_file)
            source_url = self.extract_source_url(md_file)
            
            process_args.append((
                str(md_file),
                str(output_path),
                source_url,
                self.api_key,
                self.model_name
            ))
        
        # Use multiprocessing to process files
        if max_workers is None:
            max_workers = min(4, mp.cpu_count())  # Limit workers to avoid API rate limits
        
        results = {
            'total_files': len(markdown_files),
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'errors': [],
            'success_files': []
        }
        
        print(f"   🔄 Processing with {max_workers} workers...")
        
        # Process files in batches to manage API rate limits
        batch_size = max_workers
        for i in range(0, len(process_args), batch_size):
            batch = process_args[i:i + batch_size]
            
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                future_to_args = {executor.submit(process_file_for_chunking, args): args for args in batch}
                
                for future in as_completed(future_to_args):
                    result = future.result()
                    
                    if result['status'] == 'success':
                        results['processed'] += 1
                        results['success_files'].append(result['output_path'])
                        print(f"   ✅ {Path(result['file_path']).name} → {result['chunks_count']} chunks")
                        
                    elif result['status'] == 'skipped':
                        results['skipped'] += 1
                        print(f"   ⏭️  {Path(result['file_path']).name} (skipped: {result['reason']})")
                        
                    elif result['status'] == 'failed':
                        results['failed'] += 1
                        print(f"   ⚠️  {Path(result['file_path']).name} (failed: {result['reason']})")
                        
                    else:  # error
                        results['failed'] += 1
                        results['errors'].append({
                            'file': result['file_path'],
                            'error': result['error']
                        })
                        print(f"   ❌ {Path(result['file_path']).name} (error: {result['error']})")
            
            # Add a small delay between batches to be nice to the API
            if i + batch_size < len(process_args):
                await asyncio.sleep(1)
        
        print(f"🎉 Semantic chunking post-processing completed!")
        print(f"   ✅ Processed: {results['processed']}")
        print(f"   ⏭️  Skipped: {results['skipped']}")
        print(f"   ❌ Failed: {results['failed']}")
        
        return results


async def run_semantic_post_processing(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run semantic post-processing as a standalone function.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Processing results
    """
    processor = SemanticPostProcessor(config)
    return await processor.process_all_files()