#!/usr/bin/env python3
"""
Single file semantic processor that runs in a separate process.
"""

import os
import sys
import asyncio
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any


def launch_semantic_processing(
    markdown_file_path: str, 
    semantic_output_path: str, 
    source_url: str,
    provider: str = "openai",  # Changed default to OpenAI
    api_key: str = None,
    model_name: str = None,
    azure_endpoint: str = None,
    azure_api_version: str = None,
    azure_deployment: str = None
) -> subprocess.Popen:
    """
    Launch semantic processing for a single file in a separate process.
    
    Args:
        markdown_file_path: Path to the markdown file to process
        semantic_output_path: Path where to save the semantic chunks
        source_url: Source URL for the content
        provider: LLM provider ("openai" or "gemini")
        api_key: API key (optional, will use env var)
        model_name: Model name
        azure_endpoint: Azure OpenAI endpoint URL
        azure_api_version: Azure OpenAI API version
        azure_deployment: Azure OpenAI deployment name
        
    Returns:
        Subprocess.Popen object
    """
    # Use the unified script with provider argument
    script_path = Path(__file__).parent / "process_single_file.py"
    
    if provider.lower() == "openai":
        default_model = "gpt-4.1"
    else:
        default_model = "gemini-2.5-flash"
    
    model_name = model_name or default_model
    
    # Build command
    cmd = [
        sys.executable,
        str(script_path),
        "--input", markdown_file_path,
        "--output", semantic_output_path,
        "--source-url", source_url,
        "--provider", provider.lower(),
        "--model", model_name
    ]
    
    # Add Azure OpenAI parameters
    if provider.lower() == "openai":
        if azure_endpoint:
            cmd.extend(["--azure-endpoint", azure_endpoint])
        if azure_api_version:
            cmd.extend(["--azure-api-version", azure_api_version])
        if azure_deployment:
            cmd.extend(["--azure-deployment", azure_deployment])
    
    # Set up environment
    env = os.environ.copy()
    if api_key:
        if provider.lower() == "openai":
            env['OPENAI_API_KEY'] = api_key  # Use standard OpenAI key
        else:
            env['GEMINI_API_KEY'] = api_key
    
    # Launch process (non-blocking)
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env
        )
        return process
    except Exception as e:
        print(f"   ⚠️ Failed to launch semantic processing: {e}")
        return None


class SingleFileSemanticProcessor:
    """Manager for single-file semantic processing."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the processor."""
        self.config = config
        self.chunking_config = config.get('contextual_chunking', {})
        
        # Get provider and settings
        self.provider = self.chunking_config.get('provider', 'openai').lower()  # Default to OpenAI
        
        if self.provider == 'openai':
            self.api_key = os.getenv('OPENAI_API_KEY')  # Use standard OpenAI key
            self.model_name = self.chunking_config.get('openai_model', 'gpt-4o-mini')
            self.endpoint = None
            self.api_version = None
        else:
            self.api_key = os.getenv('GEMINI_API_KEY')
            self.model_name = self.chunking_config.get('gemini_model', 'gemini-2.5-flash')
            self.endpoint = None
            self.api_version = None
        
        # Track running processes
        self.running_processes = []
    
    def is_enabled(self) -> bool:
        """Check if semantic chunking is enabled."""
        return self.chunking_config.get('enabled', False)
    
    def process_file_async(
        self, 
        markdown_file_path: str, 
        semantic_output_path: str, 
        source_url: str
    ) -> bool:
        """
        Start processing a single file asynchronously.
        
        Args:
            markdown_file_path: Path to the markdown file
            semantic_output_path: Path for semantic output
            source_url: Source URL
            
        Returns:
            True if process was launched successfully
        """
        if not self.is_enabled():
            return False
            
        if not self.api_key:
            if self.provider == 'openai':
                print(f"   ⚠️ Semantic chunking disabled: OPENAI_API_KEY not found")
            else:
                print(f"   ⚠️ Semantic chunking disabled: GEMINI_API_KEY not found")
            return False
        
        # Launch the process
        process = launch_semantic_processing(
            markdown_file_path,
            semantic_output_path, 
            source_url,
            self.provider,
            self.api_key,
            self.model_name,
            self.endpoint,
            self.api_version
        )
        
        if process:
            self.running_processes.append({
                'process': process,
                'input_file': markdown_file_path,
                'output_file': semantic_output_path
            })
            print(f"   🧠 Started semantic chunking process for {Path(markdown_file_path).name}")
            return True
        
        return False
    
    def cleanup_finished_processes(self):
        """Clean up finished processes and report results."""
        finished = []
        
        for proc_info in self.running_processes:
            process = proc_info['process']
            if process.poll() is not None:  # Process has finished
                finished.append(proc_info)
                
                # Get output
                stdout, stderr = process.communicate()
                
                filename = Path(proc_info['input_file']).name
                if process.returncode == 0:
                    # Success case
                    try:
                        from ..console import add_processing_step
                        print(f"✅ Semantic chunking completed: {filename}")
                    except ImportError:
                        print(f"   ✅ Semantic chunking completed: {filename}")
                    if stderr and stderr.strip():
                        # Show processing info from stderr (our enhanced logging)
                        for line in stderr.strip().split('\n'):
                            if line.startswith('SUCCESS:') or line.startswith('PROCESSING:'):
                                print(f"      {line}")
                else:
                    # Failure case - parse stderr for reason
                    try:
                        from ..console import add_processing_step
                        failure_reason = "Unknown error"
                        if stderr and stderr.strip():
                            # Extract the main error reason
                            stderr_lines = stderr.strip().split('\n')
                            for line in stderr_lines:
                                if line.startswith('SKIPPED:'):
                                    failure_reason = line.replace('SKIPPED:', '').strip()
                                    print(f"⚠️ Semantic processing skipped: {filename} - {failure_reason}")
                                    return  # Don't treat skips as failures
                                elif line.startswith('ERROR:'):
                                    failure_reason = line.replace('ERROR:', '').strip()
                                    break
                        print(f"❌ Semantic chunking failed: {filename} - {failure_reason}")
                    except ImportError:
                        print(f"   ❌ Semantic chunking failed: {filename}")
                        if stderr and stderr.strip():
                            print(f"      Error: {stderr.strip()[:200]}...")  # Truncate long errors
        
        # Remove finished processes
        for proc_info in finished:
            self.running_processes.remove(proc_info)
    
    def wait_for_all_processes(self, timeout: Optional[float] = None):
        """
        Wait for all running processes to complete.
        
        Args:
            timeout: Maximum time to wait in seconds
        """
        if not self.running_processes:
            return
            
        print(f"   ⏳ Waiting for {len(self.running_processes)} semantic chunking processes...")
        
        for proc_info in self.running_processes:
            try:
                process = proc_info['process']
                stdout, stderr = process.communicate(timeout=timeout)
                
                filename = Path(proc_info['input_file']).name
                if process.returncode == 0:
                    # Success case
                    try:
                        from ..console import add_processing_step
                        print(f"✅ Semantic chunking completed: {filename}")
                    except ImportError:
                        print(f"   ✅ Semantic chunking completed: {filename}")
                    if stderr and stderr.strip():
                        # Show processing info from stderr (our enhanced logging)
                        for line in stderr.strip().split('\n'):
                            if line.startswith('SUCCESS:') or line.startswith('PROCESSING:'):
                                print(f"      {line}")
                else:
                    # Failure case - parse stderr for reason
                    try:
                        from ..console import add_processing_step
                        failure_reason = "Unknown error"
                        if stderr and stderr.strip():
                            # Extract the main error reason
                            stderr_lines = stderr.strip().split('\n')
                            for line in stderr_lines:
                                if line.startswith('SKIPPED:'):
                                    failure_reason = line.replace('SKIPPED:', '').strip()
                                    print(f"⚠️ Semantic processing skipped: {filename} - {failure_reason}")
                                    continue  # Don't treat skips as failures, but continue processing other files
                                elif line.startswith('ERROR:'):
                                    failure_reason = line.replace('ERROR:', '').strip()
                                    break
                        if not any(line.startswith('SKIPPED:') for line in stderr_lines):
                            print(f"❌ Semantic chunking failed: {filename} - {failure_reason}")
                    except ImportError:
                        print(f"   ❌ Semantic chunking failed: {filename}")
                        if stderr and stderr.strip():
                            print(f"      Error: {stderr.strip()[:200]}...")  # Truncate long errors
                        
            except subprocess.TimeoutExpired:
                print(f"   ⏰ Semantic chunking timeout: {Path(proc_info['input_file']).name}")
                process.kill()
            except Exception as e:
                print(f"   ⚠️ Error waiting for process: {e}")
        
        self.running_processes.clear()
    
    def get_semantic_output_path(self, source_file_path: str) -> str:
        """
        Get the semantic output path for a given source file.
        
        Args:
            source_file_path: Path to the source markdown file (from crawled_docling or crawled_pdf)
            
        Returns:
            Corresponding semantic output path
        """
        file_config = self.config.get('crawler', {}).get('file_manager', {})
        docling_dir = file_config.get('pages_output_dir', 'crawled_docling')
        pdf_dir = file_config.get('pdf_output_dir', 'crawled_pdf')
        semantic_dir = file_config.get('semantic_output_dir', 'crawled_semantic')
        
        source_path = Path(source_file_path)
        
        # Determine which base directory to use
        docling_base = Path(docling_dir)
        pdf_base = Path(pdf_dir)
        
        # Try to get relative path from either base directory
        relative_path = None
        try:
            relative_path = source_path.relative_to(docling_base)
        except ValueError:
            try:
                relative_path = source_path.relative_to(pdf_base)
            except ValueError:
                # If not relative to either, use just the filename
                relative_path = source_path.name
        
        # Create semantic path and change extension to .json
        semantic_path = Path(semantic_dir) / relative_path
        if semantic_path.suffix == '.md':
            semantic_path = semantic_path.with_suffix('.json')
        return str(semantic_path)