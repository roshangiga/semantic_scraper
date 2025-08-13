#!/usr/bin/env python3
"""
Sequential semantic processor that processes files one by one using a threaded queue.
Supports parallel task addition while processing sequentially.
"""

import os
import sys
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
from queue import Queue, Empty
from dataclasses import dataclass


@dataclass
class ChunkingTask:
    """A single chunking task."""
    markdown_file_path: str
    semantic_output_path: str
    source_url: str


class SequentialSemanticProcessor:
    """Sequential processor for semantic chunking using a threaded queue."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the sequential processor."""
        self.config = config
        self.chunking_config = config.get('contextual_chunking', {})
        
        # Initialize task queue and tracking
        self.task_queue = Queue()
        self.completed_tasks = []
        self.failed_tasks = []
        self.total_added = 0
        
        # Threading control
        self.worker_thread = None
        self.stop_worker_event = threading.Event()
        self.worker_lock = threading.Lock()
        self.is_running = False
        
        # Provider: 'openai' | 'azure' | 'gemini'
        self.provider = self.chunking_config.get('provider', 'openai').lower()
        
        if self.provider == 'openai':
            # OpenAI
            self.api_key = os.getenv('OPENAI_API_KEY')
            self.model_name = (
                self.chunking_config.get('openai_model')
                or self.chunking_config.get('model')
                or 'gpt-4o-mini'
            )
            self.azure_endpoint = None
            self.azure_api_version = None
            self.azure_deployment = None
        elif self.provider == 'azure':
            # Azure OpenAI
            self.api_key = os.getenv('OPENAI_API_KEY')
            # Model for Azure is typically the deployment name
            self.model_name = (
                self.chunking_config.get('azure_model')
                or self.chunking_config.get('model')
                or self.chunking_config.get('azure_deployment')
                or 'gpt-4o-mini'
            )
            # Allow config or environment for Azure settings
            self.azure_endpoint = (
                self.chunking_config.get('azure_endpoint')
                or os.getenv('AZURE_OPENAI_ENDPOINT')
            )
            self.azure_api_version = (
                self.chunking_config.get('azure_api_version')
                or os.getenv('AZURE_OPENAI_API_VERSION')
            )
            self.azure_deployment = (
                self.chunking_config.get('azure_deployment')
                or os.getenv('AZURE_OPENAI_DEPLOYMENT')
                or os.getenv('AZURE_OPENAI_MODEL')
            )
        elif self.provider == 'gemini':
            # Gemini
            self.api_key = os.getenv('GEMINI_API_KEY')
            self.model_name = (
                self.chunking_config.get('gemini_model')
                or self.chunking_config.get('model')
                or 'gemini-2.5-flash'
            )
            self.azure_endpoint = None
            self.azure_api_version = None
            self.azure_deployment = None
        else:
            # Unknown provider: disable semantic chunking until fixed
            print(f"   ❌ Unknown semantic provider: '{self.provider}'. Use one of: openai | azure | gemini")
            self.api_key = None
            self.model_name = None
            self.azure_endpoint = None
            self.azure_api_version = None
            self.azure_deployment = None
    
    def is_enabled(self) -> bool:
        """Check if semantic chunking is enabled."""
        return self.chunking_config.get('enabled', False)
    
    def start_worker(self):
        """Start the worker thread for processing tasks."""
        if not self.is_enabled():
            return False
            
        if not self.api_key:
            if self.provider in ('openai', 'azure'):
                print(f"   ⚠️ Semantic chunking disabled: OPENAI_API_KEY not found")
            else:
                print(f"   ⚠️ Semantic chunking disabled: GEMINI_API_KEY not found")
            return False
        
        with self.worker_lock:
            if not self.is_running:
                self.stop_worker_event.clear()
                self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
                self.worker_thread.start()
                self.is_running = True
                # Worker started - logging handled by orchestrator
                return True
        return False
    
    def stop_worker(self):
        """Stop the worker thread and wait for completion."""
        with self.worker_lock:
            if self.is_running:
                print(f"   🛑 Stopping semantic chunking worker...")
                self.stop_worker_event.set()
                
                # Add sentinel task to wake up worker
                self.task_queue.put(None)
                
                if self.worker_thread:
                    self.worker_thread.join(timeout=30)
                    if self.worker_thread.is_alive():
                        print(f"   ⚠️ Worker thread did not stop gracefully")
                
                self.is_running = False
                print(f"   ✅ Semantic chunking worker stopped")
    
    def _worker_loop(self):
        """Main worker loop that processes tasks sequentially."""
        # Worker loop started - suppressed for clean progress bar
        
        while not self.stop_worker_event.is_set():
            try:
                # Wait for a task with timeout
                task = self.task_queue.get(timeout=1.0)
                
                # Check for sentinel value (stop signal)
                if task is None:
                    self.task_queue.task_done()
                    break
                
                # Process the task
                self._process_single_task_thread_safe(task)
                self.task_queue.task_done()
                
            except Empty:
                # Timeout occurred, check stop condition
                continue
            except Exception as e:
                print(f"   ⚠️ Error in worker loop: {e}")
                
        print(f"   🏁 Semantic chunking worker loop ended")
    
    def _process_single_task_thread_safe(self, task: ChunkingTask):
        """Process a single task in a thread-safe manner."""
        try:
            completed_count = len(self.completed_tasks)
            total_processed = completed_count + len(self.failed_tasks)
            
            # Processing status message suppressed for clean progress bar
            
            success = self.process_single_task(task)
            
            with self.worker_lock:
                if success:
                    self.completed_tasks.append(task)
                else:
                    self.failed_tasks.append(task)
            
            # Send completion message
            completed_count = len(self.completed_tasks)
            failed_count = len(self.failed_tasks)
            remaining = self.task_queue.qsize()
            
            if success:
                print(f"   ✅ Completed ({completed_count + failed_count}/{self.total_added}): {Path(task.markdown_file_path).name}")
            
            if remaining == 0 and (completed_count + failed_count) == self.total_added:
                print(f"   🎉 All semantic chunking tasks completed! ✅ {completed_count} | ❌ {failed_count}")
                
        except Exception as e:
            print(f"   ⚠️ Error processing task {Path(task.markdown_file_path).name}: {e}")
            with self.worker_lock:
                self.failed_tasks.append(task)
    
    def add_task(
        self, 
        markdown_file_path: str, 
        semantic_output_path: str, 
        source_url: str
    ) -> bool:
        """
        Add a task to the processing queue (thread-safe).
        
        Args:
            markdown_file_path: Path to the markdown file
            semantic_output_path: Path for semantic output
            source_url: Source URL
            
        Returns:
            True if task was added successfully
        """
        if not self.is_enabled():
            return False
        
        # Start worker if not already running
        if not self.is_running:
            self.start_worker()
        
        if not self.is_running:  # Failed to start
            return False
        
        task = ChunkingTask(markdown_file_path, semantic_output_path, source_url)
        self.task_queue.put(task)
        
        with self.worker_lock:
            self.total_added += 1
        
        # Semantic queue logging handled by orchestrator for clean progress bar
        return True
    
    def process_single_task(self, task: ChunkingTask) -> bool:
        """
        Process a single chunking task.
        
        Args:
            task: The chunking task to process
            
        Returns:
            True if successful, False otherwise
        """
        # Use the unified script with provider argument
        script_path = Path(__file__).parent / "process_single_file.py"
        
        if self.provider == "openai":
            default_model = "gpt-4o-mini"
        else:
            default_model = "gemini-2.5-flash"
        
        model_name = self.model_name or default_model
        
        # Build command
        # Pass provider directly so the script can distinguish 'openai' vs 'azure'
        provider_for_script = self.provider

        cmd = [
            sys.executable,
            str(script_path),
            "--input", task.markdown_file_path,
            "--output", task.semantic_output_path,
            "--source-url", task.source_url,
            "--provider", provider_for_script,
            "--model", model_name
        ]
        
        # Add Azure OpenAI parameters when provider is 'azure'
        if self.provider == "azure":
            # Validate minimal Azure settings and warn if missing
            missing = []
            if not self.azure_endpoint:
                missing.append("azure_endpoint (or AZURE_OPENAI_ENDPOINT)")
            if not self.azure_api_version:
                missing.append("azure_api_version (or AZURE_OPENAI_API_VERSION)")
            if not self.azure_deployment:
                missing.append("azure_deployment (or AZURE_OPENAI_DEPLOYMENT)")
            if missing:
                print("   ❌ Azure provider selected but missing settings: " + ", ".join(missing))
                return False
            if self.azure_endpoint:
                cmd.extend(["--azure-endpoint", self.azure_endpoint])
            if self.azure_api_version:
                cmd.extend(["--azure-api-version", self.azure_api_version])
            if self.azure_deployment:
                cmd.extend(["--azure-deployment", self.azure_deployment])
        
        # Set up environment
        env = os.environ.copy()
        if self.api_key:
            if self.provider in ("openai", "azure"):
                env['OPENAI_API_KEY'] = self.api_key
            elif self.provider == "gemini":
                env['GEMINI_API_KEY'] = self.api_key
        
        # Run process and wait for completion
        try:
            # Processing message suppressed for clean progress bar
            
            process = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                env=env,
                timeout=300  # 5 minute timeout per task
            )
            
            if process.returncode == 0:
                # Suppress subprocess output - let orchestrator handle display
                self.completed_tasks.append(task)
                # Return success info for orchestrator to display
                task.success_info = {
                    'stdout': process.stdout.strip() if process.stdout else '',
                    'completed': len(self.completed_tasks),
                    'total': self.total_added
                }
                return True
            else:
                # Suppress subprocess output - let orchestrator handle display
                self.failed_tasks.append(task)
                # Return error info for orchestrator to display
                task.error_info = {
                    'stderr': process.stderr.strip() if process.stderr else 'Unknown error',
                    'failed': len(self.failed_tasks),
                    'total': self.total_added
                }
                return False
                
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Semantic chunking timeout: {Path(task.markdown_file_path).name}")
            self.failed_tasks.append(task)
            return False
        except Exception as e:
            print(f"   ⚠️ Failed to process semantic chunking: {e}")
            self.failed_tasks.append(task)
            return False
    
    def wait_for_completion(self, timeout: Optional[float] = None) -> Dict[str, int]:
        """
        Wait for all queued tasks to complete.
        
        Args:
            timeout: Maximum time to wait in seconds (None for no timeout)
            
        Returns:
            Dictionary with processing statistics
        """
        if not self.is_enabled() or not self.is_running:
            return {"processed": 0, "completed": len(self.completed_tasks), "failed": len(self.failed_tasks)}
        
        if self.total_added == 0:
            print(f"   ℹ️ No semantic chunking tasks were queued")
            return {"processed": 0, "completed": 0, "failed": 0}
        
        print(f"   ⏳ Waiting for {self.total_added} semantic chunking tasks to complete...")
        
        start_time = time.time()
        last_status_time = start_time
        
        try:
            while self.is_running:
                completed_count = len(self.completed_tasks)
                failed_count = len(self.failed_tasks)
                total_processed = completed_count + failed_count
                remaining = self.task_queue.qsize()
                
                # Check if all tasks are done
                if total_processed >= self.total_added and remaining == 0:
                    break
                
                # Print status every 30 seconds
                current_time = time.time()
                if current_time - last_status_time > 30:
                    print(f"   📊 Progress: {total_processed}/{self.total_added} (✅ {completed_count} | ❌ {failed_count}) | Queue: {remaining}")
                    last_status_time = current_time
                
                # Check timeout
                if timeout and (current_time - start_time) > timeout:
                    print(f"   ⏰ Timeout reached waiting for semantic chunking completion")
                    break
                
                time.sleep(1)
                
        except KeyboardInterrupt:
            print(f"   ⚠️ Interrupted while waiting for semantic chunking")
        
        # Final statistics
        completed_count = len(self.completed_tasks)
        failed_count = len(self.failed_tasks)
        total_processed = completed_count + failed_count
        
        return {"processed": total_processed, "completed": completed_count, "failed": failed_count}
    
    def wait_and_stop(self, timeout: Optional[float] = 300) -> Dict[str, int]:
        """
        Wait for completion and then stop the worker.
        This is the main method to call at the end of crawling.
        
        Args:
            timeout: Maximum time to wait in seconds (default: 5 minutes)
            
        Returns:
            Dictionary with processing statistics
        """
        try:
            stats = self.wait_for_completion(timeout)
            return stats
        finally:
            self.stop_worker()
    
    def get_queue_size(self) -> int:
        """Get the current queue size."""
        return self.task_queue.qsize()
    
    def get_status(self) -> Dict[str, Any]:
        """Get current processing status."""
        with self.worker_lock:
            return {
                "is_running": self.is_running,
                "total_added": self.total_added,
                "queue_size": self.task_queue.qsize(),
                "completed": len(self.completed_tasks),
                "failed": len(self.failed_tasks),
                "pending": self.total_added - len(self.completed_tasks) - len(self.failed_tasks)
            }
    
    def clear_queue(self):
        """Clear all tasks from the queue."""
        while not self.task_queue.empty():
            try:
                self.task_queue.get_nowait()
                self.task_queue.task_done()
            except Empty:
                break
    
    def get_semantic_output_path(self, source_file_path: str) -> str:
        """
        Get the semantic output path for a given source file.
        
        Args:
            source_file_path: Path to the source markdown file
            
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