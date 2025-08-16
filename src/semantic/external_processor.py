#!/usr/bin/env python3
"""
External semantic processor that communicates with a separate worker process.
"""

import os
import sys
import json
import time
import uuid
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable

QUEUE_DIR = Path("semantic_queue")
TASK_DIR = QUEUE_DIR / "tasks"
RESULT_DIR = QUEUE_DIR / "results"
WORKER_LOCK = QUEUE_DIR / "worker.lock"

class ExternalSemanticProcessor:
    """External semantic processor using file-based IPC."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.chunking_config = config.get('contextual_chunking', {})
        
        # Track processed results to avoid duplicate callbacks
        self.processed_results = set()
        
        # Completion callback
        self.completion_callback: Optional[Callable] = None
        
        # Setup directories
        self._setup_directories()
        
        # Start worker process if not running
        self._ensure_worker_running()
    
    def _setup_directories(self):
        """Setup queue directories."""
        import os
        import shutil
        
        # Check if this is a fresh start (no checkpoint)
        if not os.path.exists('crawler_checkpoint.json'):
            # Clear semantic queue folder for fresh start
            if QUEUE_DIR.exists():
                try:
                    shutil.rmtree(QUEUE_DIR)
                    print("[OK] Cleared semantic queue folder for fresh start")
                except Exception as e:
                    print(f"[WARNING] Could not clear semantic queue folder: {e}")
        
        QUEUE_DIR.mkdir(exist_ok=True)
        TASK_DIR.mkdir(exist_ok=True)
        RESULT_DIR.mkdir(exist_ok=True)
    
    def _ensure_worker_running(self):
        """Start worker process if not already running."""
        # Just delete the lock if it exists and start fresh
        if WORKER_LOCK.exists():
            try:
                WORKER_LOCK.unlink()
            except:
                pass
        
        # Start worker process
        worker_script = Path(__file__).parent.parent.parent / "semantic_worker.py"
        subprocess.Popen([
            sys.executable, str(worker_script)
        ], creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0)
        
        # Wait a bit for worker to start
        time.sleep(1)
    
    def is_enabled(self) -> bool:
        """Check if semantic chunking is enabled."""
        return self.chunking_config.get('enabled', False)
    
    def add_task(self, markdown_file_path: str, semantic_output_path: str, source_url: str) -> bool:
        """Add a task to the external worker queue."""
        if not self.is_enabled():
            return False
        
        # Skip if semantic output already exists
        if Path(semantic_output_path).exists():
            return False
        
        # Skip if input markdown file doesn't exist
        if not Path(markdown_file_path).exists():
            return False
        
        # Check if there's already a pending task for this file
        for existing_task_file in TASK_DIR.glob("*.json"):
            try:
                with open(existing_task_file, 'r') as f:
                    existing_task = json.load(f)
                if existing_task.get('semantic_output_path') == semantic_output_path:
                    return False  # Task already exists
            except:
                continue
        
        # Generate unique task ID
        task_id = str(uuid.uuid4())
        
        # Create task data
        task_data = {
            "task_id": task_id,
            "markdown_file_path": markdown_file_path,
            "semantic_output_path": semantic_output_path,
            "source_url": source_url,
            "timestamp": time.time()
        }
        
        # Write task file
        task_file = TASK_DIR / f"{task_id}.json"
        with open(task_file, 'w') as f:
            json.dump(task_data, f)
        
        
        # Update semantic_pending in checkpoint
        try:
            with open("crawler_checkpoint.json", "r") as f:
                checkpoint = json.load(f)
            checkpoint["semantic_pending"] = checkpoint.get("semantic_pending", 0) + 1
            with open("crawler_checkpoint.json", "w") as f:
                json.dump(checkpoint, f, indent=2)
        except:
            pass
        
        return True
    
    def check_completed_tasks(self) -> int:
        """Check for newly completed tasks - simplified to avoid file scanning crashes."""
        # Temporarily disable file scanning to prevent access violations
        # The semantic worker will handle result processing
        return 0
    
    def get_queue_size(self) -> int:
        """Get number of pending tasks from checkpoint."""
        try:
            with open("crawler_checkpoint.json", "r") as f:
                checkpoint = json.load(f)
            return checkpoint.get("semantic_pending", 0)
        except:
            return 0
    
    
    def get_status(self) -> Dict[str, Any]:
        """Get processing status from checkpoint - avoid file scanning to prevent crashes."""
        try:
            with open("crawler_checkpoint.json", "r") as f:
                checkpoint = json.load(f)
                completed = checkpoint.get('semantic_completed', 0)
                pending = checkpoint.get('semantic_pending', 0)
                failed = checkpoint.get('semantic_failed', 0)
        except:
            completed, pending, failed = 0, 0, 0
        
        return {
            "completed": completed,
            "failed": failed,
            "pending": pending,
            "total": completed + pending + failed,
            "chunks": 0,
            "enabled": self.is_enabled()
        }
    
    def set_completion_callback(self, callback: Callable):
        """Set completion callback."""
        self.completion_callback = callback
    
    def get_semantic_output_path(self, markdown_path: str) -> str:
        """Generate semantic output path using configuration."""
        markdown_file = Path(markdown_path)
        
        # Get semantic output directory from config
        file_manager_config = self.config.get('crawler', {}).get('file_manager', {})
        semantic_output_dir = file_manager_config.get('semantic_output_dir', 'crawled_semantic')
        
        # Replace 'crawled_docling' with semantic output dir while maintaining the rest of the path structure
        # Example: output/crawled_docling/20250816_132142/devices.myt.mu/file.md
        # becomes: output/crawled_semantic/20250816_132142/devices.myt.mu/file.json
        path_parts = markdown_file.parts
        
        # Find and replace crawled_docling with semantic directory
        new_parts = []
        for part in path_parts[:-1]:  # Exclude filename
            if 'crawled_docling' in part:
                # Replace the crawled_docling part with semantic output directory
                new_parts.extend(Path(semantic_output_dir).parts)
            else:
                new_parts.append(part)
        
        semantic_dir = Path(*new_parts) if new_parts else Path(semantic_output_dir)
        semantic_dir.mkdir(parents=True, exist_ok=True)
        
        output_filename = markdown_file.stem + ".json"
        return str(semantic_dir / output_filename)
    
    def process_all_remaining(self) -> Dict[str, int]:
        """Wait for all tasks to complete."""
        print("🔄 Waiting for external semantic worker to complete all tasks...")
        
        # Poll for completion - check if pending count reaches 0
        while True:
            self.check_completed_tasks()
            try:
                with open("crawler_checkpoint.json", "r") as f:
                    checkpoint = json.load(f)
                pending = checkpoint.get("semantic_pending", 0)
                if pending == 0:
                    break
            except:
                break
            time.sleep(0.5)
        
        # Don't shutdown worker - let it keep running for next session
        
        try:
            with open("crawler_checkpoint.json", "r") as f:
                checkpoint = json.load(f)
            return {
                "processed": checkpoint.get("semantic_completed", 0) + checkpoint.get("semantic_failed", 0),
                "completed": checkpoint.get("semantic_completed", 0),
                "failed": checkpoint.get("semantic_failed", 0)
            }
        except:
            return {"processed": 0, "completed": 0, "failed": 0}
    
    def cleanup(self):
        """Cleanup external worker."""
        # Signal shutdown
        shutdown_file = QUEUE_DIR / "shutdown"
        shutdown_file.touch()
        
        # Wait a bit
        time.sleep(1)
        
        # Clean up files
        try:
            shutdown_file.unlink()
        except:
            pass