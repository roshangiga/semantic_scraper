#!/usr/bin/env python3
"""
Unified semantic progress tracker with persistent file-based state.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime


class SemanticProgressTracker:
    """Centralized semantic progress tracking with file-based persistence."""
    
    def __init__(self, queue_dir: str = "semantic_queue"):
        self.queue_dir = Path(queue_dir)
        self.progress_file = self.queue_dir / "progress_state.json"
        self.queue_dir.mkdir(exist_ok=True)
        
        # Initialize state
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load progress state from file."""
        default_state = {
            "session_start": datetime.now().isoformat(),
            "total_submitted": 0,
            "session_completed": 0,
            "session_failed": 0,
            "last_update": datetime.now().isoformat(),
            "processed_files": {}  # filename -> timestamp mapping
        }
        
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    loaded_state = json.load(f)
                    # Merge with defaults to handle new fields
                    default_state.update(loaded_state)
                    return default_state
            except Exception:
                pass
        
        return default_state
    
    def _save_state(self):
        """Save progress state to file."""
        try:
            self.state["last_update"] = datetime.now().isoformat()
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save progress state: {e}")
    
    def count_existing_completed(self) -> int:
        """Count existing completed semantic files from timestamped directories."""
        try:
            semantic_base_dir = Path("crawled_semantic")
            if semantic_base_dir.exists():
                semantic_files = list(semantic_base_dir.glob("*/*/*.json"))
                return len(semantic_files)
        except Exception:
            pass
        return 0
    
    def count_pending_tasks(self) -> int:
        """Count pending tasks in queue that have valid source files."""
        valid_tasks = 0
        try:
            task_dir = self.queue_dir / "tasks"
            if task_dir.exists():
                for task_file in task_dir.glob("*.json"):
                    try:
                        with open(task_file, 'r', encoding='utf-8') as f:
                            task_data = json.load(f)
                        # Only count tasks where the source markdown file exists
                        source_file = Path(task_data.get('markdown_file_path', ''))
                        if source_file.exists():
                            valid_tasks += 1
                    except Exception:
                        continue
        except Exception:
            pass
        return valid_tasks
    
    def count_total_chunks(self) -> int:
        """Count total chunks created from all completed semantic files."""
        total_chunks = 0
        try:
            semantic_base_dir = Path("crawled_semantic")
            if semantic_base_dir.exists():
                for semantic_file in semantic_base_dir.glob("*/*/*.json"):
                    try:
                        with open(semantic_file, 'r', encoding='utf-8') as f:
                            semantic_data = json.load(f)
                            chunks = semantic_data.get('chunks', [])
                            total_chunks += len(chunks)
                    except:
                        continue
        except Exception:
            pass
        return total_chunks
    
    def get_comprehensive_status(self) -> Dict[str, Any]:
        """Get comprehensive status including all counts."""
        existing_completed = self.count_existing_completed()
        pending_tasks = self.count_pending_tasks()
        total_chunks = self.count_total_chunks()
        
        # Calculate totals the same way as checkpoint logic
        # Total should be completed + pending (not some abstract number)
        total_completed = existing_completed + self.state["session_completed"]
        total_failed = self.state["session_failed"]
        total_known = total_completed + pending_tasks
        
        return {
            # Core counts
            "existing_completed": existing_completed,
            "session_completed": self.state["session_completed"],
            "total_completed": total_completed,
            "session_failed": self.state["session_failed"],
            "pending_tasks": pending_tasks,
            "total_chunks": total_chunks,
            
            # Legacy compatibility - use actual known total, not inflated number
            "completed": total_completed,
            "failed": total_failed,
            "active_tasks": pending_tasks,
            "total_submitted": total_known,  # Use actual known total
            
            # Metadata
            "last_update": self.state["last_update"],
            "session_start": self.state["session_start"]
        }
    
    def mark_task_submitted(self, count: int = 1):
        """Mark tasks as submitted."""
        self.state["total_submitted"] += count
        self._save_state()
    
    def mark_task_completed(self, filename: str = None):
        """Mark a task as completed."""
        self.state["session_completed"] += 1
        if filename:
            self.state["processed_files"][filename] = datetime.now().isoformat()
        self._save_state()
    
    def mark_task_failed(self, filename: str = None):
        """Mark a task as failed."""
        self.state["session_failed"] += 1
        if filename:
            self.state["processed_files"][filename] = datetime.now().isoformat()
        self._save_state()
    
    def reset_session_stats(self):
        """Reset session-specific statistics (for new sessions)."""
        self.state["session_start"] = datetime.now().isoformat()
        self.state["session_completed"] = 0
        self.state["session_failed"] = 0
        self.state["processed_files"] = {}
        self._save_state()
    
    def get_recent_files(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recently processed files with details."""
        recent_files = []
        try:
            semantic_base_dir = Path("crawled_semantic")
            if semantic_base_dir.exists():
                # Get files with modification times
                semantic_files_with_time = []
                for semantic_file in semantic_base_dir.glob("*/*/*.json"):
                    try:
                        mod_time = semantic_file.stat().st_mtime
                        with open(semantic_file, 'r', encoding='utf-8') as f:
                            semantic_data = json.load(f)
                            chunk_count = len(semantic_data.get('chunks', []))
                            
                        semantic_files_with_time.append((semantic_file, chunk_count, mod_time))
                    except:
                        continue
                
                # Sort by modification time (most recent first) and take requested limit
                semantic_files_with_time.sort(key=lambda x: x[2], reverse=True)
                
                for semantic_file, chunk_count, _ in semantic_files_with_time[:limit]:
                    # Clean up filename
                    real_filename = semantic_file.stem
                    real_filename = real_filename.replace('%20', ' ').replace('%2B', '+').replace('&', ' ')
                    if len(real_filename) > 30:
                        real_filename = real_filename[:27] + "..."
                    
                    recent_files.append({
                        'name': real_filename,
                        'time': "restored",
                        'chunks': chunk_count
                    })
        except Exception:
            pass
        
        return recent_files
    
    def cleanup_stale_tasks(self) -> int:
        """Remove task files that reference non-existent markdown files."""
        removed_count = 0
        try:
            task_dir = self.queue_dir / "tasks"
            if task_dir.exists():
                for task_file in task_dir.glob("*.json"):
                    try:
                        with open(task_file, 'r', encoding='utf-8') as f:
                            task_data = json.load(f)
                        
                        # Check if source markdown file exists
                        source_file = Path(task_data.get('markdown_file_path', ''))
                        if not source_file.exists():
                            # Remove stale task
                            task_file.unlink()
                            removed_count += 1
                    except Exception:
                        # Remove corrupted task files too
                        try:
                            task_file.unlink()
                            removed_count += 1
                        except:
                            pass
        except Exception:
            pass
        
        return removed_count