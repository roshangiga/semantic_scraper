#!/usr/bin/env python3
"""
Standalone semantic worker process.
Reads tasks from a file queue and processes them independently.
"""

import os
import sys
import time
import json
import subprocess
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich import box

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Import after path modification
from semantic.cost_tracker import CostTracker

console = Console()

# Load environment variables
load_dotenv()

QUEUE_DIR = Path("semantic_queue")
TASK_DIR = QUEUE_DIR / "tasks"
RESULT_DIR = QUEUE_DIR / "results"
WORKER_LOCK = QUEUE_DIR / "worker.lock"


def check_stale_lock():
    """Check for stale worker lock and remove if process is not running."""
    if WORKER_LOCK.exists():
        try:
            with open(WORKER_LOCK, 'r') as f:
                old_pid = f.read().strip()

            # Check if process is still running (Windows)
            import subprocess
            try:
                result = subprocess.run(['tasklist', '/FI', f'PID eq {old_pid}'],
                                        capture_output=True, text=True, check=False)
                if old_pid not in result.stdout:
                    # Process not running, remove stale lock
                    WORKER_LOCK.unlink()
                    print(f"🔓 Removed stale worker lock (PID {old_pid} not running)")
                else:
                    print(f"❌ Another worker is already running (PID {old_pid})")
                    return False
            except Exception:
                # If we can't check, assume it's stale and remove
                WORKER_LOCK.unlink()
                print(f"🔓 Removed stale worker lock (could not verify PID {old_pid})")
        except Exception as e:
            print(f"⚠️ Could not check worker lock: {e}")
            try:
                WORKER_LOCK.unlink()
                print("🔓 Removed problematic worker lock")
            except Exception:
                pass
    return True


def setup_directories():
    """Setup queue directories and cleanup logs if no checkpoint exists."""
    QUEUE_DIR.mkdir(exist_ok=True)
    TASK_DIR.mkdir(exist_ok=True)
    RESULT_DIR.mkdir(exist_ok=True)

    # Check if checkpoint exists - if not, delete cost and error logs for fresh start
    checkpoint_file = Path("crawler_checkpoint.json")
    if not checkpoint_file.exists():
        # Delete cost log
        cost_log_path = QUEUE_DIR / "cost_log.txt"
        if cost_log_path.exists():
            try:
                cost_log_path.unlink()
                print("🗑️ Cleared cost log for fresh start")
            except Exception as e:
                print(f"Warning: Could not delete cost log: {e}")

        # Delete error log
        error_log_path = QUEUE_DIR / "error_log.txt"
        if error_log_path.exists():
            try:
                error_log_path.unlink()
                print("🗑️ Cleared error log for fresh start")
            except Exception as e:
                print(f"Warning: Could not delete error log: {e}")


def get_provider_config():
    """Get provider configuration from config file."""
    try:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)

        chunking_config = config.get('contextual_chunking', {})
        provider = chunking_config.get('provider', 'gemini').lower()

        if provider == 'gemini':
            api_key = os.getenv('GEMINI_API_KEY')
            model_name = chunking_config.get('gemini_model', 'gemini-2.5-flash')
        elif provider == 'openai':
            api_key = os.getenv('OPENAI_API_KEY')
            model_name = chunking_config.get('openai_model', 'gpt-4o-mini')
        elif provider == 'spacy':
            api_key = 'not_required'
            model_name = chunking_config.get('spacy_model', 'en_core_web_trf')
        else:
            api_key = None
            model_name = None

        return provider, model_name, api_key
    except Exception as e:
        print(f"Error loading config: {e}")
        return 'gemini', 'gemini-2.5-flash', os.getenv('GEMINI_API_KEY')


def process_task(task_data, cost_tracker=None):
    """Process a single semantic task."""
    provider, model_name, api_key = get_provider_config()

    try:
        script_path = Path("src/semantic/process_single_file.py")
        cmd = [
            sys.executable, str(script_path),
            "--input", task_data['markdown_file_path'],
            "--output", task_data['semantic_output_path'],
            "--source-url", task_data['source_url'],
            "--provider", provider,
            "--model", model_name
        ]

        env = os.environ.copy()
        if api_key and provider == 'gemini':
            env['GEMINI_API_KEY'] = api_key
        elif api_key and provider == 'openai':
            env['OPENAI_API_KEY'] = api_key

        # Read input file for cost tracking
        input_text = ""
        if cost_tracker and cost_tracker.enabled:
            try:
                with open(task_data['markdown_file_path'], 'r', encoding='utf-8') as f:
                    input_text = f.read()
            except Exception:
                pass

        process = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            timeout=180
        )

        # Extract chunk count from stderr if successful
        chunk_count = 0
        if process.returncode == 0 and process.stderr:
            # Look for "SUCCESS: Generated X semantic chunks" in stderr
            import re
            chunk_match = re.search(r'SUCCESS: Generated (\d+) semantic chunks', process.stderr)
            if chunk_match:
                chunk_count = int(chunk_match.group(1))

        # Track costs if successful and cost tracking enabled
        if (process.returncode == 0 and cost_tracker and cost_tracker.enabled and input_text):
            try:
                # Read output file to get generated content
                output_text = ""
                if os.path.exists(task_data['semantic_output_path']):
                    with open(task_data['semantic_output_path'], 'r', encoding='utf-8') as f:
                        output_data = json.load(f)
                        # Estimate output size from chunks
                        if 'chunks' in output_data:
                            output_text = json.dumps(output_data['chunks'])

                # Log the usage (use free tier for gemini by default)
                tier = "free_tier" if provider == "gemini" else "paid_tier"
                print(f"[DEBUG] Tracking cost: provider={provider}, chunks={chunk_count}, input_len={len(input_text)}")
                cost_tracker.log_usage(
                    provider=provider,
                    model=model_name,
                    input_text=input_text,
                    output_text=output_text,
                    source_file=os.path.basename(task_data['markdown_file_path']),
                    tier=tier
                )
            except Exception as e:
                print(f"Warning: Could not track costs: {e}")
        elif process.returncode == 0 and cost_tracker and cost_tracker.enabled:
            print(f"[DEBUG] Cost tracking skipped: input_text={len(input_text) if input_text else 0} chars")

        return {
            "success": process.returncode == 0,
            "task_id": task_data['task_id'],
            "file_path": task_data['semantic_output_path'],
            "source_file": task_data['markdown_file_path'],
            "chunk_count": chunk_count,
            "error": process.stderr if process.returncode != 0 else None
        }

    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "task_id": task_data['task_id'],
            "file_path": task_data['semantic_output_path'],
            "source_file": task_data['markdown_file_path'],
            "chunk_count": 0,
            "error": "Task timeout after 3 minutes"
        }
    except Exception as e:
        return {
            "success": False,
            "task_id": task_data['task_id'],
            "file_path": task_data['semantic_output_path'],
            "source_file": task_data['markdown_file_path'],
            "chunk_count": 0,
            "error": str(e)
        }


def create_worker_layout(stats, current_processing="", queue_size=0, done_count=0, chunks_created=0, recent_logs=None, start_time=None, completed_tasks=None, pending_tasks=None, cost_tracker=None):
    """Create comprehensive worker layout with Rich components."""
    if recent_logs is None:
        recent_logs = []
    if completed_tasks is None:
        completed_tasks = []
    if pending_tasks is None:
        pending_tasks = []

    # Combined progress bar and stats in same panel
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]📊 Processing Queue"),
        BarColumn(bar_width=30, style="blue", complete_style="green"),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        expand=False
    )

    # Use stats total instead of calculating done_count + queue_size
    total_work = stats.get("total", done_count + queue_size)
    if total_work > 0:
        progress.add_task("queue", total=total_work, completed=done_count)
    else:
        progress.add_task("queue", total=1, completed=0)

    # Stats line with cost information
    elapsed = time.time() - (start_time or time.time()) if start_time else 1
    completed_rate = f"{done_count/elapsed*60:.1f}/min" if elapsed > 0 and done_count > 0 else "0/min"
    chunk_rate = f"{chunks_created/elapsed*60:.1f}/min" if elapsed > 0 and chunks_created > 0 else "0/min"

    # Format elapsed time
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = int(elapsed % 60)
    if hours > 0:
        elapsed_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        elapsed_str = f"{minutes:02d}:{seconds:02d}"

    # Get cost information
    cost_info = ""
    if cost_tracker and cost_tracker.enabled:
        costs = cost_tracker.get_total_costs()
        total_cost = costs.get('total_cost_usd', 0.0)
        total_calls = costs.get('total_entries', 0)
        if total_calls > 0 or total_cost > 0:
            cost_info = f" • 💰 [yellow]${total_cost:.2f}[/yellow] USD ({total_calls} calls)"

    stats_line = f"⏱️ [cyan]{elapsed_str}[/cyan] • ✅ [green]{done_count}[/green] completed ({completed_rate}) • 🧩 [blue]{chunks_created}[/blue] chunks ({chunk_rate}) • ❌ [red]{stats.get('failed', 0)}[/red] failed{cost_info}"

    # Combine progress and stats
    progress_and_stats = Group(progress, "", stats_line)

    # Completed tasks table (left side) - show total count but display recent 10
    total_completed = stats.get('completed', len(completed_tasks))
    completed_table = Table(title=f"✅ Completed Tasks ({total_completed})", box=box.ROUNDED, header_style="bold green")
    completed_table.add_column("File", style="white", min_width=20)
    completed_table.add_column("Time", style="dim", min_width=8)
    completed_table.add_column("Chunks", justify="right", style="blue", min_width=8)

    # Show last 10 completed tasks
    tasks_to_show = completed_tasks[-10:] if len(completed_tasks) > 10 else completed_tasks
    for task in tasks_to_show:
        completed_table.add_row(
            task.get('name', 'Unknown'),
            task.get('time', 'N/A'),
            str(task.get('chunks', 0))
        )

    # If showing fewer than total, add a note
    if len(completed_tasks) > 10:
        completed_table.add_row(f"[dim]... and {len(completed_tasks) - 10} more[/dim]", "", "")

    if not completed_tasks:
        completed_table.add_row("[dim]No completed tasks yet[/dim]", "", "")

    # Pending tasks table (right side)
    pending_count = queue_size + (1 if current_processing else 0)
    pending_table = Table(title=f"📋 Pending Tasks ({pending_count})", box=box.ROUNDED, header_style="bold yellow")
    pending_table.add_column("File", style="white", min_width=20)
    pending_table.add_column("Added", style="dim", min_width=8)
    pending_table.add_column("Status", style="cyan", min_width=12)

    # Add currently processing file with spinner
    if current_processing:
        spinner = "🔄"
        pending_table.add_row(
            f"[bold yellow]{current_processing}[/bold yellow]",
            "now",
            f"[bold yellow]{spinner} Processing...[/bold yellow]"
        )

    # Add other pending tasks
    for task in pending_tasks[:15]:  # Show up to 15 pending tasks
        if task.get('name') != current_processing:  # Don't duplicate current processing
            pending_table.add_row(
                task.get('name', 'Unknown'),
                task.get('added_time', 'N/A'),
                "[dim]Waiting...[/dim]"
            )

    if not pending_tasks and not current_processing:
        pending_table.add_row("[dim]No pending tasks[/dim]", "", "")

    # Create simplified layout - just progress+stats and tasks
    layout = Layout()
    layout.split_column(
        Layout(Panel(progress_and_stats, title="[bold]📈 Progress & Statistics", border_style="blue"), size=7),
        Layout(name="tasks", minimum_size=20)
    )

    # Split tasks section into completed (left) and pending (right)
    layout["tasks"].split_row(
        Layout(Panel(completed_table, border_style="green")),
        Layout(Panel(pending_table, border_style="yellow"))
    )

    return layout


def log_error(filename, error_msg, task_data=None):
    """Log error details to error_log.txt file."""
    import datetime

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"\n[{timestamp}] FAILED: {filename}\n"
    log_entry += f"Error: {error_msg}\n"

    # Add explanation for file not found errors
    if "Input file not found" in error_msg:
        log_entry += "Reason: The markdown file was not created by the crawler (likely crawling failed)\n"
        log_entry += "Solution: Check if the website was accessible and crawler succeeded\n"

    if task_data:
        log_entry += f"Source URL: {task_data.get('source_url', 'Unknown')}\n"
        log_entry += f"Input file: {task_data.get('markdown_file_path', 'Unknown')}\n"
        log_entry += f"Output file: {task_data.get('semantic_output_path', 'Unknown')}\n"

    log_entry += "-" * 80 + "\n"

    # Use semantic_queue folder for error log
    error_log_path = QUEUE_DIR / "error_log.txt"
    try:
        with open(error_log_path, "a", encoding="utf-8") as f:
            f.write(log_entry)
    except Exception as e:
        print(f"Failed to write to error log: {e}")


def load_existing_stats():
    """Load stats from checkpoint."""
    try:
        with open("crawler_checkpoint.json", "r") as f:
            checkpoint = json.load(f)
            completed = checkpoint.get('semantic_completed', 0)
            pending = checkpoint.get('semantic_pending', 0)
            failed = checkpoint.get('semantic_failed', 0)
            chunks = checkpoint.get('semantic_chunks', 0)
    except Exception:
        completed = 0
        pending = 0
        failed = 0
        chunks = 0

    stats = {
        "completed": completed,
        "failed": failed,
        "total": completed + pending + failed,
        "chunks_created": chunks,
        "pending": pending
    }

    return stats, []


def worker_loop():
    """Main worker loop with comprehensive Rich live display."""
    # Show startup banner
    console.print(Panel.fit(
        "[bold green]🧠 Semantic Worker Started[/bold green]\n[dim]Loading existing statistics...[/dim]",
        border_style="green"
    ))

    # Load existing statistics from checkpoint
    stats, _ = load_existing_stats()
    start_time = time.time()

    # Load completed tasks from result files
    def get_completed_tasks_from_results():
        """Get completed task list by scanning result files."""
        completed_tasks = []
        try:
            if not RESULT_DIR.exists():
                return completed_tasks

            for result_file in RESULT_DIR.glob("*.json"):
                try:
                    if not result_file.exists() or not result_file.is_file():
                        continue

                    with open(result_file, 'r', encoding='utf-8') as f:
                        result = json.load(f)

                    if result.get('success', False):
                        # Extract filename and time from result
                        source_file = result.get('source_file', '')
                        filename = os.path.basename(source_file) if source_file else 'Unknown'
                        chunk_count = result.get('chunk_count', 0)

                        # Use file modification time as completion time
                        try:
                            mtime = result_file.stat().st_mtime
                            time_str = time.strftime("%H:%M:%S", time.localtime(mtime))
                            timestamp = mtime
                        except Exception:
                            time_str = "N/A"
                            timestamp = 0

                        completed_tasks.append({
                            'name': filename,
                            'time': time_str,
                            'chunks': chunk_count,
                            'timestamp': timestamp
                        })
                except Exception:
                    continue

            # Sort by completion time (newest first, like checkpoint did)
            return sorted(completed_tasks, key=lambda x: x.get('timestamp', 0), reverse=True)
        except Exception:
            return []

    try:
        completed_tasks = get_completed_tasks_from_results()
    except Exception as e:
        print(f"Error loading completed tasks: {e}")
        completed_tasks = []

    print(f"📊 Loaded stats: {stats['completed']} completed, {stats['failed']} failed, {stats['chunks_created']} chunks")

    # Initialize cost tracker
    try:
        cost_tracker = CostTracker()
        print(f"Cost tracker initialized: enabled={cost_tracker.enabled}")
    except Exception as e:
        print(f"Warning: Could not initialize cost tracker: {e}")
        cost_tracker = None

    # Load any in-progress task if worker crashed
    in_progress_file = QUEUE_DIR / "in_progress.json"

    def get_pending_task_files():
        """Get list of actual pending task files for display only."""
        tasks = []
        try:
            if not TASK_DIR.exists():
                return tasks

            for task_file in TASK_DIR.glob("*.json"):
                try:
                    if not task_file.exists() or not task_file.is_file():
                        continue

                    with open(task_file, 'r', encoding='utf-8') as f:
                        task_data = json.load(f)

                    filename = os.path.basename(task_data.get('markdown_file_path', 'Unknown'))
                    timestamp = task_data.get('timestamp', time.time())
                    added_time = time.strftime("%H:%M", time.localtime(timestamp))

                    tasks.append({
                        'name': filename,
                        'added_time': added_time,
                        'timestamp': timestamp
                    })
                except Exception:
                    continue

            # Sort by timestamp (oldest first)
            return sorted(tasks, key=lambda x: x.get('timestamp', 0))
        except Exception:
            return []

    def get_completed_tasks_from_results():
        """Get completed task list by scanning result files."""
        completed_tasks = []
        try:
            if not RESULT_DIR.exists():
                return completed_tasks

            for result_file in RESULT_DIR.glob("*.json"):
                try:
                    if not result_file.exists() or not result_file.is_file():
                        continue

                    with open(result_file, 'r', encoding='utf-8') as f:
                        result = json.load(f)

                    if result.get('success', False):
                        # Extract filename and time from result
                        source_file = result.get('source_file', '')
                        filename = os.path.basename(source_file) if source_file else 'Unknown'
                        chunk_count = result.get('chunk_count', 0)

                        # Use file modification time as completion time
                        try:
                            mtime = result_file.stat().st_mtime
                            time_str = time.strftime("%H:%M:%S", time.localtime(mtime))
                            timestamp = mtime
                        except Exception:
                            time_str = "N/A"
                            timestamp = 0

                        completed_tasks.append({
                            'name': filename,
                            'time': time_str,
                            'chunks': chunk_count,
                            'timestamp': timestamp
                        })
                except Exception:
                    continue

            # Sort by completion time (newest first, like checkpoint did)
            return sorted(completed_tasks, key=lambda x: x.get('timestamp', 0), reverse=True)
        except Exception:
            return []

    # Main worker loop with Rich Live display
    try:
        initial_layout = create_worker_layout(stats, "", 0, 0, 0, None, start_time, completed_tasks, [], cost_tracker)
    except Exception as e:
        print(f"Error creating initial layout: {e}")
        print("Running without live display...")
        # Run without live display if there's an error
        while True:
            time.sleep(1)
            # Basic polling without display
            task_files = list(TASK_DIR.glob("*.json"))
            if task_files:
                print(f"Processing {len(task_files)} pending tasks...")
            else:
                print("No tasks to process...")
        return

    with Live(
        initial_layout,
        console=console,
        refresh_per_second=3,
        screen=True
    ) as live:

        while True:
            # Check for shutdown or pause signals
            if (QUEUE_DIR / "shutdown").exists():
                break

            if (QUEUE_DIR / "pause").exists():
                while (QUEUE_DIR / "pause").exists():
                    live.update(create_worker_layout(stats, "⏸️ PAUSED", 0, stats["completed"], stats["chunks_created"], None, start_time, completed_tasks, [], cost_tracker))
                    time.sleep(1)
                    if (QUEUE_DIR / "shutdown").exists():
                        return

            # Check if directories still exist (could be cleared by fresh start)
            if not TASK_DIR.exists() or not RESULT_DIR.exists():
                setup_directories()

            # Reload stats from checkpoint and completed tasks from result files
            try:
                with open("crawler_checkpoint.json", "r") as f:
                    checkpoint = json.load(f)
                    stats["pending"] = checkpoint.get('semantic_pending', 0)
                    stats["completed"] = checkpoint.get('semantic_completed', 0)
                    stats["failed"] = checkpoint.get('semantic_failed', 0)
                    stats["chunks_created"] = checkpoint.get('semantic_chunks', 0)
                    stats["total"] = stats["completed"] + stats["pending"] + stats["failed"]
            except Exception:
                pass

            # Reload completed tasks from result files
            try:
                completed_tasks = get_completed_tasks_from_results()
            except Exception:
                completed_tasks = []

            # Use checkpoint pending count but get actual files for display
            queue_size = stats.get("pending", 0)
            pending_task_files = get_pending_task_files()

            current_processing = ""
            if in_progress_file.exists():
                try:
                    with open(in_progress_file, 'r') as f:
                        task = json.load(f)
                    current_processing = os.path.basename(task['markdown_file_path'])
                except Exception:
                    pass

            # Update display
            live.update(create_worker_layout(stats, current_processing, queue_size, stats["completed"], stats["chunks_created"], None, start_time, completed_tasks, pending_task_files, cost_tracker))

            if queue_size == 0:
                time.sleep(0.5)  # Wait for tasks
                continue

            # Process oldest task first
            task_files = list(TASK_DIR.glob("*.json"))
            if not task_files:
                continue

            task_file = sorted(task_files)[0]

            try:
                # Read task
                with open(task_file, 'r') as f:
                    task_data = json.load(f)

                filename = os.path.basename(task_data['markdown_file_path'])

                # Check if input file exists before processing
                input_file = Path(task_data['markdown_file_path'])
                if not input_file.exists():
                    # Input file doesn't exist, skip this task
                    result = {
                        "success": False,
                        "task_id": task_data['task_id'],
                        "file_path": task_data['semantic_output_path'],
                        "source_file": task_data['markdown_file_path'],
                        "chunk_count": 0,
                        "error": f"Input file not found: {task_data['markdown_file_path']}"
                    }
                else:
                    # Mark as in progress (for crash recovery)
                    with open(in_progress_file, 'w') as f:
                        json.dump(task_data, f)

                    # Update display to show current processing
                    live.update(create_worker_layout(stats, filename, queue_size - 1, stats["completed"], stats["chunks_created"], None, start_time, completed_tasks, pending_task_files, cost_tracker))

                    # Process task
                    result = process_task(task_data, cost_tracker)

                # Write result
                result_file = RESULT_DIR / f"{task_data['task_id']}.json"
                with open(result_file, 'w') as f:
                    json.dump(result, f)

                # Remove task file and in-progress marker
                task_file.unlink()
                if in_progress_file.exists():
                    in_progress_file.unlink()

                if result['success']:
                    chunk_count = result.get('chunk_count', 0)

                    # Update checkpoint - increment completed, decrement pending, track chunks, save task details
                    try:
                        with open("crawler_checkpoint.json", "r") as f:
                            checkpoint = json.load(f)
                        checkpoint["semantic_completed"] = checkpoint.get("semantic_completed", 0) + 1
                        checkpoint["semantic_pending"] = max(0, checkpoint.get("semantic_pending", 0) - 1)
                        checkpoint["semantic_chunks"] = checkpoint.get("semantic_chunks", 0) + chunk_count

                        with open("crawler_checkpoint.json", "w") as f:
                            json.dump(checkpoint, f, indent=2)
                    except Exception as e:
                        print(f"Warning: Could not update checkpoint: {e}")

                else:

                    # Update checkpoint - decrement pending, increment failed
                    try:
                        with open("crawler_checkpoint.json", "r") as f:
                            checkpoint = json.load(f)
                        checkpoint["semantic_pending"] = max(0, checkpoint.get("semantic_pending", 0) - 1)
                        checkpoint["semantic_failed"] = checkpoint.get("semantic_failed", 0) + 1

                        with open("crawler_checkpoint.json", "w") as f:
                            json.dump(checkpoint, f, indent=2)
                    except Exception as e:
                        print(f"Warning: Could not update checkpoint: {e}")
                    # Log the error details
                    error_msg = result.get('error', 'Unknown error')
                    log_error(filename, error_msg, task_data)

                # Final update to show completion
                live.update(create_worker_layout(stats, "", queue_size - 1, stats["completed"], stats["chunks_created"], None, start_time, completed_tasks, pending_task_files, cost_tracker))

            except Exception as e:
                error_msg = str(e)
                log_error(filename if 'filename' in locals() else "Unknown file", error_msg, task_data if 'task_data' in locals() else None)

                # Remove problematic task file
                try:
                    task_file.unlink()
                except Exception:
                    pass


if __name__ == "__main__":
    setup_directories()

    # Check for stale lock and exit if another worker is running
    if not check_stale_lock():
        print("Exiting - another worker is already running")
        sys.exit(1)

    # Create worker lock file
    with open(WORKER_LOCK, 'w') as f:
        f.write(str(os.getpid()))

    try:
        worker_loop()
    finally:
        # Remove lock file
        try:
            WORKER_LOCK.unlink()
        except Exception:
            pass
        print("🏁 Semantic worker stopped")
