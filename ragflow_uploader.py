#!/usr/bin/env python3
"""
RAGFlow Upload Console - Feature-Rich Live UI

This console app provides a production-ready, Rich-powered UI for monitoring and selecting
timestamp batches to upload into RAGFlow. It includes:
- Side-by-side layout: ⚙️ Configuration | 📅 Available Timestamps
- Background file monitoring via os.scandir() with thread-safe caching
- Auto-refresh every 5 seconds (only numbers/timestamps update) with visible countdown
- Dynamic header from YAML (🚀 {CLIENT_NAME} Upload Console)
- Status indicator: 💡 Recent activity detected or 🔄 Auto-refreshing
- Claude Code style input panel with bordered UI and ">" cursor
- Windows UTF-8 support and clean shutdown with Ctrl+C
"""

import os
import sys
import json
import yaml
import time
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from contextlib import contextmanager

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import io
    import locale
    try:
        # Try to set console to UTF-8
        import ctypes
        ctypes.windll.kernel32.SetConsoleCP(65001)
        ctypes.windll.kernel32.SetConsoleOutputCP(65001)
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except:
        # Fallback to default encoding with error replacement
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Rich imports for beautiful console output
from rich.console import Console
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.layout import Layout
from rich.live import Live
from rich import box
from rich.align import Align
from rich.columns import Columns
from rich.measure import Measurement
from rich.padding import Padding

# Import existing RAGFlow functionality
from src.rag_clients.rag_uploader import RAGUploader

# Use explicit terminal control to ensure proper Live redraw on Windows; auto width
console = Console(force_terminal=True, legacy_windows=False)

@dataclass
class TimestampStats:
    """Statistics for a timestamp directory."""
    timestamp: str
    readable_date: str
    domain_count: int
    total_files: int
    last_updated: str
    has_recent_activity: bool

    # Not persisted fields used for dynamic age text computation without rescanning FS.
    # latest_modified_epoch is only set internally inside LiveDataManager and used to format
    # "Xs ago" live without rebuilding filesystem state. Keeping it optional for safety.
    latest_modified_epoch: float = 0.0

class LiveDataManager:
    """Manages live data refresh for timestamp statistics."""
    
    def __init__(self, semantic_output_dir: str):
        self.semantic_output_dir = semantic_output_dir
        self._stop_refresh = threading.Event()
        self._refresh_thread = None
        self._data_lock = threading.Lock()
        self._current_stats: List[TimestampStats] = []
        self._last_scan_time = 0.0
        
    def start_refresh(self, callback=None):
        """Start background refresh thread."""
        self._stop_refresh.clear()
        self._refresh_thread = threading.Thread(
            target=self._refresh_worker, 
            args=(callback,),
            daemon=True
        )
        self._refresh_thread.start()
        
    def stop_refresh(self):
        """Stop background refresh thread."""
        if self._refresh_thread:
            self._stop_refresh.set()
            self._refresh_thread.join(timeout=2)
            
    def _refresh_worker(self, callback=None):
        """Background worker for refreshing data."""
        while not self._stop_refresh.is_set():
            try:
                # Refresh every 5 seconds in small sleeps to allow quick stop
                for _ in range(50):  # 5 seconds in 0.1s increments
                    if self._stop_refresh.is_set():
                        return
                    time.sleep(0.1)

                # Update data
                new_stats = self._get_fresh_timestamp_stats()
                with self._data_lock:
                    self._current_stats = new_stats
                    self._last_scan_time = time.time()

                # Notify callback if provided
                if callback:
                    callback(new_stats)

            except Exception:
                # Silent error handling in background thread
                continue
                
    def get_current_stats(self) -> List[TimestampStats]:
        """Get current timestamp statistics."""
        with self._data_lock:
            return self._current_stats.copy()
            
    def get_fresh_stats(self) -> List[TimestampStats]:
        """Get fresh timestamp statistics immediately."""
        stats = self._get_fresh_timestamp_stats()
        with self._data_lock:
            self._current_stats = stats
        return stats

    def wait_for_activity(self, timestamp_dir: str, interval: float = 1.0) -> str:
        """Continuously watch for domain activity.
        Returns 'activity' when detected, or 'continue' if Enter is pressed.
        """
        clear = " " * 60
        def _write_line(s: str):
            sys.stdout.write("\r" + s)
            sys.stdout.flush()
        while True:
            # Check activity
            stats = self.get_domain_stats(timestamp_dir)
            if any(s.get('recently_updated', False) for s in stats.values()):
                _write_line(("💡 Activity detected. Refreshing…").ljust(40))
                sys.stdout.write("\r" + clear + "\r")
                sys.stdout.flush()
                return 'activity'
            # Allow user to continue with Enter
            if sys.platform == 'win32':
                import msvcrt
                t_end = time.time() + interval
                while time.time() < t_end:
                    if msvcrt.kbhit():
                        key = msvcrt.getwch()
                        if key in ('\r', '\n'):
                            _write_line(("✅ Continuing…").ljust(40))
                            sys.stdout.write("\r" + clear + "\r")
                            sys.stdout.flush()
                            return 'continue'
                    time.sleep(0.05)
            else:
                time.sleep(interval)
        # unreachable
        return 'continue'
        
    def _get_fresh_timestamp_stats(self) -> List[TimestampStats]:
        """Get fresh statistics from filesystem."""
        timestamps = self._get_available_timestamps()
        stats = []
        
        for timestamp in timestamps:
            try:
                dt = datetime.strptime(timestamp, '%Y%m%d_%H%M%S')
                readable_date = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                readable_date = "Invalid format"
            
            # Get domain statistics
            domain_stats = self._get_domain_stats(timestamp)
            domain_count = len(domain_stats)
            total_files = sum(stats['files'] for stats in domain_stats.values())
            
            # Calculate last updated time
            latest_modification = 0
            for stats_data in domain_stats.values():
                if stats_data['latest_modified'] > latest_modification:
                    latest_modification = stats_data['latest_modified']
            
            # Format last updated time
            if latest_modification > 0:
                current_time = time.time()
                time_diff = current_time - latest_modification
                
                if time_diff < 60:
                    last_updated = f"{int(time_diff)}s ago"
                    has_recent_activity = time_diff < 5
                elif time_diff < 3600:
                    last_updated = f"{int(time_diff // 60)}m ago"
                    has_recent_activity = False
                elif time_diff < 86400:
                    last_updated = f"{int(time_diff // 3600)}h ago"
                    has_recent_activity = False
                else:
                    last_updated = f"{int(time_diff // 86400)}d ago"
                    has_recent_activity = False
            else:
                last_updated = "Never"
                has_recent_activity = False
            
            stats.append(TimestampStats(
                timestamp=timestamp,
                readable_date=readable_date,
                domain_count=domain_count,
                total_files=total_files,
                last_updated=last_updated,
                has_recent_activity=has_recent_activity,
                latest_modified_epoch=latest_modification
            ))
            
        return stats
        
    def _get_available_timestamps(self) -> List[str]:
        """Get list of available timestamp directories."""
        if not os.path.exists(self.semantic_output_dir):
            return []
        
        timestamps = []
        for item in os.listdir(self.semantic_output_dir):
            item_path = os.path.join(self.semantic_output_dir, item)
            if os.path.isdir(item_path):
                try:
                    datetime.strptime(item, '%Y%m%d_%H%M%S')
                    timestamps.append(item)
                except ValueError:
                    continue
        
        return sorted(timestamps, reverse=True)
        
    def _get_domain_stats(self, timestamp_dir: str) -> Dict[str, Dict]:
        """Get detailed statistics for each domain."""
        timestamp_path = os.path.join(self.semantic_output_dir, timestamp_dir)
        stats = {}
        
        if not os.path.exists(timestamp_path):
            return stats
        
        try:
            with os.scandir(timestamp_path) as entries:
                for entry in entries:
                    if entry.is_dir():
                        domain_name = entry.name
                        
                        json_files = []
                        total_size = 0
                        latest_modified = 0
                        
                        try:
                            with os.scandir(entry.path) as domain_files:
                                for file_entry in domain_files:
                                    if file_entry.is_file() and file_entry.name.endswith('.json'):
                                        try:
                                            file_stat = file_entry.stat()
                                            json_files.append(file_entry.path)
                                            total_size += file_stat.st_size
                                            latest_modified = max(latest_modified, file_stat.st_mtime)
                                        except (OSError, IOError):
                                            continue
                        except (OSError, IOError):
                            continue
                        
                        stats[domain_name] = {
                            'files': len(json_files),
                            'size_mb': round(total_size / (1024 * 1024), 2) if total_size > 0 else 0,
                            'latest_modified': latest_modified,
                            'recently_updated': (time.time() - latest_modified) < 5 if latest_modified > 0 else False
                        }
        except (OSError, IOError):
            pass
        
        return stats

class LiveTimestampTable:
    """Component for creating live-updating timestamp tables."""
    
    def __init__(self):
        self.refresh_time = None
        self._last_render_now = 0.0
        
    def _format_last_updated(self, stat: TimestampStats) -> Tuple[str, bool]:
        """Format "Xs/m/h/d ago" using latest_modified_epoch to avoid FS rescan."""
        if not stat.latest_modified_epoch or stat.latest_modified_epoch <= 0:
            return (stat.last_updated, False)
        now = time.time()
        diff = max(0, now - stat.latest_modified_epoch)
        if diff < 60:
            return (f"{int(diff)}s ago", diff < 5)
        if diff < 3600:
            return (f"{int(diff // 60)}m ago", False)
        if diff < 86400:
            return (f"{int(diff // 3600)}h ago", False)
        return (f"{int(diff // 86400)}d ago", False)
        
    def create_table(self, stats: List[TimestampStats]) -> Table:
        """Create timestamp table with current statistics."""
        self.refresh_time = datetime.now().strftime('%H:%M:%S')
        
        table = Table(
            title=f"📅 Available Timestamps (Updated: {self.refresh_time})", 
            box=box.ROUNDED, 
            expand=True
        )
        table.add_column("#", style="dim", width=3)
        table.add_column("Timestamp", style="cyan")
        table.add_column("Date/Time", style="green")
        table.add_column("Last Updated", style="yellow", justify="center")
        table.add_column("Domains", style="blue", justify="center")
        table.add_column("Total Files", style="magenta", justify="center")
        
        for i, stat in enumerate(stats, 1):
            files_display = f"📄 {stat.total_files}" if stat.total_files > 0 else "📭 0"
            last_updated_text, _recent = self._format_last_updated(stat)
            
            table.add_row(
                str(i),
                stat.timestamp,
                stat.readable_date,
                last_updated_text,
                str(stat.domain_count),
                files_display
            )
        
        return table

class ConfigurationTable:
    """Component for creating static configuration table."""
    
    def __init__(self, config: Dict):
        self.config = config
        
    def create_table(self) -> Table:
        """Create configuration table with simple layout (no vertical edges)."""
        table = Table(
            box=box.HORIZONTALS,
            expand=True,
            show_edge=False,
            pad_edge=False,
            show_header=False,
        )
        table.add_column("Setting", style="cyan", width=20)
        table.add_column("Value", style="green")
        
        # RAG Upload settings
        rag_config = self.config.get('rag_upload', {})
        client = rag_config.get('client', 'ragflow')
        streaming = rag_config.get('streaming', True)
        
        table.add_row("RAG Client", client)
        table.add_row("Streaming Mode", str(streaming))
        
        # Environment variables
        base_url = os.getenv("RAGFLOW_URL")
        table.add_row("RAGFLOW_URL", base_url if base_url else "Not Set")
        
        # Source directory
        semantic_output_dir = rag_config.get('source', 'output/crawled_semantic')
        table.add_row("Source Directory", semantic_output_dir)
        
        return table

class RAGFlowLiveConsole:
    """Main RAGFlow Upload Console with live refresh capability."""
    
    def __init__(self):
        self.config = None
        self.semantic_output_dir = None
        self.rag_uploader = None
        self.data_manager = None
        self.config_table_component = None
        self.timestamp_table_component = None
        self._live: Optional[Live] = None
        self._auto_refresh_seconds = 5
        self._countdown = self._auto_refresh_seconds
        self._running = False
        self._selected_ts: Optional[str] = None

    def _drain_keyboard(self):
        """Drain any pending keypresses (Windows)."""
        if sys.platform == 'win32':
            try:
                import msvcrt
                while msvcrt.kbhit():
                    try:
                        _ = msvcrt.getwch()
                    except Exception:
                        break
            except Exception:
                pass
        
    def load_config(self, config_path: str = "config.yaml") -> bool:
        """Load configuration from YAML file."""
        try:
            if not os.path.exists(config_path):
                console.print(f"[red]❌ Configuration file not found: {config_path}[/red]")
                return False
                
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            # Get semantic output directory from rag_upload config
            rag_config = self.config.get('rag_upload', {})
            self.semantic_output_dir = rag_config.get('source', 'output/crawled_semantic')
            
            # Validate environment variables
            api_key = os.getenv("RAGFLOW_API_KEY")
            base_url = os.getenv("RAGFLOW_URL")
            
            if not api_key:
                raise ValueError("RAGFLOW_API_KEY environment variable is not set.")
            if not base_url:
                raise ValueError("RAGFLOW_URL environment variable is not set.")
            
            # Initialize components
            self.rag_uploader = RAGUploader(rag_config)
            self.data_manager = LiveDataManager(self.semantic_output_dir)
            self.config_table_component = ConfigurationTable(self.config)
            self.timestamp_table_component = LiveTimestampTable()
            
            return True
            
        except Exception as e:
            console.print(f"[red]❌ Error loading configuration: {e}[/red]")
            return False
    
    def build_header(self) -> Panel:
        """Build application header panel (do not print directly)."""
        client_name = "RAGFlow"
        if self.config:
            rag_config = self.config.get('rag_upload', {})
            client_name = rag_config.get('client', 'ragflow').upper()

        header_text = Text()
        header_text.append(f"🚀 {client_name} Upload Console", style="bold cyan")

        panel = Panel(
            Align.center(header_text),
            box=box.DOUBLE,
            border_style="cyan",
            padding=(1, 2)
        )
        return panel
    
    def _build_side_by_side(self, stats: List[TimestampStats]) -> Columns:
        """Build side-by-side columns without clearing the screen."""
        config_table = self.config_table_component.create_table()
        timestamp_table = self.timestamp_table_component.create_table(stats)
        return Columns([config_table, timestamp_table], expand=True, equal=True)

    def _render_status(self, stats: List[TimestampStats]) -> Text:
        has_recent_activity = any(stat.has_recent_activity for stat in stats)
        status = Text()
        if has_recent_activity:
            status.append("💡 Recent activity detected  ", style="yellow")
        else:
            status.append("🔄 Auto-refreshing  ", style="cyan")
        status.append(f"(Auto-refresh in {self._countdown}s)", style="dim")
        return status

    def _render_input_panel(self, buffer: str, choices: List[str]) -> Group:
        """Render a Claude Code style 1x1 input (thin bordered span) inside Live.
        Users can type immediately; Enter submits.
        """
        formatted = [f"[cyan]{c}[/cyan]" for c in choices]
        if len(formatted) > 1:
            choices_text = ",".join(formatted[:-1]) + " or " + formatted[-1]
        else:
            choices_text = formatted[0] if formatted else ""

        prompt = Text.from_markup(
            f"[bold]🕐 Enter timestamp:[/bold]  [dim]Choose from: {choices_text}. (Enter for latest)[/dim]"
        )

        # 1x1 table (span) with thin border, no headers; dim white border, white input text
        input_table = Table(show_header=False, box=box.SQUARE, expand=True, padding=(0,1), border_style="dim white")
        input_table.add_column(justify="left", ratio=1, no_wrap=True)
        input_table.add_row(Text.from_markup(f"[white]> {buffer}█[/white]"))
        hint = Text.from_markup("[dim italic]Ctrl+C to quit[/dim italic]")
        return Group(prompt, input_table, Padding(hint, (0, 0, 1, 0)))

    def _render_domain_input_panel(self, buffer: str, choices: List[str]) -> Group:
        """Render a Claude-style 1x1 input for domain selection inside Live."""
        formatted = [f"[cyan]{c}[/cyan]" for c in choices]
        if len(formatted) > 1:
            choices_text = ",".join(formatted[:-1]) + " or " + formatted[-1]
        else:
            choices_text = formatted[0] if formatted else ""

        prompt = Text.from_markup(
            f"[bold]🌍 Enter domain:[/bold]  [dim]Choose from: {choices_text}. (Enter for all)[/dim]"
        )
        input_table = Table(show_header=False, box=box.SQUARE, expand=True, padding=(0,1), border_style="dim white")
        input_table.add_column(justify="left", ratio=1, no_wrap=True)
        input_table.add_row(Text.from_markup(f"[white]> {buffer}█[/white]"))
        hint = Text.from_markup("[dim italic]Ctrl+C to quit[/dim italic]")
        return Group(prompt, input_table, Padding(hint, (0, 0, 1, 0)))

    def claude_style_prompt(self, message: str, choices: List[str] = None, default: str = None, show_choices: bool = True) -> str:
        """Custom prompt with Claude Code style 1x1 input field and follow-ups (from backup)."""
        # Display the prompt message with choices and default on one line
        prompt_parts = [f"[bold]{message}:[/bold]"]
        if choices and show_choices:
            formatted_choices = [f"[cyan]{choice}[/cyan]" for choice in choices]
            if len(formatted_choices) > 1:
                choices_text = ",".join(formatted_choices[:-1]) + " or " + formatted_choices[-1]
            else:
                choices_text = formatted_choices[0]
            if default:
                if str(default).lower() == "latest":
                    prompt_parts.append(f"[dim]Choose from: {choices_text}. (Enter for latest)[/dim]")
                else:
                    prompt_parts.append(f"[dim]Choose from: {choices_text}. (Enter for [cyan]{default}[/cyan])[/dim]")
            else:
                prompt_parts.append(f"[dim]Choose from: {choices_text}.[/dim]")
        elif default:
            if str(default).lower() == "latest":
                prompt_parts.append(f"[dim](Enter for latest)[/dim]")
            else:
                prompt_parts.append(f"[dim](Enter for [cyan]{default}[/cyan])[/dim]")
        full_prompt = "  ".join(prompt_parts)
        console.print(f"\n{full_prompt}")

        while True:
            try:
                # Create a 1x1 table for input with smooth thin border (Claude style)
                input_table = Table(
                    box=box.SQUARE,
                    border_style="dim white",
                    expand=True,
                    show_header=False,
                    padding=(0, 1)
                )
                input_table.add_column("input", style="")
                input_table.add_row("[bold white]>[/bold white] ")
                console.print(input_table)

                # Position cursor and get input
                print("\033[2A\033[4C", end="")  # Move up 2 lines, right 4 chars
                user_input = input().strip()
                print("\033[1B")  # Move down 1 line
                console.print("[dim]Ctrl+C to exit[/dim]")

                user_input = user_input.strip()
                if not user_input and default:
                    user_input = str(default)
                if choices:
                    if user_input.lower() in [str(choice).lower() for choice in choices]:
                        console.print(f"[green]✅ Selected: [cyan]{user_input}[/cyan][/green]")
                        return user_input.lower()
                    else:
                        console.print(f"[red]❌ Invalid choice. Please select from: {', '.join([str(c) for c in choices])}[/red]")
                        continue
                console.print(f"[green]✅ Input: [cyan]{user_input}[/cyan][/green]")
                return user_input
            except KeyboardInterrupt:
                console.print("\n[dim]Operation cancelled[/dim]")
                raise
            except EOFError:
                console.print("\n[dim]Input ended[/dim]")
                if default:
                    return str(default)
                raise

    def _live_loop_select_timestamp(self) -> Optional[str]:
        """Run a Live-rendered loop that auto-refreshes and allows typing to interrupt."""
        # Reset previous selection and drain any stray keypresses before starting
        self._selected_ts = None
        self._drain_keyboard()
        # Kick off the background scanner
        self.data_manager.start_refresh()
        try:
            # Prime cached stats immediately
            current_stats = self.data_manager.get_fresh_stats()
            if not current_stats:
                console.print("[red]❌ No timestamp directories found in semantic output directory[/red]")
                return None

            # Build initial renderables for a simple vertical Group to avoid clipping/gaps
            header = self.build_header()
            config_table = self.config_table_component.create_table()
            ts_table = self.timestamp_table_component.create_table(current_stats)
            status_text = self._render_status(current_stats)
            input_buffer = ""
            choices = [str(i) for i in range(1, len(current_stats) + 1)] + ["latest"]
            input_panel = self._render_input_panel(input_buffer, choices)

            stacked_tables = Group(
                config_table,
                Padding(ts_table, (1, 0, 0, 0)),
            )
            group = Group(
                header,
                stacked_tables,
                Panel(status_text, box=box.MINIMAL, border_style="dim", padding=0),
                input_panel,
            )

            # Use normal screen to avoid clearing/restoring screen on exit
            with Live(group, console=console, transient=False, refresh_per_second=8, screen=False) as live:
                self._live = live
                self._running = True
                self._countdown = self._auto_refresh_seconds

                while self._running:
                    # Update status, right table, and input panel
                    current_stats = self.data_manager.get_current_stats()
                    if not current_stats:
                        current_stats = []
                    ts_table = self.timestamp_table_component.create_table(current_stats)
                    status_text = self._render_status(current_stats)
                    choices = [str(i) for i in range(1, len(current_stats) + 1)] + ["latest"]
                    input_panel = self._render_input_panel(input_buffer, choices)

                    updated_stacked = Group(
                        self.config_table_component.create_table(),  # static rebuild to reflect width
                        Padding(ts_table, (1, 0, 0, 0)),
                    )
                    live.update(Group(
                        header,
                        updated_stacked,
                        Panel(status_text, box=box.MINIMAL, border_style="dim", padding=0),
                        input_panel,
                    ))

                    # Handle keyboard input live (Windows only)
                    if sys.platform == 'win32':
                        import msvcrt
                        # Poll in small sleeps across 1 second tick
                        start = time.time()
                        while time.time() - start < 1.0:
                            # Process all pending keys each cycle for responsive typing
                            while msvcrt.kbhit():
                                try:
                                    ch = msvcrt.getwch()
                                except Exception:
                                    ch = ''
                                # Skip extended key prefix and consume next code
                                if ch in ('\x00', '\xe0'):
                                    try:
                                        _ = msvcrt.getwch()
                                    except Exception:
                                        pass
                                    continue
                                if ch in ('\r', '\n'):
                                    # Submit selection from the live buffer
                                    submission = input_buffer.strip() or 'latest'
                                    valid_choices = [c.lower() for c in choices]
                                    if submission.lower() not in valid_choices:
                                        input_buffer = ""
                                        # show error inline above input
                                        live.update(Group(
                                            header,
                                            updated_stacked,
                                            Panel(Text.from_markup(f"[red]❌ Invalid choice. Please select from: {', '.join(choices)}[/red]"), box=box.MINIMAL, border_style="red", padding=0),
                                            self._render_input_panel(input_buffer, choices),
                                        ))
                                        # Do not break; allow user to retype
                                        continue
                                    selected_choice = submission.lower()
                                    selected_ts = None
                                    if selected_choice == 'latest':
                                        if current_stats:
                                            selected_ts = current_stats[0].timestamp
                                    else:
                                        try:
                                            idx = int(selected_choice) - 1
                                            if 0 <= idx < len(current_stats):
                                                selected_ts = current_stats[idx].timestamp
                                        except Exception:
                                            selected_ts = None
                                    if selected_ts:
                                        self._running = False
                                        self._selected_ts = selected_ts
                                        # Drain any additional buffered keys (like extra Enters)
                                        self._drain_keyboard()
                                        break
                                elif ch in ('\x08', '\b'):
                                    # Backspace
                                    input_buffer = input_buffer[:-1]
                                elif ch and ch.isprintable():
                                    input_buffer += ch
                            if not self._running:
                                break
                            time.sleep(0.03)
                    else:
                        time.sleep(1)

                    # Countdown management
                    self._countdown -= 1
                    if self._countdown <= 0:
                        # Force data refresh (thread will also refresh soon, but ensure UI shows latest)
                        self.data_manager.get_fresh_stats()
                        self._countdown = self._auto_refresh_seconds

                # Exited after valid submission
        finally:
            # Ensure background thread is stopped when leaving the live loop
            try:
                if self.data_manager:
                    self.data_manager.stop_refresh()
            except Exception:
                pass

        # Return the selection made inside the Live loop
        selected = getattr(self, "_selected_ts", None)
        if selected:
            console.print(f"[green]✅ Selected:[/green] [cyan]{selected}[/cyan]")
        return selected
    
    def _get_user_choice(self, prompt: str, choices: List[str], default: str) -> str:
        """Get user input with validation."""
        # Format choices
        formatted_choices = [f"[cyan]{choice}[/cyan]" for choice in choices]
        if len(formatted_choices) > 1:
            choices_text = ",".join(formatted_choices[:-1]) + " or " + formatted_choices[-1]
        else:
            choices_text = formatted_choices[0]
        
        full_prompt = f"[bold]{prompt}:[/bold]  [dim]Choose from: {choices_text}. (Enter for latest)[/dim]"
        console.print(Panel(full_prompt, box=box.ROUNDED, border_style="white", padding=(0,1)))
        
        # Claude Code style input panel
        input_table = Table(box=box.SQUARE, border_style="dim white", expand=True, show_header=False, padding=(0, 1))
        input_table.add_column("input", style="")
        input_table.add_row("[bold white]>[/bold white] ")
        console.print(input_table)
        
        # Get input
        while True:
            try:
                # Position cursor to the line with ">"
                print("\033[2A\033[4C", end="")
                user_input = input().strip()
                print("\033[1B")  # Move down
                console.print("[dim]Ctrl+C to exit[/dim]")
                
                if not user_input and default:
                    user_input = default
                
                if user_input.lower() in [choice.lower() for choice in choices]:
                    console.print(f"[green]✅ Selected: [cyan]{user_input}[/cyan][/green]")
                    return user_input.lower()
                else:
                    console.print(f"[red]❌ Invalid choice. Please select from: {', '.join(choices)}[/red]")
                    continue
                    
            except KeyboardInterrupt:
                console.print("\n[dim]Operation cancelled[/dim]")
                raise
            except EOFError:
                if default:
                    return default
                raise
    
    # ===== Backup-inspired helpers for domain selection and uploads =====
    def show_countdown_refresh(self, seconds: int) -> str:
        """Windows-friendly countdown that can be interrupted.
        Returns 'refresh' after countdown, or 'continue' if Enter pressed.
        """
        # Use raw stdout to keep countdown on a single line
        def _write_line(s: str):
            sys.stdout.write("\r" + s)
            sys.stdout.flush()
        clear = " " * 60
        if sys.platform == 'win32':
            import msvcrt
            for i in range(seconds, 0, -1):
                _write_line(f"🔄 auto-refresh: {i}s".ljust(40))
                start = time.time()
                while time.time() - start < 1:
                    if msvcrt.kbhit():
                        key = msvcrt.getwch()
                        if key in ('\r', '\n'):
                            _write_line(("✅ Continuing...").ljust(40))
                            sys.stdout.write("\r" + clear + "\r")
                            sys.stdout.flush()
                            return "continue"
                    time.sleep(0.05)
            _write_line(("🔄 Auto-refreshing...").ljust(40))
            # Clear the line after message
            sys.stdout.write("\r" + clear + "\r")
            sys.stdout.flush()
            return "refresh"
        else:
            for i in range(seconds, 0, -1):
                _write_line(f"🔄 auto-refresh: {i}s".ljust(40))
                time.sleep(1)
            _write_line(("🔄 Auto-refreshing...").ljust(40))
            sys.stdout.write("\r" + clear + "\r")
            sys.stdout.flush()
            return "refresh"

    def get_json_files(self, timestamp_dir: str, domain: Optional[str] = None) -> List[str]:
        base_dir = Path(self.semantic_output_dir) / timestamp_dir
        if not base_dir.exists():
            return []
        files: List[str] = []
        if domain:
            target = base_dir / domain
            if target.exists() and target.is_dir():
                for p in target.rglob('*.json'):
                    files.append(str(p))
        else:
            for p in base_dir.rglob('*.json'):
                files.append(str(p))
        return sorted(files)

    def get_available_domains(self, timestamp_dir: str) -> List[str]:
        base_dir = Path(self.semantic_output_dir) / timestamp_dir
        if not base_dir.exists():
            return []
        return sorted([p.name for p in base_dir.iterdir() if p.is_dir()])

    def get_domain_stats(self, timestamp_dir: str) -> Dict[str, Dict[str, float]]:
        """Return per-domain stats: files, size_mb, recently_updated, latest_modified."""
        stats: Dict[str, Dict[str, float]] = {}
        base_dir = Path(self.semantic_output_dir) / timestamp_dir
        if not base_dir.exists():
            return stats
        now = time.time()
        for dom_dir in base_dir.iterdir():
            if not dom_dir.is_dir():
                continue
            files = list(dom_dir.rglob('*.json'))
            file_count = len(files)
            size_mb = sum(p.stat().st_size for p in files) / (1024 * 1024) if file_count else 0.0
            latest_mod = 0.0
            for p in files:
                try:
                    latest_mod = max(latest_mod, p.stat().st_mtime)
                except Exception:
                    continue
            recently_updated = (now - latest_mod) < 5 if latest_mod > 0 else False
            stats[dom_dir.name] = {
                'files': file_count,
                'size_mb': size_mb,
                'recently_updated': recently_updated,
                'latest_modified': latest_mod,
            }
        return stats

    def wait_for_activity(self, timestamp_dir: str, interval: float = 1.0) -> str:
        """Continuously watch for domain activity.
        Returns 'activity' when detected, or 'continue' if Enter is pressed.
        """
        clear = " " * 60
        def _write_line(s: str):
            sys.stdout.write("\r" + s)
            sys.stdout.flush()
        while True:
            # Check activity using console helper
            domain_stats = self.get_domain_stats(timestamp_dir)
            if any(s.get('recently_updated', False) for s in domain_stats.values()):
                _write_line(("💡 Activity detected. Refreshing…").ljust(40))
                sys.stdout.write("\r" + clear + "\r")
                sys.stdout.flush()
                return 'activity'
            # Allow user to continue with Enter
            if sys.platform == 'win32':
                import msvcrt
                t_end = time.time() + interval
                while time.time() < t_end:
                    if msvcrt.kbhit():
                        key = msvcrt.getwch()
                        if key in ('\r', '\n'):
                            _write_line(("✅ Continuing…").ljust(40))
                            sys.stdout.write("\r" + clear + "\r")
                            sys.stdout.flush()
                            return 'continue'
                    time.sleep(0.05)
            else:
                time.sleep(interval)
        # unreachable
        return 'continue'

    def display_domain_selection(self, timestamp_dir: str) -> Optional[str]:
        """Live domain selection with auto-refresh and immediate Claude-style input.
        Returns a domain name, or None to mean 'all domains'.
        """
        # Initialize
        self._countdown = self._auto_refresh_seconds
        input_buffer = ""
        self._drain_keyboard()

        # Prime
        domains = self.get_available_domains(timestamp_dir)
        if not domains:
            console.print(f"[red]❌ No domains with JSON files found in {timestamp_dir}[/red]")
            return None

        domain_stats = self.get_domain_stats(timestamp_dir)
        choices = [str(i) for i in range(1, len(domains) + 1)] + ["all"]

        def _build_domain_table() -> Table:
            refresh_time = datetime.now().strftime('%H:%M:%S')
            table = Table(title=f"🌐 Available Domains in {timestamp_dir} (Updated: {refresh_time})", box=box.ROUNDED)
            table.add_column("#", style="dim", width=3)
            table.add_column("Domain", style="cyan")
            table.add_column("Files", style="green", justify="center")
            table.add_column("Size", style="blue", justify="center")
            for i, dom in enumerate(domains, 1):
                stats = domain_stats.get(dom, {'files': 0, 'size_mb': 0})
                file_display = f"📄 {stats['files']}" if stats.get('files', 0) > 0 else "📭 0"
                size_val = stats.get('size_mb', 0)
                size_display = f"{size_val:.1f}MB" if size_val > 0 else "-"
                table.add_row(str(i), dom, file_display, size_display)
            return table

        has_active = any(s.get('recently_updated', False) for s in domain_stats.values())
        status = Text()
        if has_active:
            status.append("💡 Domain activity detected  ", style="yellow")
        else:
            status.append("🔄 Auto-refreshing  ", style="cyan")
        status.append(f"(Auto-refresh in {self._countdown}s)", style="dim")

        domain_table = _build_domain_table()
        input_panel = self._render_domain_input_panel(input_buffer, choices)
        group = Group(
            Padding(domain_table, (1, 0, 0, 0)),
            Panel(status, box=box.MINIMAL, border_style="dim", padding=0),
            input_panel,
        )

        selected_domain: Optional[str] = None
        with Live(group, console=console, transient=False, refresh_per_second=8, screen=False) as live:
            start_time = time.time()
            while True:
                # Periodic updates (1s tick)
                now = time.time()
                if now - start_time >= 1.0:
                    start_time = now
                    self._countdown -= 1
                    if self._countdown <= 0:
                        # Refresh domain list and stats
                        domains = self.get_available_domains(timestamp_dir)
                        domain_stats = self.get_domain_stats(timestamp_dir)
                        choices = [str(i) for i in range(1, len(domains) + 1)] + ["all"]
                        self._countdown = self._auto_refresh_seconds
                    # Update status
                    has_active = any(s.get('recently_updated', False) for s in domain_stats.values())
                    status = Text()
                    if has_active:
                        status.append("💡 Domain activity detected  ", style="yellow")
                    else:
                        status.append("🔄 Auto-refreshing  ", style="cyan")
                    status.append(f"(Auto-refresh in {self._countdown}s)", style="dim")

                # Rebuild view
                domain_table = _build_domain_table()
                input_panel = self._render_domain_input_panel(input_buffer, choices)
                live.update(Group(
                    Padding(domain_table, (1, 0, 0, 0)),
                    Panel(status, box=box.MINIMAL, border_style="dim", padding=0),
                    input_panel,
                ))

                # Handle input (Windows)
                if sys.platform == 'win32':
                    import msvcrt
                    # Poll for up to ~1s in small slices to stay responsive
                    slice_start = time.time()
                    while time.time() - slice_start < 0.2:
                        while msvcrt.kbhit():
                            try:
                                ch = msvcrt.getwch()
                            except Exception:
                                ch = ''
                            if ch in ('\x00', '\xe0'):
                                # consume extended key code
                                try:
                                    _ = msvcrt.getwch()
                                except Exception:
                                    pass
                                continue
                            if ch in ('\r', '\n'):
                                submission = input_buffer.strip() or 'all'
                                valid = [c.lower() for c in choices]
                                if submission.lower() not in valid:
                                    input_buffer = ""
                                    # show inline error above input
                                    live.update(Group(
                                        Padding(domain_table, (1, 0, 0, 0)),
                                        Panel(Text.from_markup(f"[red]❌ Invalid choice. Please select from: {', '.join(choices)}[/red]"), box=box.MINIMAL, border_style="red", padding=0),
                                        self._render_domain_input_panel(input_buffer, choices),
                                    ))
                                    break
                                # Resolve selection
                                if submission.lower() == 'all':
                                    selected_domain = None
                                else:
                                    try:
                                        idx = int(submission) - 1
                                        if 0 <= idx < len(domains):
                                            selected_domain = domains[idx]
                                        else:
                                            selected_domain = None
                                    except Exception:
                                        selected_domain = None
                                self._drain_keyboard()
                                console.print("[green]✅ Selected:[/green] " + ("[cyan]All domains[/cyan]" if selected_domain is None else f"[cyan]{selected_domain}[/cyan]"))
                                return selected_domain
                            elif ch in ('\x08', '\b'):
                                input_buffer = input_buffer[:-1]
                            elif ch and ch.isprintable():
                                input_buffer += ch
                        time.sleep(0.02)
                else:
                    time.sleep(0.1)
    
    def run(self):
        """Main application entry point."""
        # Header is rendered inside Live; do not print anything before Live starts
        
        # Load configuration
        if not self.load_config():
            console.print("[red]❌ Failed to load configuration. Exiting.[/red]")
            return
        
        # Check if RAG upload is enabled
        if not self.rag_uploader.is_enabled():
            console.print(Panel(
                "[red]RAG Upload is not enabled or properly configured.[/red]\n"
                "Please check:\n"
                "• Set 'rag_upload.enabled: true' in config.yaml\n"
                "• Set RAGFLOW_API_KEY environment variable\n"
                "• Set RAGFLOW_URL environment variable",
                title="Configuration Required",
                border_style="red"
            ))
            return
        
        try:
            # Main selection loop
            while True:
                # Live loop that refreshes every 5s and allows interrupt by typing
                # Ensure no pending keypress carries over into the live prompt
                self._drain_keyboard()
                ts = self._live_loop_select_timestamp()
                if not ts:
                    # Stay in the app; keep waiting for a valid selection instead of exiting
                    continue

                # Selection already confirmed by the live prompt; no extra line needed

                # Domain selection (backup behavior)
                selected_domain = self.display_domain_selection(ts)
                if selected_domain is None:
                    console.print("[dim]Proceeding with all domains[/dim]")
                else:
                    console.print(f"[dim]Proceeding with domain: {selected_domain}[/dim]")

                # Next step (placeholder for actual upload)
                console.print("[yellow]Upload step will be integrated next.[/yellow]")
                # Do not exit on Enter; loop back to UI for further actions
                console.print("[dim]Tip: Press Ctrl+C to quit at any time.[/dim]")
                # Drain any stray keypresses from domain prompt before looping back
                self._drain_keyboard()
                continue
                
        except KeyboardInterrupt:
            # Propagate to main() so it can pause before exit
            raise
        except Exception:
            # Propagate to main() for unified traceback + pause handling
            raise
        finally:
            # Ensure background thread is stopped on any exit path
            try:
                if self.data_manager:
                    self.data_manager.stop_refresh()
            except Exception:
                pass

def main():
    """Main entry point."""
    try:
        app = RAGFlowLiveConsole()
        app.run()
        # If run() returns, keep window open so user can see output
        try:
            console.print("\n[dim]Run completed. Press Enter to close...[/dim]")
            input()
        except Exception:
            pass
    except KeyboardInterrupt:
        console.print("\n[dim]Operation cancelled by user[/dim]")
        try:
            console.print("[dim]Press Enter to close...[/dim]")
            input()
        except Exception:
            pass
    except Exception as e:
        console.print(f"\n[red]❌ Fatal error: {e}[/red]")
        import traceback
        console.print("[dim]" + traceback.format_exc() + "[/dim]")
        try:
            console.print("[dim]Press Enter to close...[/dim]")
            input()
        except Exception:
            pass

if __name__ == "__main__":
    # Ensure the console doesn't close before the user can read messages
    def _pause_before_exit(msg: str = "[dim]Press any key to exit...[/dim]"):
        try:
            console.print("\n" + msg)
            if sys.platform == 'win32':
                try:
                    import msvcrt
                    # Wait for any key
                    while True:
                        if msvcrt.kbhit():
                            msvcrt.getwch()
                            break
                        time.sleep(0.05)
                    return
                except Exception:
                    pass
            # Fallback to Enter
            try:
                input()
            except Exception:
                # Last resort: wait a bit to allow reading
                time.sleep(10)
        except Exception:
            try:
                time.sleep(10)
            except Exception:
                pass

    try:
        main()
    finally:
        _pause_before_exit()