#!/usr/bin/env python3
"""
Rich console utilities for beautiful console output.
"""

from rich.console import Console
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.syntax import Syntax
from rich.highlighter import ReprHighlighter
from rich.tree import Tree
from rich import box
from rich.console import Group
from rich.rule import Rule
from rich.layout import Layout
from rich.align import Align
from rich.padding import Padding
from rich.spinner import Spinner
import logging
import sys
from datetime import datetime

# Create a global console instance with highlighting
console = Console(highlighter=ReprHighlighter())

# Module-level Live instance for in-place page panel updates
_page_live: Live | None = None
_current_domain: str = "unknown"
_current_url: str = ""
_queue_progress_data: dict = {"processed": 0, "total": 0}

def print_app_title():
    """Print the application title at the very top."""
    title_text = Text("🚀 Craw4AI Docling - Web Crawler", style="bold bright_white")
    title_panel = Panel(
        Align.center(title_text),
        box=box.DOUBLE,
        border_style="bright_magenta",
        padding=(1, 2)
    )
    console.print(title_panel)
    console.print()  # Add some space

def setup_rich_logging():
    """Setup rich logging handler for beautiful logs."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, show_path=False)]
    )

def print_success(message: str):
    """Print a success message with green styling."""
    console.print(f"✅ {message}", style="bold bright_green")

def print_error(message: str):
    """Print an error message with red styling."""
    console.print(f"❌ {message}", style="bold bright_red")

def print_warning(message: str):
    """Print a warning message with yellow styling."""
    console.print(f"⚠️ {message}", style="bold bright_yellow")

def print_info(message: str):
    """Print an info message with blue styling."""
    console.print(f"ℹ️ {message}", style="bold bright_blue")

def print_processing(message: str):
    """Print a processing message with cyan styling."""
    console.print(f"🔄 {message}", style="bold bright_cyan")

def print_url(url: str):
    """Print a URL with special highlighting."""
    console.print(f"🔗 {url}", style="link")

def print_file_saved(filename: str, format_type: str, time_taken: float = None):
    """Print file saved message with highlighting."""
    if time_taken:
        console.print(f"📄 Saved [bright_cyan]{format_type.upper()}[/bright_cyan]: [green]{filename}[/green] [dim](in {time_taken:.2f}s)[/dim]")
    else:
        console.print(f"📄 Saved [bright_cyan]{format_type.upper()}[/bright_cyan]: [green]{filename}[/green]")

def print_progress_step(step: str, current: int, total: int):
    """Print progress step with colors."""
    console.print(f"🔄 [{current}/{total}] [bright_magenta]{step}[/bright_magenta]")

def print_semantic_processing(message: str):
    """Print semantic processing message with special styling."""
    console.print(f"🧠 {message}", style="bold purple")

def print_rag_upload(message: str):
    """Print RAG upload message with special styling."""
    console.print(f"📤 {message}", style="bold cyan")

def print_panel(title: str, content: str, style: str = "blue"):
    """Print content in a compact, panel-like block without heavy borders."""
    title_text = Text(title, style=f"bold {style}")
    try:
        content_text = Text.from_markup(content)
    except Exception:
        content_text = Text(str(content))
    block = Group(title_text, content_text)
    console.print(block)

def print_header(title: str):
    """Print a compact header without a surrounding panel."""
    console.print(Text(title, style="bold magenta"))
    console.print(Rule(style="grey23"))

def create_progress():
    """Create a rich progress bar."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    )

def create_table(title: str = None, style: str = "bright_blue") -> Table:
    """Create a compact table with minimal borders for cleaner UX."""
    table = Table(
        title=title,
        show_header=False,
        header_style="bold bright_magenta",
        title_style="bold bright_white",
        box=box.SIMPLE,
        show_lines=False,
        expand=True,
        pad_edge=False,
        padding=(0, 0)
    )
    return table

def print_syntax(code: str, language: str = "python"):
    """Print syntax-highlighted code."""
    syntax = Syntax(code, language, theme="monokai", line_numbers=True)
    console.print(syntax)

def print_json(data: dict):
    """Print JSON data with syntax highlighting."""
    import json
    json_str = json.dumps(data, indent=2)
    syntax = Syntax(json_str, "json", theme="monokai")
    console.print(syntax)

def _get_checkpoint_counts():
    """Get current processed and remaining counts from checkpoint file."""
    try:
        import json
        with open('crawler_checkpoint.json', 'r', encoding='utf-8') as f:
            cp = json.load(f)
        queue = cp.get('crawl_queue', []) or []
        visited = cp.get('visited_urls', []) or []
        
        if isinstance(visited, list):
            processed = len(visited)
        elif isinstance(visited, int):
            processed = visited
        else:
            processed = 0
            
        remaining = len(queue)
        return processed, remaining
    except Exception:
        return 0, 0

def _create_checkpoint_status():
    """Create checkpoint status display."""
    try:
        import json
        with open('crawler_checkpoint.json', 'r', encoding='utf-8') as f:
            cp = json.load(f)
        
        visited = cp.get('visited_urls', []) or []
        visited_count = len(visited) if isinstance(visited, list) else (visited if isinstance(visited, int) else 0)
        
        # Check for semantic tasks (this is a placeholder - adjust based on actual semantic task tracking)
        semantic_tasks = 0  # You may need to adjust this based on how semantic tasks are tracked
        
        status_text = Text()
        status_text.append("📥 ", style="dim")
        status_text.append(f"Loaded checkpoint with {visited_count} visited URLs, {semantic_tasks} semantic tasks", style="dim")
        
        return status_text
    except Exception:
        return Text("📥 No checkpoint found", style="dim")

def _create_queue_progress():
    """Create the queue progress display."""
    processed, remaining = _get_checkpoint_counts()
    total = processed + remaining
    
    if total == 0:
        # Return empty panel to maintain layout
        empty_table = Table(show_header=False, box=None, expand=True, padding=(0, 1))
        empty_table.add_column()
        empty_table.add_row(Text("No queue data", style="dim"))
        return Panel(empty_table, box=box.ROUNDED, border_style="dim", padding=(0, 1))
    
    # Create progress bar table (1 row x 3 columns)
    progress_table = Table(
        show_header=False,
        box=None,
        expand=True,
        padding=(0, 1)
    )
    progress_table.add_column(width=20)  # Label column
    progress_table.add_column(ratio=1)   # Progress bar column
    progress_table.add_column(justify="right", width=12)  # Count column
    
    # Calculate available width dynamically (console width - label - count - padding)
    try:
        total_width = console.size.width
        # Account for label (20) + count (12) + borders/padding (~10)
        available_width = max(20, total_width - 20 - 12 - 10)
    except:
        available_width = 50
    
    # Create progress bar to fill available space
    bar_width = available_width - 2  # Subtract 2 for brackets
    progress_ratio = processed / total
    filled_width = int(bar_width * progress_ratio)
    empty_width = bar_width - filled_width
    
    # Label text
    label_text = Text("📊 Queue Progress", style="bold bright_magenta")
    
    # Progress bar text that fills the column
    progress_text = Text()
    progress_text.append("[", style="dim white")
    progress_text.append("█" * filled_width, style="bold bright_green")
    progress_text.append("░" * empty_width, style="dim white")
    progress_text.append("]", style="dim white")
    
    # Count text
    count_text = Text(f"{processed}/{total}", style="dim bright_white")
    
    # Add row to table
    progress_table.add_row(label_text, progress_text, count_text)
    
    # Wrap in a panel with rounded top corners
    return Panel(progress_table, box=box.ROUNDED, border_style="bright_blue", padding=(0, 1))

def _create_upcoming_urls():
    """Create the upcoming URLs panel."""
    try:
        import json
        with open('crawler_checkpoint.json', 'r', encoding='utf-8') as f:
            cp = json.load(f)
        queue = cp.get('crawl_queue', []) or []
        urls = queue[:16]  # Show 16 URLs
        remaining = max(0, len(queue) - len(urls))
    except Exception:
        urls = []
        remaining = 0
    
    # Create table with numbering
    table = Table.grid(expand=True, padding=(0,0))
    table.add_column(width=3, justify="right", style="dim", no_wrap=True)  # Number column
    table.add_column(ratio=1, no_wrap=True, overflow="ellipsis")  # URL column
    
    if urls:
        try:
            max_chars = max(30, int(console.size.width * 0.45))
        except Exception:
            max_chars = 60
            
        def ellipsize_left(s: str, max_len: int) -> str:
            if max_len <= 1:
                return "…" if s else ""
            return s if len(s) <= max_len else ("…" + s[-(max_len-1):])
        
        for idx, url in enumerate(urls):
            shortened = ellipsize_left(str(url), max_chars)
            if idx == 0:
                # First item with spinner
                row = Table.grid(padding=(0,0))
                row.add_column(width=2, no_wrap=True)
                row.add_column(ratio=1)
                row.add_row(Spinner('dots', style='grey70'), Text(shortened, style="grey70"))
                table.add_row(Text(f"{idx+1}.", style="dim"), row)
            else:
                table.add_row(Text(f"{idx+1}.", style="dim"), Text(shortened, style="grey70"))
    else:
        table.add_row("", Text("(queue empty)", style="grey58"))
    
    # Add "more" footer
    if remaining > 0:
        table.add_row("", Text(f"+ {remaining} more", style="grey58"))
    
    return Panel(table, title="Upcoming URLs", title_align="left", border_style="grey50", padding=(1,1))

def _get_next_url_from_queue():
    """Get the first URL from the crawl queue."""
    try:
        import json
        with open('crawler_checkpoint.json', 'r', encoding='utf-8') as f:
            cp = json.load(f)
        queue = cp.get('crawl_queue', []) or []
        return queue[0] if queue else None
    except Exception:
        return None

def _create_header():
    """Create the header with next URL from queue and clock."""
    header_table = Table.grid(expand=True)
    header_table.add_column(ratio=3)
    header_table.add_column(justify="right", ratio=1)
    
    # Left side: spinner + globe + next URL from queue
    left_grid = Table.grid(padding=(0,0), expand=True)
    left_grid.add_column(width=2, no_wrap=True)
    left_grid.add_column(width=3, no_wrap=True)
    left_grid.add_column(ratio=1)
    
    try:
        max_url_len = max(24, int(console.size.width * 0.60))
    except Exception:
        max_url_len = 60
    
    def ellipsize_right(s: str, max_len: int) -> str:
        return s if len(s) <= max_len else (s[:max_len-1] + "…")
    
    # Get next URL from queue instead of current processing URL
    next_url = _get_next_url_from_queue()
    url_display = next_url or _current_domain or "(queue empty)"
    url_text = Text(ellipsize_right(str(url_display), max_url_len), style="bold bright_cyan", no_wrap=True, overflow="ellipsis")
    
    left_grid.add_row(
        Spinner('dots', style='bright_magenta'),
        Text("🌐", no_wrap=True),
        url_text,
    )
    
    # Right side: clock
    clock_text = Text(datetime.now().strftime("%H:%M:%S"), style="dim")
    
    header_table.add_row(left_grid, clock_text)
    return Panel(header_table, border_style="bright_magenta", padding=(0,1))

def _create_layout_content(tree: Tree):
    """Create the complete layout content within Live update region."""
    # Get queue progress and checkpoint status
    queue_progress = _create_queue_progress()
    checkpoint_status = _create_checkpoint_status()
    
    # Create header
    header = _create_header()
    
    # Create body panels
    body_left = Panel(tree, border_style="white", padding=(1,1))
    body_right = _create_upcoming_urls()
    
    # Create footer
    hint_rule = Rule(style="dim")
    hint_text = Align.center(Text("Press Ctrl+C to quit", style="dim"))
    footer = Group(hint_rule, hint_text)
    
    # Main layout structure: queue, header, body, footer (removed checkpoint)
    layout = Layout(name="root")
    layout.split(
        Layout(name="queue", size=3),
        Layout(name="header", size=3), 
        Layout(name="body"),
        Layout(name="footer", size=2)
    )
    
    # Update all sections
    layout["queue"].update(queue_progress)
    layout["header"].update(header)
    layout["body"].split_row(Layout(name="left", ratio=1), Layout(name="right", ratio=1))
    layout["body"]["left"].update(body_left)
    layout["body"]["right"].update(body_right)
    layout["footer"].update(footer)
    
    return layout

def create_page_processing_tree(page_num: int, queue_size: int, domain: str, url: str) -> Tree:
    """Create a rich tree for page processing with panels and enhanced colors."""
    global _current_domain, _current_url, _page_live
    
    # Update global state
    _current_domain = domain
    _current_url = str(url)
    
    # Create main tree
    tree = Tree(
        label="",
        guide_style="bright_white",
        hide_root=True
    )
    
    # Set tree as current parent for adding steps
    setattr(tree, "_current_parent", tree)
    
    # Create complete layout with Live update
    layout = _create_layout_content(tree)
    
    # Start or update Live display
    if _page_live is None:
        _page_live = Live(layout, console=console, refresh_per_second=10, screen=False)
        _page_live.start()
    else:
        _page_live.update(layout, refresh=True)
    
    return tree

def add_processing_step(tree: Tree, step_type: str, message: str, style: str = "green") -> Tree:
    """Add a processing step to the tree with enhanced colors and styling."""
    step_text = Text()
    parent = getattr(tree, "_current_parent", tree)
    
    if step_type == "success":
        step_text.append(message, style="bold green")
    elif step_type == "info":
        step_text.append(message, style="blue")
    elif step_type == "warning":
        # Check if this is a duplicate removal message and add padding
        if "duplicate" in message.lower():
            # Add vertical padding before
            parent.add(Text(" ", style="dim"))
            step_text.append(message, style="yellow")
            node = parent.add(step_text)
            # Add vertical padding after
            parent.add(Text(" ", style="dim"))
            # Update live display
            if _page_live is not None:
                layout = _create_layout_content(tree)
                _page_live.update(layout, refresh=True)
            return node
        else:
            step_text.append(message, style="yellow")
    elif step_type == "processing":
        step_text.append(message, style="cyan")
    elif step_type == "semantic":
        step_text.append(message, style="bold magenta")
    elif step_type == "file":
        # Handle file saved messages
        import re
        if "HTML" in message:
            label = "HTML saved"
        elif "MARKDOWN" in message:
            label = "Markdown saved"
        elif "DOCX" in message:
            label = "DOCX saved"
        else:
            label = "File saved"
        
        # Extract timing
        timing_match = re.search(r"\((?:in\s+)?\d+(?:\.\d+)?s\)", message)
        timing = timing_match.group(0) if timing_match else ""
        
        # Create file saved display
        file_table = Table.grid(padding=(0,0))
        file_table.add_column(ratio=1)
        file_table.add_column(justify="right", width=12, no_wrap=True)
        
        left_text = Text()
        left_text.append("💾 ", style="bright_green")
        left_text.append(label, style="bold green")
        
        file_table.add_row(left_text, Text(timing, style="dim") if timing else "")
        node = parent.add(file_table)
        
        # Update live display
        if _page_live is not None:
            layout = _create_layout_content(tree)
            _page_live.update(layout, refresh=True)
        return node
    else:
        step_text.append(message, style=style)
    
    # Add step to tree
    node = parent.add(step_text)
    
    # Update live display
    if _page_live is not None:
        layout = _create_layout_content(tree)
        _page_live.update(layout, refresh=True)
    
    return node

def print_processing_tree_final(tree: Tree, page_num: int, domain: str = "unknown"):
    """Update the live page panel in-place for final completion."""
    global _current_domain
    _current_domain = domain
    
    parent = getattr(tree, "_current_parent", tree)
    
    # Add completion message with padding
    parent.add(Text(" ", style="dim"))
    completion_text = Text()
    completion_text.append("🎉 ", style="bold bright_green")
    completion_text.append(f"Page {page_num} completed successfully!", style="bold bright_green")
    completion_panel = Panel(completion_text, border_style="bright_green", padding=(0,1))
    parent.add(completion_panel)
    parent.add(Text(" ", style="dim"))
    
    # Update live display
    if _page_live is not None:
        layout = _create_layout_content(tree)
        _page_live.update(layout, refresh=True)

def stop_page_live():
    """Stop and clear the persistent live page panel."""
    global _page_live
    if _page_live is not None:
        try:
            _page_live.stop()
        finally:
            _page_live = None

def print_page_tree(tree: Tree):
    """Print a page processing tree."""
    console.print(tree)

def add_tree_step(tree: Tree, message: str, style: str = "green"):
    """Add a processing step to the tree."""
    tree.add(f"[{style}]{message}[/{style}]")

def print_tree_separator():
    """Print a separator line after tree processing."""
    console.print("[dim]" + "═" * 50 + "[/dim]")