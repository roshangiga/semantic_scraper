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
import logging

# Create a global console instance with highlighting
console = Console(highlighter=ReprHighlighter())

# Module-level Live instance for in-place page panel updates
_page_live: Live | None = None
_current_domain: str = "unknown"
_current_queue_line: str = ""
_current_sem_queue_line: str = ""
_upcoming_urls: list[str] = []
_upcoming_remaining: int = 0
_last_tree: Tree | None = None
# Track the current URL for the header; ensure it always exists
_current_url: str = ""

def _render_page_panel(tree: Tree, domain: str):
    """Render a modern Layout-based UI with header/body/footer around the page content.

    Kept the function name for backward-compatibility; it now returns a `Layout` instead of a simple `Panel`.
    """
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.console import Group
    from rich.text import Text
    from rich.rule import Rule
    from rich.align import Align
    from rich.table import Table
    from rich.padding import Padding
    from datetime import datetime

    # Optional queue lines above the domain header (no panel)
    from rich.text import Text as _Text
    from rich.console import Group as _Group
    queue_items = []
    if _current_queue_line:
        queue_items.append(_Text(_current_queue_line, style="white"))
    if _current_sem_queue_line:
        queue_items.append(_Text(_current_sem_queue_line, style="white"))
    queue_renderable = _Group(*queue_items) if queue_items else _Text("")
    # Add a bit of horizontal padding to the queue region as requested
    from rich.padding import Padding as _Padding
    queue_renderable = _Padding(queue_renderable, (0, 1, 0, 1))
    # Wrap queue region inside a subtle panel (no header)
    if queue_items:
        from rich.panel import Panel as _Panel
        queue_renderable = _Panel(queue_renderable, border_style="grey50", padding=(0,1))

    # Header: left = animated spinner (Rich Spinner) + current URL (not domain), right = live clock
    header_table = Table.grid(expand=True)
    header_table.add_column(ratio=3)
    header_table.add_column(justify="right", ratio=1)
    # Avoid blue tones for dark consoles; use bright cyan/white
    try:
        # Prefer the current URL; robust fallback to provided domain, then global domain, then placeholder
        _url_val = (_current_url or "").strip()
        _dom_arg = (domain or "").strip()
        _dom_glob = (_current_domain or "").strip()
        current_url_hdr = _url_val or _dom_arg or _dom_glob or "(no URL)"
    except Exception:
        current_url_hdr = (domain or _current_domain or "(no URL)")
    # For header, prefer right-ellipsis so the URL starts with the scheme/host
    def _ellipsize_right_hdr(s: str, max_len: int) -> str:
        return s if len(s) <= max_len else (s[: max_len - 1] + "…")
    try:
        # Give more space to the left area where URL lives
        _hdr_max = max(24, int(console.size.width * 0.70))
    except Exception:
        _hdr_max = 60
    # Use Rich Spinner renderable so it animates automatically inside Live
    from rich.spinner import Spinner as _Spinner
    # Build left-side mini-grid: spinner | globe | URL (with safe single-line ellipsis)
    left_grid = Table.grid(padding=(0,0), expand=True)
    left_grid.add_column(width=2, no_wrap=True)
    left_grid.add_column(width=3, no_wrap=True)
    left_grid.add_column(ratio=1)
    url_text = Text("" + _ellipsize_right_hdr(str(current_url_hdr), _hdr_max), style="bold bright_cyan", no_wrap=True, overflow="ellipsis")
    left_grid.add_row(
        _Spinner('dots', style='bright_magenta'),
        Text("🌐", no_wrap=True, overflow="crop"),
        url_text,
    )
    header_left = left_grid
    # The clock text will be re-rendered each refresh by Live
    header_right = Text.from_markup("[dim]" + datetime.now().strftime("%H:%M:%S") + "[/dim]")
    header_table.add_row(header_left, header_right)
    header = Panel(header_table, border_style="bright_magenta", padding=(0,1))

    # Remember last tree for live refresh from setters
    global _last_tree
    _last_tree = tree

    # Body: split into left (main processing) and right (upcoming URLs); left panel contains only the processing tree
    body_left = Panel(tree, border_style="white", padding=(1,1))

    # Build upcoming URLs list on the right
    from rich.table import Table as _Table
    from rich.text import Text as _T
    # Load upcoming URLs directly from crawler_checkpoint.json
    def _get_upcoming_from_checkpoint(max_items: int = 16):
        try:
            import json as _json
            with open('crawler_checkpoint.json', 'r', encoding='utf-8') as _f:
                _cp = _json.load(_f)
            _queue = _cp.get('crawl_queue', []) or []
            _urls = _queue[:max_items]
            _remaining = max(0, len(_queue) - len(_urls))
            return _urls, _remaining
        except Exception:
            return [], 0
    _urls, _remaining = _get_upcoming_from_checkpoint()
    upcoming_table = _Table.grid(expand=True, padding=(0,1))
    upcoming_table.add_column(ratio=1)
    # Helper: ellipsize from the start (keep tail)
    def _ellipsize_left(s: str, max_len: int) -> str:
        if max_len <= 1:
            return "…" if s else ""
        return s if len(s) <= max_len else ("…" + s[-(max_len-1):])

    if _urls:
        # Estimate a conservative width for right column text
        try:
            # Use slightly wider estimate for the right panel and remove extra subtraction
            max_chars = max(20, int(console.size.width * 0.48))
        except Exception:
            max_chars = 60
        from rich.spinner import Spinner as _Spinner
        from rich.table import Table as __Table
        for idx, url in enumerate(_urls):
            # No bullets; tighter vertical spacing (no extra blank rows)
            shortened = _ellipsize_left(str(url), max_chars)
            if idx == 0:
                # Top item shows an animated spinner on the left
                row = __Table.grid(padding=(0,0))
                row.add_column(width=2, no_wrap=True)
                row.add_column(ratio=1)
                row.add_row(_Spinner('dots', style='grey70'), _T(shortened, style="grey70"))
                upcoming_table.add_row(row)
            else:
                upcoming_table.add_row(_T(shortened, style="grey70"))
    else:
        upcoming_table.add_row(_T("(queue empty)", style="grey58"))

    # Footer line: + X more
    if _remaining and _remaining > 0:
        upcoming_table.add_row(_T(f"+ {_remaining} more", style="grey58"))

    body_right = Panel(upcoming_table, title="Upcoming URLs", title_align="left", border_style="grey50", padding=(1,1))

    # Footer: separator + centered quit hint
    hint_rule = Rule(style="dim")
    hint_text = Align.center(Text("Press Ctrl+C to quit", style="dim"))
    footer_group = Group(hint_rule, hint_text)
    footer = Padding(footer_group, (1, 0, 0, 0))

    # Compose layout: queue (auto), header (fixed height 3), body (flex), footer (auto)
    layout = Layout(name="root")
    layout.split(Layout(name="queue", size=3), Layout(name="header", size=3), Layout(name="body"), Layout(name="footer", size=3))
    layout["queue"].update(queue_renderable)
    layout["header"].update(header)
    # Split body into two equal columns
    layout["body"].split_row(Layout(name="left", ratio=1), Layout(name="right", ratio=1))
    layout["body"]["left"].update(body_left)
    layout["body"]["right"].update(body_right)
    layout["footer"].update(footer)

    return layout

def update_upcoming_urls(urls: list[str], remaining_count: int = 0):
    """Update the upcoming URLs list shown on the right-hand side and refresh the live view.

    Args:
        urls: A small window of upcoming URLs to display (e.g., next 5-10 items)
        remaining_count: Additional queued items not shown in the list
    """
    global _upcoming_urls, _upcoming_remaining, _page_live, _current_domain, _last_tree
    _upcoming_urls = list(urls) if urls else []
    _upcoming_remaining = int(remaining_count) if remaining_count else 0
    if _page_live is not None and _last_tree is not None:
        panel = _render_page_panel(_last_tree, _current_domain)
        _page_live.update(panel, refresh=True)

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
    """Print content in a styled panel."""
    console.print(Panel(content, title=title, style=style))

def print_header(title: str):
    """Print a styled header."""
    text = Text(title, style="bold magenta")
    console.print(Panel(text, style="magenta"))

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
    """Create a styled table with enhanced colors."""
    table = Table(
        title=title, 
        show_header=True, 
        header_style="bold bright_magenta",
        border_style=style,
        title_style="bold bright_white"
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

def create_page_processing_tree(page_num: int, queue_size: int, domain: str, url: str) -> Tree:
    """Create a rich tree for page processing with panels and enhanced colors."""
    from rich.text import Text
    from rich.panel import Panel
    from rich.markup import escape
    
    # Create main tree with minimal root
    tree = Tree(
        label="",
        guide_style="bright_white",
        hide_root=True
    )
    
    # Add static progress bar for queue status using simple rich components
    from rich.text import Text
    from rich.table import Table
    
    # Calculate progress using checkpoint JSON for accurate counts
    def _get_checkpoint_counts():
        try:
            import json as _json
            with open('crawler_checkpoint.json', 'r', encoding='utf-8') as _f:
                _cp = _json.load(_f)
            queue = _cp.get('crawl_queue', []) or []
            visited = _cp.get('visited_urls', []) or []
            
            # If visited_urls is a list, get its length; if it's a number, use it directly
            if isinstance(visited, list):
                completed_count = len(visited)
            elif isinstance(visited, int):
                completed_count = visited
            else:
                completed_count = 0
                
            remaining_count = len(queue)
            return completed_count, remaining_count
        except Exception:
            return page_num, queue_size
    
    processed, remaining = _get_checkpoint_counts()
    estimated_total = processed + remaining
    progress_ratio = processed / estimated_total if estimated_total > 0 else 0
    
    # Create a simple visual progress bar using text
    bar_width = 16
    filled_width = int(bar_width * progress_ratio)
    empty_width = bar_width - filled_width
    
    # Create static progress bar using Rich components
    progress_table = Table(show_header=False, box=None, padding=(0, 0), expand=False)
    progress_table.add_column(style="bold bright_magenta", width=18)  # Label
    progress_table.add_column(width=25)  # Progress bar
    # Avoid background colors for readability on dark consoles
    progress_table.add_column(style="dim bright_white", width=9)  # Stats (narrower)
    
    # Create progress bar text
    progress_bar_text = Text()
    progress_bar_text.append("[", style="dim white")
    progress_bar_text.append("█" * filled_width, style="bold bright_green")
    progress_bar_text.append("░" * empty_width, style="dim white")
    progress_bar_text.append("]", style="dim white")
    
    progress_table.add_row(
        "📊 Queue Progress:",
        progress_bar_text,
        f"{processed}/{estimated_total}"
    )

    # Build unified single-line queue style and store globally for header rendering (smaller bar for header)
    from rich.text import Text as _Text
    try:
        _tw2 = max(0, int(console.size.width))
        bar_width = 80  # Much wider bar for better progress visualization
    except Exception:
        bar_width = 80
    filled = int(min(max(processed, 0) / max(estimated_total, 1), 1.0) * bar_width) if estimated_total > 0 else 0
    empty = bar_width - filled
    bar = "█" * filled + "░" * empty
    global _current_queue_line
    # Add spaces to right-align the count
    spaces = " " * (console.size.width - len(f"📊 Queue Progress: [{bar}] {processed}/{estimated_total}") - 10) if hasattr(console, 'size') else " " * 20
    _current_queue_line = f"📊 Queue Progress: [{bar}]{spaces}{processed}/{estimated_total}"
    # Do not add queue line inside the tree; it will render above the domain panel
    
    # Track current URL for the header (store full URL; header will handle truncation)
    try:
        global _current_url
        _current_url = str(url)
    except Exception:
        pass
    # Use the tree itself as the parent for subsequent steps (no nested 'Source' group)
    try:
        setattr(tree, "_current_parent", tree)
    except Exception:
        pass

    # Start or update Live to show the initial panel immediately
    global _page_live, _current_domain
    _current_domain = domain
    panel = _render_page_panel(tree, _current_domain)
    if _page_live is None:
        _page_live = Live(panel, console=console, refresh_per_second=10, screen=False)
        _page_live.start()
    else:
        _page_live.update(panel, refresh=True)

    return tree

def add_processing_step(tree: Tree, step_type: str, message: str, style: str = "green") -> Tree:
    """Add a processing step to the tree with enhanced colors and styling."""
    from rich.text import Text
    from rich.panel import Panel
    
    # Create step text with enhanced icon and styling
    step_text = Text()

    # Special-case styling: ensure "Redirected to PDF document" is blue and uses redirect icon
    try:
        if isinstance(message, str) and "redirected to pdf document" in message.lower():
            # Normalize message without any pre-existing icon
            norm_msg = message
            if norm_msg.startswith("📥 "):
                norm_msg = norm_msg[2:].lstrip()
            # Prepend proper redirect icon
            display_msg = f"↪️ {norm_msg}"
            step_text.append(display_msg, style="bright_blue")
            parent = getattr(tree, "_current_parent", tree)
            node = parent.add(step_text)
            # Update live panel
            if _page_live is not None:
                panel = _render_page_panel(tree, _current_domain)
                _page_live.update(panel, refresh=True)
            return node
    except Exception:
        pass

    # Special-case: beautify page completion message (celebratory line with padding)
    try:
        if isinstance(message, str) and ("completed" in message.lower()) and ("page" in message.lower()):
            parent = getattr(tree, "_current_parent", tree)
            from rich.text import Text as _Tx
            # Top padding
            parent.add(Text(" ", style="dim"))
            # Celebration line
            completed_line = _Tx()
            completed_line.append("🎉 ", style="bold bright_green")
            completed_line.append(message, style="bold bright_green")
            node = parent.add(completed_line)
            # Bottom padding
            parent.add(Text(" ", style="dim"))
            if _page_live is not None:
                panel = _render_page_panel(tree, _current_domain)
                _page_live.update(panel, refresh=True)
            return node
    except Exception:
        pass
    
    if step_type == "success":
        step_text.append(message, style="bold green")

    elif step_type == "info":
        step_text.append(message, style="blue")

    elif step_type == "warning":
        step_text.append(message, style="yellow")

    elif step_type == "fallback_panel":
        # Create panel for fallback processing warnings with external padding
        from rich.panel import Panel
        from rich.padding import Padding
        fallback_panel = Panel(
            f"[yellow]{message}[/yellow]",
            title="Fallback Processing",
            title_align="left",
            style="yellow",
            border_style="yellow",
            padding=(0, 1)
        )
        # Add vertical padding around the panel
        padded_panel = Padding(fallback_panel, (1, 0))
        return tree.add(padded_panel)

    elif step_type == "processing":
        step_text.append(message, style="cyan")

    elif step_type == "semantic":
        step_text.append(message, style="bold magenta")

    elif step_type == "semantic_panel":
        # Create panel for semantic chunking success
        from rich.panel import Panel
        semantic_panel = Panel(
            f"[bold bright_green]{message}[/bold bright_green]",
            title="✅ Semantic Completed",
            title_align="left",
            style="bold green",
            border_style="bright_green",
            padding=(0, 1)
        )
        return tree.add(semantic_panel)

    elif step_type == "semantic_error_panel":
        # Create panel for semantic chunking errors
        from rich.panel import Panel
        semantic_panel = Panel(
            f"[bold bright_red]{message}[/bold bright_red]",
            title="❌ Semantic Failed",
            title_align="left",
            style="bold red",
            border_style="bright_red",
            padding=(0, 1)
        )
        return tree.add(semantic_panel)

    elif step_type == "semantic_progress_panel":
        # Update single-line semantic queue status outside the panel (no panel rendering here)
        # Message format: "completed,failed,total,filename"
        try:
            parts = message.split(',')
            completed, failed, total = map(int, parts[:3])
        except Exception:
            completed, failed, total = 0, 0, 0

        processed = max(0, int(completed + failed))
        total = max(0, int(total))
        bar_width = 22
        filled = int(bar_width * (processed / total)) if total > 0 else 0
        filled = min(max(filled, 0), bar_width)
        empty = bar_width - filled
        bar = "█" * filled + "░" * empty

        global _current_sem_queue_line
        # Include small stats for done/error at the end for quick glance
        suffix = f"  (✅ {completed} | ❌ {failed})" if (completed or failed) else ""
        _current_sem_queue_line = f"│ 🧠 Semantic Queue:[{bar}]   {processed}/{total}{suffix}"

        # Trigger live update by re-rendering the panel layout
        if _page_live is not None:
            panel = _render_page_panel(tree, _current_domain)
            _page_live.update(panel, refresh=True)
        return tree

    elif step_type == "rag_success":
        # Create panel for successful RAG operations
        from rich.panel import Panel
        rag_panel = Panel(
            f"[bold bright_green]{message}[/bold bright_green]",
            title="📤 RAG Upload Success",
            title_align="left",
            style="bold green",
            border_style="bright_green",
            padding=(0, 1)
        )
        return tree.add(rag_panel)

    elif step_type == "rag_error":
        # Create panel for RAG errors
        from rich.panel import Panel
        rag_panel = Panel(
            f"[bold bright_red]{message}[/bold bright_red]",
            title="❌ RAG Upload Error",
            title_align="left",
            style="bold red",
            border_style="bright_red",
            padding=(0, 1)
        )
        return tree.add(rag_panel)

    elif step_type == "file":
        # Deduplicate and render file-saved info in a simple borderless table; support timing formats "(0.06s)" and "(in 0.06s)"
        from rich.text import Text as _Tx
        from rich.table import Table as _Tbl
        import re as _re
        parent = getattr(tree, "_current_parent", tree)

        # Determine type label
        if "HTML" in message:
            label = "HTML saved"
        elif "MARKDOWN" in message:
            label = "Markdown saved"
        elif "DOCX" in message:
            label = "DOCX saved"
        else:
            label = "File saved"

        # Extract timing if present: matches "(0.06s)" or "(in 0.06s)"
        m = _re.search(r"\((?:in\s+)?\d+(?:\.\d+)?s\)", message)
        timing = m.group(0) if m else ""

        # Dedup key (label + timing)
        key = f"{label}|{timing}"
        try:
            seen = getattr(tree, "_file_seen", set())
        except Exception:
            seen = set()
        if key in seen:
            return parent  # skip duplicate
        seen.add(key)
        try:
            setattr(tree, "_file_seen", seen)
        except Exception:
            pass

        # Build a simple borderless two-column row: left label, right timing
        left = _Tx()
        left.append("💾 ", style="bright_green")
        left.append(label, style="bold green")
        tbl = _Tbl.grid(padding=(0,0))
        tbl.add_column(ratio=1)
        tbl.add_column(justify="right", width=12, no_wrap=True)
        tbl.add_row(left, _Tx(timing, style="dim") if timing else "")
        node = parent.add(tbl)
        return node
    elif step_type == "duplicate_panel":
        # Create panel for duplicate removal messages like other operations
        from rich.panel import Panel
        duplicate_panel = Panel(
            f"[bold bright_yellow]{message}[/bold bright_yellow]",
            title="🗑️ Duplicate Removed",
            title_align="left",
            style="bold yellow",
            border_style="bright_yellow",
            padding=(0, 1)
        )
        return tree.add(duplicate_panel)
    
    elif step_type == "crawl_reports_panel":
        # Create panel for crawl reports list
        from rich.panel import Panel
        reports_panel = Panel(
            f"[bold bright_green]{message}[/bold bright_green]",
            title="📊 Crawl Reports Generated",
            title_align="left",
            style="bold green",
            border_style="bright_green",
            padding=(0, 1)
        )
        return tree.add(reports_panel)
    
    elif step_type == "completion_panel":
        # Create panel for completion summary
        from rich.panel import Panel
        completion_panel = Panel(
            f"[bold bright_green]{message}[/bold bright_green]",
            title="🎉 Crawling Completed",
            title_align="left",
            style="bold green",
            border_style="bright_green",
            padding=(0, 1)
        )
        return tree.add(completion_panel)
    else:
        # Handle regular duplicate file messages by converting them to use the new panel
        if "Removed duplicate file" in message or "duplicate" in message.lower():
            from rich.panel import Panel as _P
            from rich.text import Text as _Tx
            parent = getattr(tree, "_current_parent", tree)
            duplicate_panel = _P(
                f"[bold bright_yellow]{message}[/bold bright_yellow]",
                title="🗑️ Duplicate Removed",
                title_align="left",
                style="bold yellow",
                border_style="bright_yellow",
                padding=(0, 1)
            )
            return parent.add(duplicate_panel)
        step_text.append(message, style=style)
    
    node = tree.add(step_text)

    # Update live panel on every step addition for in-place refresh
    if _page_live is not None:
        panel = _render_page_panel(tree, _current_domain)
        _page_live.update(panel, refresh=True)

    return node

def print_processing_tree_final(tree: Tree, page_num: int, domain: str = "unknown"):
    """Update the live page panel in-place instead of printing new blocks."""

    from rich.text import Text
    from rich.panel import Panel as _P
    # Render celebratory completion panel with vertical padding
    parent = getattr(tree, "_current_parent", tree)
    parent.add(Text(" ", style="dim"))  # top padding
    # Get actual current count from checkpoint
    try:
        import json as _json
        with open('crawler_checkpoint.json', 'r', encoding='utf-8') as _f:
            _cp = _json.load(_f)
        visited = _cp.get('visited_urls', []) or []
        if isinstance(visited, list):
            actual_count = len(visited)
        elif isinstance(visited, int):
            actual_count = visited
        else:
            actual_count = page_num
    except Exception:
        actual_count = page_num
        
    content = Text()
    content.append("🎉 ", style="bold bright_green")
    content.append(f"Page {actual_count} completed successfully!", style="bold bright_green")
    page_panel = _P(content, border_style="bright_green", padding=(0,1))
    parent.add(page_panel)
    parent.add(Text(" ", style="dim"))  # bottom padding

    # Render/update a persistent Live panel
    global _page_live, _current_domain
    _current_domain = domain
    panel = _render_page_panel(tree, _current_domain)
    if _page_live is None:
        _page_live = Live(panel, console=console, refresh_per_second=10, screen=False)
        _page_live.start()
    else:
        _page_live.update(panel, refresh=True)

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