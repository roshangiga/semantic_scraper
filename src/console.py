#!/usr/bin/env python3
"""
Rich console utilities for beautiful console output.
"""

from rich.console import Console
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
    console.print(f"[OK] {message}", style="bold bright_green")

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
        guide_style="bright_blue",
        hide_root=True
    )
    
    # Add static progress bar for queue status using simple rich components
    from rich.text import Text
    from rich.table import Table
    
    # Calculate progress (estimate total based on current state)
    estimated_total = queue_size + page_num  # Rough estimate  
    processed = page_num
    progress_ratio = processed / estimated_total if estimated_total > 0 else 0
    
    # Create a simple visual progress bar using text
    bar_width = 20
    filled_width = int(bar_width * progress_ratio)
    empty_width = bar_width - filled_width
    
    # Create static progress bar using Rich components
    progress_table = Table(show_header=False, box=None, padding=(0, 0), expand=False)
    progress_table.add_column(style="bold bright_magenta underline", width=18)  # Label
    progress_table.add_column(width=25)  # Progress bar
    progress_table.add_column(style="bold bright_white on blue", width=12)  # Stats
    
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
    
    tree.add(progress_table)
    
    # Add URL as simple text without panel
    if len(url) > 80:
        display_url = url[:77] + "..."
    else:
        display_url = url
        
    url_text = Text()
    url_text.append("🔗 URL: ", style="bold bright_blue")
    url_text.append(display_url, style="italic bright_blue underline")
    tree.add(url_text)
    
    return tree

def add_processing_step(tree: Tree, step_type: str, message: str, style: str = "green") -> Tree:
    """Add a processing step to the tree with enhanced colors and styling."""
    from rich.text import Text
    from rich.panel import Panel
    
    # Create step text with enhanced icon and styling
    step_text = Text()
    
    if step_type == "success":
        step_text.append("✅", style="bold blink bright_green")
        step_text.append(" " + message, style="bold bright_green underline")
    elif step_type == "info":
        step_text.append("ℹ️", style="bold bright_blue") 
        step_text.append(" " + message, style="italic bright_blue")
    elif step_type == "warning":
        step_text.append("⚠️", style="bold blink bright_yellow")
        step_text.append(" " + message, style="bold yellow on black")
    elif step_type == "fallback_panel":
        # Create panel for fallback processing warnings
        from rich.panel import Panel
        fallback_panel = Panel(
            f"[bold bright_yellow]{message}[/bold bright_yellow]",
            title="⚠️ Fallback Processing",
            title_align="left",
            style="bold yellow",
            border_style="bright_yellow",
            padding=(0, 1)
        )
        return tree.add(fallback_panel)
    elif step_type == "processing":
        step_text.append("🔄", style="bold bright_cyan")
        step_text.append(" " + message, style="italic cyan")
    elif step_type == "semantic":
        step_text.append("🧠", style="bold blink bright_magenta")
        step_text.append(" " + message, style="bold magenta on black underline")
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
            f"[bold red]{message}[/bold red]",
            title="❌ Semantic Failed",
            title_align="left",
            style="bold red",
            border_style="bright_red",
            padding=(0, 1)
        )
        return tree.add(semantic_panel)
    elif step_type == "semantic_progress_panel":
        # Create panel for semantic queue progress
        from rich.panel import Panel
        from rich.table import Table
        from rich.text import Text
        
        # Parse progress data from message (format: "completed,failed,total,filename")
        try:
            parts = message.split(',')
            completed, failed, total = map(int, parts[:3])
            filename = parts[3] if len(parts) > 3 else ""
        except:
            completed, failed, total = 0, 0, 0
            filename = ""
        
        # Calculate progress
        processed = completed + failed
        progress_ratio = processed / total if total > 0 else 0
        
        # Create progress bar
        bar_width = 20
        filled_width = int(bar_width * progress_ratio)
        empty_width = bar_width - filled_width
        
        # Create progress table
        progress_table = Table(show_header=False, box=None, padding=(0, 0), expand=False)
        progress_table.add_column(style="bold bright_cyan", width=16)  # Label
        progress_table.add_column(width=25)  # Progress bar
        progress_table.add_column(style="bold bright_white on blue", width=12)  # Stats
        
        # Create progress bar text
        progress_bar_text = Text()
        progress_bar_text.append("[", style="dim white")
        progress_bar_text.append("█" * filled_width, style="bold bright_green")
        progress_bar_text.append("░" * empty_width, style="dim white")
        progress_bar_text.append("]", style="dim white")
        
        progress_table.add_row(
            "📊 Queue:",
            progress_bar_text,
            f"{processed}/{total}"
        )
        
        # Add completion/failure stats
        stats_table = Table(show_header=False, box=None, padding=(0, 0), expand=False)
        stats_table.add_column(style="bold green", width=16)
        stats_table.add_column(style="bold red", width=16)
        stats_table.add_row(
            f"✅ Done: {completed}" if completed > 0 else "",
            f"❌ Error: {failed}" if failed > 0 else ""
        )
        
        # Create panel content without filename
        from rich.console import Group
        panel_content = Group(progress_table, stats_table)
        
        semantic_panel = Panel(
            panel_content,
            title="🧠 Queued for semantic processing",
            title_align="left",
            style="bold cyan",
            border_style="bright_cyan",
            padding=(0, 1)
        )
        return tree.add(semantic_panel)
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
        # Create a table for file save information
        from rich.table import Table
        
        file_table = Table(show_header=False, box=None, padding=(0, 0), collapse_padding=True)
        file_table.add_column(style="bold blink", width=2, no_wrap=True)  # Icon column
        file_table.add_column(style="bold white underline", min_width=20, no_wrap=True)  # File info  
        file_table.add_column(style="bold bright_yellow on black", justify="right", width=15, no_wrap=True)  # Timing
        
        # Choose icon and color based on file type with enhanced styles
        if "HTML" in message:
            icon = "✅"  # Green checkmark for HTML files
            file_style = "bold bright_green italic"
        elif "MARKDOWN" in message:
            icon = "✅"  # Green checkmark for Markdown files
            file_style = "bold bright_green italic"
        elif "DOCX" in message:
            icon = "📄"
            file_style = "bold bright_red italic"
        else:
            icon = "✅"  # Default to green checkmark
            file_style = "bold bright_green italic"
        
        # Extract just the file type, not the full filename
        if "HTML" in message:
            file_info = " HTML file saved"  # Single space before text
        elif "MARKDOWN" in message:
            file_info = " Markdown file saved"  # Single space before text
        elif "DOCX" in message:
            file_info = " DOCX file saved"  # Single space before text
        else:
            file_info = " File saved"  # Single space before text
        
        # Show timing for files that have it, empty for others
        if " (in " in message and "s)" in message:
            timing_part = message.split(" (in ")[1]
            timing = "⏱️ (" + timing_part  # Show timing for files with conversion time
        else:
            timing = ""  # No timing display needed since icon is already checkmark
            
        file_table.add_row(icon, f"[{file_style}]{file_info}[/{file_style}]", timing)
        return tree.add(file_table)
    else:
        step_text.append(message, style=style)
    
    return tree.add(step_text)

def print_processing_tree_final(tree: Tree, page_num: int, domain: str = "unknown"):
    """Print the final tree wrapped in a panel with enhanced styling."""
    from rich.text import Text
    from rich.panel import Panel
    from rich.table import Table
    
    # Add enhanced completion message 
    from rich.text import Text
    completion_text = Text()
    completion_text.append("🎉", style="bold blink bright_green")
    completion_text.append(f" Page {page_num} completed successfully!", style="bold bright_green on black underline")
    tree.add(completion_text)
    
    # Wrap the entire tree in a domain panel
    from rich.panel import Panel
    tree_panel = Panel(
        tree,
        title=f"🌐 Domain: {domain}",
        title_align="left",
        style="bold blue",
        border_style="bright_blue",
        padding=(0, 1),
        expand=False
    )
    
    # Print the wrapped tree
    console.print()
    console.print(tree_panel)
    # Removed separator line - clean output

def print_page_tree(tree: Tree):
    """Print a page processing tree."""
    console.print(tree)

def add_tree_step(tree: Tree, message: str, style: str = "green"):
    """Add a processing step to the tree."""
    tree.add(f"[{style}]{message}[/{style}]")

def print_tree_separator():
    """Print a separator line after tree processing."""
    console.print("[dim]" + "═" * 50 + "[/dim]")