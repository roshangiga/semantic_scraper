#!/usr/bin/env python3
"""
Main entry point for the web crawler application.
"""

import argparse
import asyncio
import sys
import warnings
import subprocess
import os
from pathlib import Path
from dotenv import load_dotenv

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

    # Suppress Windows-specific asyncio warnings
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed transport")
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*pipe")

from src.scraper.crawler_orchestrator import CrawlerOrchestrator

# Import rich console utilities
try:
    from src.console import (
        console, print_success, print_error, print_warning,
        print_info, print_processing, print_panel, print_header,
        setup_rich_logging, create_table
    )
    RICH_AVAILABLE = True
except ImportError:
    # Fallback to regular print if rich not available
    RICH_AVAILABLE = False

    def print_error(msg):
        print(f"❌ {msg}")

    def print_success(msg):
        print(f"✅ {msg}")

    def print_warning(msg):
        print(f"⚠️ {msg}")

    def print_info(msg):
        print(f"ℹ️ {msg}")

    def print_processing(msg):
        print(f"🔄 {msg}")

    def print_header(msg):
        print(f"\n=== {msg} ===")

    def print_panel(title, content, style=None):
        print(f"\n{title}:\n{content}")

    def setup_rich_logging():
        pass


def start_ragflow_console():
    """Start the RAGFlow uploader console using exact same logic as semantic worker."""
    try:
        # Use EXACT same logic as semantic worker
        ragflow_script = 'ragflow_uploader.py'
        subprocess.Popen([
            sys.executable, ragflow_script
        ], creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0)
        
        print_info("🚀 RAGFlow Upload Console started in new console")
        return True
    except Exception as e:
        print_warning(f"Could not start RAGFlow console: {e}")
        return False


def check_rag_config(orchestrator):
    """Check if RAG upload is enabled and configured."""
    try:
        rag_config = orchestrator.config.get('rag_upload', {})
        return rag_config.get('enabled', False)
    except Exception:
        return False


def create_argument_parser():
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        description='Web crawler for converting web content to various formats',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                              # Crawl all configured domains
  %(prog)s --domain myt.mu              # Crawl specific domain
  %(prog)s --formats markdown           # Output only markdown
  %(prog)s --formats html markdown docx # Output multiple formats
  %(prog)s --config custom.yaml         # Use custom config file
  %(prog)s --delete-folders             # Delete existing output folders
  %(prog)s --validate                   # Validate configuration only
        """
    )

    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )

    parser.add_argument(
        '--domain',
        type=str,
        help='Crawl specific domain only'
    )

    parser.add_argument(
        '--formats',
        nargs='+',
        choices=['html', 'markdown', 'md', 'docx'],
        help='Output formats to generate'
    )

    parser.add_argument(
        '--delete-folders',
        action='store_true',
        help='Delete existing output folders before crawling'
    )

    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate configuration and exit'
    )

    parser.add_argument(
        '--summary',
        action='store_true',
        help='Print configuration summary and exit'
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume crawling from checkpoint if available'
    )

    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose output'
    )

    parser.add_argument(
        '--quiet',
        '-q',
        action='store_true',
        help='Suppress non-error output'
    )

    return parser


def setup_logging(verbose: bool, quiet: bool):
    """Setup logging configuration."""
    # Setup rich logging if available
    if RICH_AVAILABLE and not quiet:
        setup_rich_logging()

    if quiet:
        import logging
        logging.getLogger().setLevel(logging.ERROR)
    elif verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)


def validate_config_file(config_path: str) -> bool:
    """
    Validate that configuration file exists.

    Args:
        config_path: Path to configuration file

    Returns:
        True if file exists and is readable
    """
    if not Path(config_path).exists():
        print_error(f"Configuration file not found: {config_path}")
        return False

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            f.read()
        return True
    except PermissionError:
        print_error(f"Cannot read configuration file: {config_path}")
        return False
    except Exception as e:
        print_error(f"Error reading configuration file: {e}")
        return False


def print_results(results: dict, quiet: bool = False):
    """
    Print crawling results.

    Args:
        results: Results dictionary from crawler
        quiet: Whether to suppress output
    """
    if quiet:
        return

    print_header("Crawling Results")

    # Print processed pages
    processed_pages = results.get('processed_pages', [])
    print_success(f"Successfully processed: {len(processed_pages)} pages")

    # Print errors
    errors = results.get('errors', [])
    if errors:
        print_error(f"Errors: {len(errors)}")
        for error in errors:
            print_error(f"{error['url']}: {error['error']}")

    # Print statistics with rich table
    stats = results.get('stats', {})
    if stats and RICH_AVAILABLE:
        table = create_table("Files Generated")
        table.add_column("Format", style="cyan")
        table.add_column("Count", justify="right", style="magenta")

        table.add_row("HTML", str(stats.get('html_files', 0)))
        table.add_row("Markdown", str(stats.get('markdown_files', 0)))
        table.add_row("DOCX", str(stats.get('docx_files', 0)))
        table.add_row("PDF", str(stats.get('pdf_files', 0)))
        table.add_row("[bold]Total[/bold]", f"[bold]{stats.get('total_files', 0)}[/bold]")

        console.print(table)
    elif stats:
        # Fallback for non-rich output
        print_info("Files generated:")
        print(f"  HTML files: {stats.get('html_files', 0)}")
        print(f"  Markdown files: {stats.get('markdown_files', 0)}")
        print(f"  DOCX files: {stats.get('docx_files', 0)}")
        print(f"  PDF files: {stats.get('pdf_files', 0)}")
        print(f"  Total files: {stats.get('total_files', 0)}")

    # Show processed URLs in a panel if rich is available
    if processed_pages and RICH_AVAILABLE and len(processed_pages) <= 20:
        urls_text = "\n".join(f"✓ {url}" for url in processed_pages)
        print_panel("Processed URLs", urls_text, "green")
    elif processed_pages and not RICH_AVAILABLE:
        print_info("Processed URLs:")
        for url in processed_pages:
            print(f"  ✓ {url}")


async def main():
    """Main function."""
    # Load environment variables from .env file
    load_dotenv()

    parser = create_argument_parser()
    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose, args.quiet)

    # Show header (unless in quiet mode or just validating/showing summary)
    if not args.quiet and not args.validate and not args.summary:
        print_header("🚀 Craw4AI Docling - Web Crawler")

    # Validate configuration file
    if not validate_config_file(args.config):
        sys.exit(1)

    # Initialize orchestrator
    try:
        orchestrator = CrawlerOrchestrator(args.config)
    except Exception as e:
        print_error(f"Error initializing crawler: {e}")
        sys.exit(1)

    # Handle delete folders option
    if args.delete_folders:
        orchestrator.file_manager.delete_existing_folders = True

    # Validate configuration
    config_errors = orchestrator.validate_config()
    if config_errors:
        print_error("Configuration validation errors:")
        for error in config_errors:
            print_error(f"  {error}")

        if args.validate:
            sys.exit(1)
        else:
            print_warning("Continuing with invalid configuration...")

    # Handle validate-only mode
    if args.validate:
        if not config_errors:
            print_success("Configuration is valid")
        sys.exit(0)

    # Handle summary mode
    if args.summary:
        orchestrator.print_config_summary()
        sys.exit(0)

    # Start RAGFlow console if RAG upload is enabled
    if check_rag_config(orchestrator) and not args.quiet and not args.validate and not args.summary:
        start_ragflow_console()

    # Determine output formats
    output_formats = args.formats if args.formats else None

    try:
        # Run crawler
        if args.domain:
            # Crawl specific domain
            if not args.quiet:
                print_info(f"Crawling domain: {args.domain}")
            results = await orchestrator.crawl_domain(args.domain, output_formats)
        else:
            # Crawl all domains
            if not args.quiet:
                print_processing("Crawling all configured domains...")
            results = await orchestrator.crawl_and_convert(output_formats)

        # Handle errors
        if 'error' in results:
            print_error(f"Error: {results['error']}")
            sys.exit(1)

        # Print results
        print_results(results, args.quiet)

        # Show completion message
        if not args.quiet:
            print_success("Crawling completed successfully! 🎉")

    except KeyboardInterrupt:
        print_warning("\nCrawling interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def run():
    """Entry point for running the crawler."""
    if sys.platform == 'win32':
        # Set Windows event loop policy to prevent cleanup warnings
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    try:
        asyncio.run(main())
    finally:
        # Force cleanup on Windows
        if sys.platform == 'win32':
            import gc
            gc.collect()
            # Give asyncio time to clean up
            import time
            time.sleep(0.1)


if __name__ == '__main__':
    run()