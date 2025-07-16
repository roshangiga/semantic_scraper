#!/usr/bin/env python3
"""
Main entry point for the web crawler application.
"""

import argparse
import asyncio
import sys
import os
import warnings
from pathlib import Path

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    # Suppress Windows-specific asyncio warnings
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed transport")
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*pipe")

from src.crawler_orchestrator import CrawlerOrchestrator


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
        print(f"Error: Configuration file not found: {config_path}")
        return False
    
    try:
        with open(config_path, 'r') as f:
            f.read()
        return True
    except PermissionError:
        print(f"Error: Cannot read configuration file: {config_path}")
        return False
    except Exception as e:
        print(f"Error reading configuration file: {e}")
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
    
    print("\n=== Crawling Results ===")
    
    # Print processed pages
    processed_pages = results.get('processed_pages', [])
    print(f"Successfully processed: {len(processed_pages)} pages")
    
    if processed_pages and not quiet:
        print("Processed URLs:")
        for url in processed_pages:
            print(f"  + {url}")
    
    # Print errors
    errors = results.get('errors', [])
    if errors:
        print(f"\nErrors: {len(errors)}")
        for error in errors:
            print(f"  - {error['url']}: {error['error']}")
    
    # Print statistics
    stats = results.get('stats', {})
    if stats:
        print(f"\nFiles generated:")
        print(f"  HTML files: {stats.get('html_files', 0)}")
        print(f"  Markdown files: {stats.get('markdown_files', 0)}")
        print(f"  DOCX files: {stats.get('docx_files', 0)}")
        print(f"  Total files: {stats.get('total_files', 0)}")
    
    print("=" * 24)


async def main():
    """Main function."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose, args.quiet)
    
    # Validate configuration file
    if not validate_config_file(args.config):
        sys.exit(1)
    
    # Initialize orchestrator
    try:
        orchestrator = CrawlerOrchestrator(args.config)
    except Exception as e:
        print(f"Error initializing crawler: {e}")
        sys.exit(1)
    
    # Handle delete folders option
    if args.delete_folders:
        orchestrator.file_manager.delete_existing_folders = True
    
    # Validate configuration
    config_errors = orchestrator.validate_config()
    if config_errors:
        print("Configuration validation errors:")
        for error in config_errors:
            print(f"  - {error}")
        
        if args.validate:
            sys.exit(1)
        else:
            print("\nContinuing with invalid configuration...")
    
    # Handle validate-only mode
    if args.validate:
        if not config_errors:
            print("Configuration is valid")
        sys.exit(0)
    
    # Handle summary mode
    if args.summary:
        orchestrator.print_config_summary()
        sys.exit(0)
    
    # Determine output formats
    output_formats = args.formats if args.formats else None
    
    try:
        # Run crawler
        if args.domain:
            # Crawl specific domain
            if not args.quiet:
                print(f"Crawling domain: {args.domain}")
            results = await orchestrator.crawl_domain(args.domain, output_formats)
        else:
            # Crawl all domains
            if not args.quiet:
                print("Crawling all configured domains...")
            results = await orchestrator.crawl_and_convert(output_formats)
        
        # Handle errors
        if 'error' in results:
            print(f"Error: {results['error']}")
            sys.exit(1)
        
        # Print results
        print_results(results, args.quiet)
        
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
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