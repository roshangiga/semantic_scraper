# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a modular web scraping application that crawls specified domains and converts HTML content to various formats (Markdown, HTML, DOCX). It features a clean architecture with separation of concerns, configurable settings, flexible output options, and streaming processing for immediate page conversion.

## Setup Instructions

1. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the initial setup for Crawl4AI browser:
   ```bash
   crawl4ai-setup
   ```

3. Run the crawler:
   ```bash
   python main_new.py
   ```

## Core Dependencies

- **crawl4ai**: Web crawling framework with JavaScript execution capabilities
- **docling**: Document converter for HTML to Markdown/DOCX conversion
- **beautifulsoup4**: HTML parsing and link extraction
- **PyYAML**: Configuration file parsing

## Project Architecture

### Modular Structure

The project is organized into separate modules with clear responsibilities:

```
src/
├── __init__.py
├── web_crawler.py          # Crawl4AI operations
├── html_processor.py       # HTML cleaning and link processing
├── document_converter.py   # Docling operations
├── file_manager.py         # File operations and directory management
└── crawler_orchestrator.py # Main orchestration logic
```

### Core Components

1. **WebCrawler** (`src/web_crawler.py`)
   - Handles Crawl4AI operations with async context manager
   - Manages crawling queue and visited URLs tracking
   - Extracts links and discovers new pages
   - Domain-specific configuration support with automatic detection
   - Supports streaming crawling for immediate processing

2. **HTMLProcessor** (`src/html_processor.py`)
   - Cleans HTML content by removing unwanted elements
   - Automatically removes empty elements (div, p, span, section, article, aside, main, h1-h6, ul, ol, li)
   - Processes links to markdown format while preserving existing markdown links
   - Handles image link filtering and section URL exclusion
   - Creates temporary files for document conversion
   - Merges global and domain-specific HTML cleaning settings
   - Supports CSS-based element removal and "only include" filtering

3. **DocumentConverter** (`src/document_converter.py`)
   - Wraps Docling functionality for various output formats
   - Supports Markdown, HTML, and DOCX conversion
   - Configurable conversion parameters
   - Automatic temporary file cleanup
   - Markdown post-processing to remove unwanted sections
   - Adds source URL headers to markdown files

4. **FileManager** (`src/file_manager.py`)
   - Manages output directories and file operations
   - Generates safe filenames from URLs
   - Handles multiple output formats
   - Provides file statistics and cleanup options
   - Supports domain-specific subdirectories
   - Windows-compatible file deletion with retry logic

5. **CrawlerOrchestrator** (`src/crawler_orchestrator.py`)
   - Coordinates all components
   - Loads and validates configuration
   - Manages the complete crawling workflow with streaming processing
   - Provides domain-specific crawling options
   - Displays detailed configuration information at startup
   - Handles real-time page processing and error reporting

### Configuration System

The application uses YAML configuration files (`config.yaml`) with the following structure:

```yaml
crawler:
  crawl4ai:        # Crawl4AI settings (verbose, delay_before_return_html, max_pages, etc.)
  docling:         # Docling conversion settings
  file_manager:    # File management settings (directories, domain subfolders)
  output_formats:  # List of output formats

# Global HTML cleaning settings (applied to all domains)
html_cleaning:
  remove_css_hidden_elements: true
  html_elements_to_remove: []
  html_classes_to_remove: []
  comment_blocks_to_remove: []

# Markdown post-processing settings
markdown_processing:
  sections_to_ignore: []

domains:           # Domain-specific configurations
  - domain: "example.com"
    start_urls: []
    js_code: ""
    wait_for: null   # Wait condition for JavaScript execution
    html_elements_to_remove: []
    html_classes_to_remove: []
    html_classes_to_only_include: []  # Only keep content from these classes
    comment_blocks_to_remove: []

link_processing:   # Link processing settings
```

## Common Development Tasks

### Adding New Domains

Edit `config.yaml` and add a new domain configuration:

```yaml
domains:
  - domain: "new-domain.com"
    start_urls:
      - "https://new-domain.com/"
    js_code: |
      document.querySelectorAll('header, footer').forEach(el => el.remove());
    html_elements_to_remove:
      - "header"
      - "footer"
```

### Modifying HTML Cleaning Rules

HTML cleaning settings can be configured at two levels:

1. **Global settings** (applied to all domains):
```yaml
html_cleaning:
  remove_css_hidden_elements: true
  html_elements_to_remove: ["header", "footer", "nav"]
  html_classes_to_remove: [".sidebar", ".hidden"]
  comment_blocks_to_remove: [["<!-- start -->", "<!-- end -->"]]
```

2. **Domain-specific settings** (merged with global settings):
```yaml
domains:
  - domain: "example.com"
    html_elements_to_remove: ["aside"]  # Added to global list
    html_classes_to_remove: [".navbar"]  # Added to global list
    html_classes_to_only_include: [".main-content"]  # Only keep these classes
```

Note: Empty elements are automatically removed by default (div, p, span, section, article, aside, main, h1-h6, ul, ol, li).

### Configuring Output Formats

Modify the `output_formats` list in `config.yaml`:
```yaml
crawler:
  output_formats:
    - "html"
    - "markdown"
    - "docx"
```

### Running the Crawler

```bash
# Crawl all configured domains
python main_new.py

# Crawl specific domain
python main_new.py --domain myt.mu

# Generate specific formats
python main_new.py --formats markdown docx

# Use custom config file
python main_new.py --config custom.yaml

# Delete existing folders before crawling
python main_new.py --delete-folders

# Validate configuration
python main_new.py --validate

# Show configuration summary
python main_new.py --summary
```

### Link Preservation

The HTMLProcessor automatically converts HTML links to Markdown format while:
- Converting relative URLs to absolute URLs
- Filtering out image links
- Excluding URLs with section fragments (#) if configured
- Preserving existing markdown links in HTML content
- Preserving the link text and URL in `[text](url)` format

### Testing and Validation

```bash
# Validate configuration
python main_new.py --validate

# Run with verbose output
python main_new.py --verbose

# Run specific domain for testing
python main_new.py --domain test-domain.com --formats markdown
```

### Configuration Management

The configuration system supports:
- Domain-specific JavaScript injection
- Custom HTML cleaning rules per domain
- Flexible output format selection
- Directory management options
- Crawl4AI and Docling parameter configuration

### Error Handling

The application includes comprehensive error handling:
- Configuration validation
- Graceful handling of inaccessible domains
- Per-page error tracking without stopping the entire process
- Detailed error reporting with URLs and error messages
- Windows-specific file operation handling with retry logic
- Automatic cleanup of temporary files
- Robust handling of None values in configuration

## Key Features

### Streaming Processing
- Pages are processed immediately after crawling instead of waiting for all pages to be crawled
- Reduces memory usage and provides faster feedback
- Real-time progress reporting with detailed logging

### Advanced Configuration
- Global HTML cleaning settings merged with domain-specific settings
- JavaScript execution with wait conditions for dynamic content
- Markdown post-processing to remove unwanted sections
- Domain subfolder organization for output files

### Domain Auto-Detection
- Automatically applies domain-specific settings when encountering configured domains
- Supports crawling across multiple configured domains in a single run
- Graceful fallback to default settings for unconfigured domains

### Windows Compatibility
- Handles Windows file system limitations
- Retry logic for file operations
- Proper asyncio event loop management

## Legacy Code

The original `main.py` contains the legacy implementation. The new modular architecture is in `main_new.py` and the `src/` directory. Use the new implementation for all development work.