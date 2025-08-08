# Crawl4AI + Docling Web Scraper

A modular web scraping application that crawls specified domains and converts HTML content to various formats (Markdown, HTML, DOCX) using Crawl4AI and Docling.

## Features

- **Modular Architecture**: Clean separation of concerns with dedicated modules for crawling, HTML processing, document conversion, and file management
- **Multiple Output Formats**: Support for Markdown, HTML, and DOCX output formats
- **Advanced HTML Cleaning**: Global and domain-specific HTML cleaning with CSS-based element removal
- **JavaScript Execution**: Support for JavaScript execution with wait conditions for dynamic content
- **Domain Auto-Detection**: Automatically applies domain-specific settings when encountering configured domains
- **Configurable Processing**: Flexible YAML-based configuration system

## Quick Start

### Prerequisites

- Python 3.8+
- pip package manager

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd Craw4Ai_Docling
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Setup Crawl4AI browser:
```bash
crawl4ai-setup
```

4. Install Playwright browsers (required for Crawl4AI):
```bash
python -m playwright install
```

**Note**: If you encounter an error like "Executable doesn't exist at ms-playwright/chromium", run the Playwright install command above to download the necessary browser binaries.

### Basic Usage

**Recommended**: Use the crash-resistant wrapper:
```bash
# Windows
crawl.bat

# Or directly
python crawler_wrapper.py

# With options
python crawler_wrapper.py --domain myt.mu --formats markdown
```

**Alternative**: Direct crawler (may crash on problematic pages):
```bash
python main_new.py
```

1. Configure domains in `config.yaml`:
```yaml
domains:
  - domain: "example.com"
    start_urls:
      - "https://example.com/"
```

2. Run the crawler:
```bash
python main_new.py
```

## Configuration

The application uses a YAML configuration file (`config.yaml`) with the following main sections:

### Crawler Settings
```yaml
crawler:
  crawl4ai:
    verbose: true
    delay_before_return_html: 2.5
    max_pages: 5
    exclude_section_urls: true
  output_formats:
    - "markdown"
    - "html"
    - "docx"
  file_manager:
    use_domain_subfolders: true
    delete_existing_folders: true
```

### Global HTML Cleaning
```yaml
html_cleaning:
  remove_css_hidden_elements: true
  html_elements_to_remove:
    - "header"
    - "footer"
    - "nav"
  html_classes_to_remove:
    - ".sidebar"
    - ".hidden"
  comment_blocks_to_remove:
    - ["<!-- start -->", "<!-- end -->"]
```

### Domain-Specific Configuration
```yaml
domains:
  - domain: "example.com"
    start_urls:
      - "https://example.com/"
    js_code: |
      document.querySelectorAll('header, footer').forEach(el => el.remove());
    wait_for: "js:() => document.querySelectorAll('.content-loaded').length > 0"
    html_classes_to_only_include:
      - ".main-content"
```

### Markdown Processing
```yaml
markdown_processing:
  sections_to_ignore:
    - "Contact Us"
    - "Related Articles"
```

## Command Line Options

```bash
# Crawl all configured domains
python main_new.py

# Crawl specific domain
python main_new.py --domain example.com

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

## Project Structure

```
├── main_new.py                 # Main application entry point
├── config.yaml                 # Configuration file
├── requirements.txt            # Python dependencies
├── src/
│   ├── __init__.py
│   ├── crawler_orchestrator.py # Main orchestration logic
│   ├── web_crawler.py          # Crawl4AI operations
│   ├── html_processor.py       # HTML cleaning and processing
│   ├── document_converter.py   # Docling document conversion
│   └── file_manager.py         # File operations and management
├── crawled_html/               # Raw HTML output (if enabled)
└── crawled_pages/              # Processed output files
    ├── example.com/            # Domain-specific subdirectories
    │   ├── page1.md
    │   ├── page2.md
    │   └── ...
    └── ...
```

## Core Components

### WebCrawler (`src/web_crawler.py`)
- Manages Crawl4AI operations with async context
- Handles crawling queue and URL discovery
- Supports domain-specific JavaScript execution
- Provides streaming crawling for immediate processing

### HTMLProcessor (`src/html_processor.py`)
- Cleans HTML content using global and domain-specific rules
- Removes CSS-hidden elements and unwanted tags
- Converts HTML links to Markdown format
- Preserves existing markdown links in content

### DocumentConverter (`src/document_converter.py`)
- Converts HTML to various formats using Docling
- Supports Markdown, HTML, and DOCX output
- Performs markdown post-processing to remove sections
- Adds source URL headers to markdown files

### FileManager (`src/file_manager.py`)
- Manages output directories and file operations
- Generates safe filenames from URLs
- Supports domain-specific subdirectories
- Handles Windows file system compatibility

### CrawlerOrchestrator (`src/crawler_orchestrator.py`)
- Coordinates all components
- Loads and validates configuration
- Manages streaming processing workflow
- Provides detailed progress reporting

## Advanced Features

### JavaScript Execution
Configure JavaScript to run on pages before content extraction:
```yaml
domains:
  - domain: "dynamic-site.com"
    js_code: |
      // Click "Load More" buttons
      document.querySelectorAll('.load-more').forEach(btn => btn.click());
      
      // Wait for content to load
      await new Promise(resolve => setTimeout(resolve, 2000));
    wait_for: "js:() => document.querySelectorAll('.content-loaded').length > 0"
```

### HTML Cleaning Rules
- **Global rules**: Applied to all domains
- **Domain-specific rules**: Merged with global rules
- **Only include filtering**: Keep only content from specified CSS classes
- **CSS hidden element removal**: Automatically remove elements hidden by CSS



## Output

The crawler generates organized output:
- **Domain subdirectories**: Each domain gets its own folder
- **Source URLs**: Markdown files include source URL headers
- **Multiple formats**: Simultaneous generation of different formats
- **Safe filenames**: URL-to-filename conversion for Windows compatibility

## Dependencies

- **crawl4ai**: Web crawling with JavaScript support
- **docling**: Document conversion library
- **beautifulsoup4**: HTML parsing and manipulation
- **pyyaml**: YAML configuration file parsing
- **asyncio**: Asynchronous operations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Troubleshooting

### Common Issues

1. **No pages crawled**: Check domain accessibility and start URLs
2. **JavaScript not working**: Verify wait conditions and delays
3. **File permission errors**: Run with appropriate permissions on Windows
4. **Memory issues**: Reduce max_pages or enable streaming processing
5. **Playwright browser not found**: Run `python -m playwright install` to download browser binaries

### Debug Options

```bash
# Run with verbose output
python main_new.py --verbose

# Validate configuration
python main_new.py --validate

# Test with single domain
python main_new.py --domain example.com --formats markdown
```

For more detailed information, see `CLAUDE.md` in the repository.