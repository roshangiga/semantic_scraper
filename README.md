# Semantic Web Scrapper

Advanced web scraping application that crawls domains and converts content to Markdown/HTML/DOCX using a sophisticated two-phase approach for maximum link discovery.

## Process Flow

```
🌐 URL Input
    │
    ▼
┌─────────────────────┐    ┌──────────────────────┐
│  📥 PHASE 1         │    │  💾 Smart Caching    │
│  Raw HTML Fetch    │◄──►│  Avoid Re-fetching   │
│  (aiohttp)         │    │  Same URLs          │
└─────────┬───────────┘    └──────────────────────┘
          │
          ▼
┌─────────────────────┐
│  🔗 Link Discovery │
│  Extract ALL Links │ ─────► 🗂️  Queue New URLs
│  (BeautifulSoup)   │        for Crawling
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐    ┌──────────────────────┐
│  📄 PHASE 2         │    │  🧠 Semantic Queue   │
│  Content Processing │◄──►│  Contextual Chunking │
│  (Crawl4AI + JS)   │    │  Threading           │
└─────────┬───────────┘    └──────────────────────┘
          │
          ▼
    ✨ Final Result:
    🔗 Maximum Links + 📄 Clean Content
```

**🚀 Two-Phase Architecture Benefits:**
- **Phase 1** 🔗: Raw HTML → Extract ALL navigation links → Cache  
- **Phase 2** 📄: Crawl4AI → JavaScript cleanup → Clean content  
- **Result** ✨: Maximum link discovery + pristine content

## Key Features

- **Two-Phase Crawling**: Raw HTML link extraction + JavaScript content processing
- **Smart Link Discovery**: Preserves navigation links before JavaScript cleanup
- **Sequential Semantic Processing**: Queue-based contextual chunking with threading
- **Duplicate Removal**: Removes duplicate content and files automatically  
- **PDF Processing**: Extracts and converts PDF content to text
- **Intelligent Caching**: Avoids re-fetching same URLs
- **Multiple Formats**: Output as Markdown, HTML, or DOCX

## Quick Start

### Install
```bash
pip install -r requirements.txt
crawl4ai-setup
python -m playwright install
```

### Run
```bash
# Run the crawler
python main_new.py

# Crawl specific domain
python main_new.py --domain example.com

# Generate specific formats
python main_new.py --formats markdown docx
```

### Configure
Edit `config.yaml` - add domains and settings:
```yaml
domains:
  - domain: "example.com"
    start_urls: ["https://example.com/"]
    js_code: "document.querySelectorAll('header, footer').forEach(el => el.remove());"
```

## Two-Phase Crawling Architecture

### Phase 1: Raw HTML Link Extraction 🔗
- Uses `aiohttp` to fetch raw HTML (preserves navigation elements)
- Extracts ALL links with BeautifulSoup before any JavaScript processing
- **Caches** fetched HTML to avoid re-fetching same URLs
- Tracks cache hits: `💾 Using cached raw HTML: url`

### Phase 2: Crawl4AI Content Processing 📄
- Uses Crawl4AI with JavaScript cleaning (removes header/nav/footer)
- Processes content for document conversion
- If Crawl4AI fails but links were extracted, still returns the links


### Sequential Semantic Processing 🧠
- **Producer-Consumer Pattern**: Crawler adds tasks to queue in parallel during crawling
- **Consumer**: Single worker thread processes tasks sequentially in background
- **Thread-safe**: All operations protected with locks
- **Smart Messaging**: Real-time progress updates and completion notifications

## Configuration

### Core Crawler Settings (`config.yaml`)

#### Crawl4AI Settings
```yaml
crawler:
  crawl4ai:
    verbose: true                    # Enable detailed logging
    bypass_cache: true               # Always fetch fresh content
    delay_before_return_html: 2      # Wait 2s before returning HTML
    js_only: false                   # Execute JavaScript + return HTML
    max_pages: 1500                  # Maximum HTML pages per domain (excludes PDFs)
    exclude_section_urls: true       # Skip URLs with # fragments
    follow_pdf_redirects: true       # Follow redirects to PDF files
    max_retries: 3                   # Retry attempts for failed pages
    retry_delay: 5                   # Delay between retry attempts (seconds)
    save_checkpoint_every: 10        # Save progress every N pages
```

#### Document Processing Settings
```yaml
  docling:
    markdown:
      include_annotations: true      # Include document annotations
      mark_annotations: true         # Mark annotations in output
      escape_underscores: true       # Escape underscores for markdown
      image_placeholder: "<!-- image -->"  # Placeholder for images
      enable_chart_tables: true      # Process charts and tables
    html:
      include_annotations: true      # Include annotations in HTML
      formula_to_mathml: true        # Convert formulas to MathML
```

#### File Management
```yaml
  file_manager:
    delete_existing_folders: true             # Clear output folders on start
    pages_output_dir: "crawled_docling"       # Main content output
    pdf_output_dir: "crawled_pdf"             # PDF extraction output
    semantic_output_dir: "crawled_semantic"   # Contextual chunks output
    report_output_dir: "crawled_report"       # Crawl reports
    use_domain_subfolders: true              # Organize by domain
    filename_template: "{sanitized_url}"      # Filename pattern
```

### Content Processing Settings

#### HTML Cleaning (Global)
```yaml
html_cleaning:
  remove_css_hidden_elements: true   # Remove display:none elements
  html_elements_to_remove:           # Elements to strip
    - "head"
    - "header" 
    - "footer"
    - "nav"
    - "aside"
  html_classes_to_remove:            # CSS classes to remove
    - ".sidebar"
    - ".navbar"
    - ".header"
    - ".footer"
    - ".hidden"
```

#### Markdown Post-Processing
```yaml
markdown_processing:
  remove_duplicate_files: true       # Remove duplicate content files
  remove_blank_files: true           # Remove files with only source headers
  sections_to_ignore:                # Skip these section titles
    - "Contact Us"
    - "Quick links" 
    - "Related Products"
```

### Contextual Chunking Settings
```yaml
contextual_chunking:
  enabled: true                      # Enable semantic processing
  provider: "gemini"                 # LLM provider: "openai" or "gemini"
  
  # OpenAI Configuration (Standard or Azure)
  openai_model: "gpt-5-mini"         # Model: gpt-5-mini, gpt-4o, gpt-4o-mini, etc.
  # Uses OPENAI_API_KEY environment variable
  
  # Azure OpenAI Configuration (optional)
  azure_api_version: "2024-12-01-preview"    # Azure API version
  azure_endpoint: "https://your-resource.cognitiveservices.azure.com/"  # Azure endpoint
  azure_deployment: "gpt-5-mini"             # Azure deployment name
  
  # Gemini Configuration  
  gemini_model: "gemini-2.5-flash"   # Model: gemini-2.5-flash, gemini-pro
  # Uses GEMINI_API_KEY environment variable
```

### Link Processing Settings
```yaml
link_processing:
  exclude_section_urls: true         # Skip URLs with # fragments
  convert_relative_to_absolute: true # Convert relative URLs
  process_pdf_links: true            # Extract PDF content
  exclude_image_extensions:          # Skip these file types
    - ".jpg"
    - ".png" 
    - ".gif"
  exclude_urls:                      # Global URL patterns to skip
    - "**/login"
```

### Domain-Specific Configuration
```yaml
domains:
  - domain: "example.com"
    start_urls:
      - "https://example.com/"
    js_code: |                       # JavaScript to execute on page
      document.querySelectorAll('header, footer, nav').forEach(el => el.remove());
    html_elements_to_remove: []      # Domain-specific elements to remove
    html_classes_to_remove: []       # Domain-specific classes to remove  
    html_classes_to_only_include:    # Keep only these classes (if found)
      - ".main-content"
    exclude_urls:                    # Domain-specific URL exclusions
      - "**/admin/**"
```

## Project Structure

```
src/
├── cli/
│   └── progress_formatter.py   # Real-time CLI progress formatting
├── scraper/
│   ├── web_crawler.py          # Two-phase crawling (aiohttp + Crawl4AI)
│   ├── html_processor.py       # HTML cleaning and link processing  
│   ├── document_converter.py   # Docling operations with duplicate removal
│   ├── file_manager.py         # File operations and directory management
│   ├── crawler_orchestrator.py # Main orchestration logic
│   └── report_generator.py     # Crawl reports and statistics
└── semantic/
    ├── sequential_processor.py # Queue-based semantic processing
    ├── openai_client.py        # OpenAI integration
    ├── gemini_client.py        # Gemini integration  
    └── process_single_file.py  # Individual file processing
```

## Output Structure

- **Pages**: `crawled_docling/` (Markdown/HTML/DOCX by domain)
- **PDFs**: `crawled_pdf/` (Extracted PDF content)
- **Semantic**: `crawled_semantic/` (Contextual chunks as JSON)
- **Reports**: `crawled_report/` (Detailed crawl statistics)

## Troubleshooting

- **Browser not found**: Run `python -m playwright install`
- **Missing dependencies**: Run `pip install -r requirements.txt`
- **Memory issues**: Reduce `max_pages` in config
- **Permission errors**: Run as administrator on Windows
- **Link discovery issues**: Two-phase crawling now preserves all navigation links

## License

MIT License
