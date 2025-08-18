# Semantic Web Scraper

Robust enterprise-grade web scraping application with crash recovery, semantic chunking, and RAG integration. Features automatic retry mechanisms, checkpoint system, duplicate removal, and intelligent content processing.


![Crawler Configuration](Screenshot%202025-08-18%20215404.png)

![Processing Results](Screenshot%202025-08-18%20215455.png)

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
┌─────────────────────┐    
│  📄 PHASE 2         │    
│  Content Processing │    
│  (Crawl4AI + JS)   │    
└─────────┬───────────┘    
          │
          ▼
┌─────────────────────┐    ┌──────────────────────┐
│  📝 DOCLING         │    │  🔄 Fallback         │
│  HTML → Markdown   │◄──►│  BeautifulSoup       │
│  Tables & Charts   │    │  (on failure)        │
└─────────┬───────────┘    └──────────────────────┘
          │
          ▼
┌─────────────────────┐    ┌──────────────────────┐
│  🧠 Semantic        │    │  📤 RAG Upload       │
│  Contextual Chunks │───►│  Real-time Stream    │
│  (Gemini/OpenAI)   │    │  (RAGFlow)           │
└─────────┬───────────┘    └──────────────────────┘
          │
          ▼
    ✨ Final Result:
    🔗 Maximum Links + 📄 Clean Content + 🧠 Smart Chunks
```

**🚀 Complete Processing Pipeline:**
- **Phase 1** 🔗: Raw HTML → Extract ALL navigation links → Cache  
- **Phase 2** 📄: Crawl4AI → JavaScript cleanup → Clean HTML
- **Docling** 📝: Advanced conversion → Markdown/DOCX with tables/charts → Fallback to BeautifulSoup
- **Semantic** 🧠: Contextual chunking → Smart segments → Real-time RAG upload
- **Result** ✨: Maximum link discovery + pristine content + intelligent chunks

## Project Structure

```
src/
├── cli/
│   └── progress_formatter.py       # Real-time CLI progress formatting
├── scraper/
│   ├── web_crawler.py              # Two-phase crawling (aiohttp + Crawl4AI)
│   ├── html_processor.py           # HTML cleaning and link processing  
│   ├── document_converter.py       # Docling operations with duplicate removal
│   ├── file_manager.py             # File operations and directory management
│   ├── pdf_processor.py            # PDF extraction and processing
│   ├── crawler_orchestrator.py     # Main orchestration logic
│   └── report_generator.py         # Crawl reports and statistics
├── semantic/
│   ├── sequential_processor.py     # Queue-based semantic processing
│   ├── providers/
│   │   ├── gemini_client.py       # Gemini API integration  
│   │   ├── openai_client.py       # OpenAI API integration
│   │   └── azure_client.py        # Azure OpenAI integration
│   └── process_single_file.py     # Individual file processing
└── rag_clients/
    ├── rag_uploader.py             # RAG system integration
    └── ragflow/
        └── add_chunk.py            # RAGFlow API client
```

## Key Features

### Core Capabilities
- **Two-Phase Crawling**: Raw HTML link extraction + JavaScript content processing
- **Smart Link Discovery**: Preserves navigation links before JavaScript cleanup
- **Sequential Semantic Processing**: Queue-based contextual chunking with threading
- **Duplicate Removal**: Removes duplicate content and files automatically  
- **PDF Processing**: Extracts and converts PDF content to text
- **Intelligent Caching**: Avoids re-fetching same URLs
- **Multiple Formats**: Output as Markdown, HTML, or DOCX

### Document Conversion with Docling
- **Advanced Document Processing**: Uses IBM's Docling for high-quality HTML to Markdown/DOCX conversion
- **Table & Chart Support**: Preserves complex document structures including tables and charts
- **Annotation Preservation**: Maintains document annotations and formatting
- **Smart Fallback**: Automatically falls back to BeautifulSoup when Docling fails
- **Memory Protection**: File size limits (10MB default) to prevent crashes
- **Pattern Detection**: Pre-emptively detects problematic content patterns

### Reliability & Recovery
- **Crash Recovery System**: Automatic restart with checkpoint resumption
- **Problematic URL Detection**: Identifies and excludes crash-causing URLs
- **Smart Retry Logic**: 3 attempts per page with configurable delays
- **Docling Crash Protection**: Handles segmentation faults with graceful fallback
- **Memory-Safe Processing**: File size limits and pattern detection for problematic content

### RAG Integration
- **RAGFlow Support**: Direct upload to RAGFlow datasets with streaming
- **Automatic Dataset Creation**: Creates datasets/documents as needed
- **Retry with Backoff**: Handles 504 timeouts and server errors gracefully
- **Duplicate Prevention**: Tracks uploaded files to prevent duplicates
- **Metadata Preservation**: Maintains source URLs and timestamps

## Quick Start

### Install
```bash
pip install -r requirements.txt
crawl4ai-setup
python -m playwright install
```

### Run
```bash
# Run with crash recovery (recommended)
crawl.bat
# or
python crawler_wrapper.py

# Run directly (no crash recovery)
python main_new.py

# Crawl specific domain
python main_new.py --domain example.com

# Generate specific formats
python main_new.py --formats markdown docx

# Resume from checkpoint after crash
python main_new.py --resume

# Delete existing folders before crawling
python main_new.py --delete-folders
```

### Configure
Edit `config.yaml` - add domains and settings:
```yaml
domains:
  - domain: "example.com"
    start_urls: ["https://example.com/"]
    js_code: "document.querySelectorAll('header, footer').forEach(el => el.remove());"
```

## Complete Processing Pipeline

### Phase 1: Raw HTML Link Extraction 🔗
- Uses `aiohttp` to fetch raw HTML (preserves navigation elements)
- Extracts ALL links with BeautifulSoup before any JavaScript processing
- **Caches** fetched HTML to avoid re-fetching same URLs
- Tracks cache hits: `💾 Using cached raw HTML: url`

### Phase 2: Crawl4AI Content Processing 📄
- Uses Crawl4AI with JavaScript cleaning (removes header/nav/footer)
- Processes content for document conversion
- If Crawl4AI fails but links were extracted, still returns the links

### Document Conversion with Docling 📝
- **Primary Converter**: IBM's Docling for high-quality HTML to Markdown/DOCX
- **Structure Preservation**: Maintains tables, charts, formulas, and annotations
- **Smart Protection**: Pre-checks for problematic patterns before processing
- **Automatic Fallback**: Uses BeautifulSoup if Docling crashes or fails
- **Memory Safety**: 10MB file size limit to prevent memory issues

### Sequential Semantic Processing 🧠
- **Producer-Consumer Pattern**: Crawler adds tasks to queue in parallel during crawling
- **Consumer**: Single worker thread processes tasks sequentially in background
- **Thread-safe**: All operations protected with locks
- **Smart Messaging**: Real-time progress updates and completion notifications

### RAG Upload Pipeline 📤
- **Real-time Streaming**: Uploads semantic chunks as they're processed
- **Automatic Organization**: Creates datasets by timestamp and domain
- **Retry Logic**: Handles 504 timeouts with exponential backoff
- **Duplicate Prevention**: Tracks uploaded files to avoid redundancy

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
    max_pages: 5000                  # Maximum HTML pages per domain (excludes PDFs)
    exclude_section_urls: true       # Skip URLs with # fragments
    follow_pdf_redirects: true       # Follow redirects to PDF files
    max_retries: 3                   # Retry attempts for failed pages
    retry_delay: 5                   # Delay between retry attempts (seconds)
    save_checkpoint_every: 10        # Save progress every N pages
```

#### Document Processing Settings (Docling)
```yaml
  docling:
    enabled: true                    # Use Docling (fallback to BeautifulSoup on failure)
    max_file_size_mb: 10            # Max file size for Docling processing
    # When disabled or on failure, uses BeautifulSoup for simple text extraction
    
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

**Docling Features:**
- Converts HTML to high-quality Markdown/DOCX with structure preservation
- Handles complex tables, charts, and mathematical formulas
- Maintains document hierarchy and formatting
- Automatic fallback to BeautifulSoup on crashes or problematic content
- Pre-emptive detection of crash-causing patterns (excessive forms, scripts, nested divs)

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
  provider: "gemini"                 # LLM provider: "openai", "azure", or "gemini"
  
  # OpenAI Configuration
  openai_model: "gpt-4o-mini"        # Model: gpt-4o, gpt-4o-mini, etc.
  # Uses OPENAI_API_KEY environment variable
  
  # Azure OpenAI Configuration (optional)
  azure_api_version: "2024-12-01-preview"    # Azure API version
  azure_endpoint: "https://your-resource.cognitiveservices.azure.com/"  # Azure endpoint
  azure_deployment: "gpt-4o-mini"            # Azure deployment name
  
  # Gemini Configuration  
  gemini_model: "gemini-2.5-flash"   # Model: gemini-2.5-flash, gemini-pro
  # Uses GEMINI_API_KEY environment variable
```

### RAG Upload Settings
```yaml
rag_upload:
  enabled: true                      # Enable automatic upload to RAG
  client: "ragflow"                  # RAG client: ragflow | defy
  streaming: true                    # Real-time upload during crawl
  # Uses RAGFLOW_URL and RAGFLOW_API_KEY environment variables
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
