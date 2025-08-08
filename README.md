# Web Scraper with Crash Recovery

Robust web scraping application that crawls domains and converts content to Markdown/HTML/DOCX with automatic crash recovery and retry mechanisms.

## Key Features

- **Crash Recovery**: Automatically handles browser crashes and restarts crawling
- **Smart Retry**: Configurable retry attempts for failed pages
- **Duplicate Removal**: Removes duplicate content and files automatically  
- **PDF Processing**: Extracts and converts PDF content to text
- **Checkpoint System**: Resume crawling from where it crashed
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
# Recommended - with crash recovery
crawl.bat

# Or
python crawler_wrapper.py --domain example.com
```

### Configure
Edit `config.yaml` - add domains and settings:
```yaml
domains:
  - domain: "example.com"
    start_urls: ["https://example.com/"]
    js_code: "document.querySelectorAll('header, footer').forEach(el => el.remove());"
```

## Configuration

| Feature | Config | Description |
|---------|--------|-------------|
| **Retry** | `max_retries: 3` | Retry failed pages |  
| **Recovery** | `save_checkpoint_every: 10` | Save progress |
| **Duplicates** | `remove_duplicate_lines: true` | Remove duplicate content |
| **Blanks** | `remove_blank_files: true` | Remove empty files |
| **PDFs** | `process_pdf_links: true` | Extract PDF content |

## Output

- **Files**: `crawled_pages/` (Markdown/HTML/DOCX)
- **PDFs**: `crawled_pdf/` (Extracted content)
- **Reports**: `failed_urls.txt`, `problematic_urls.txt`

## Troubleshooting

- **Browser not found**: Run `python -m playwright install`
- **Crashes**: Use `crawler_wrapper.py` for auto-recovery  
- **Memory issues**: Reduce `max_pages` in config
- **Permission errors**: Run as administrator on Windows

## License

MIT License
