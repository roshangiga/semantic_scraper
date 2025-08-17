"""
Web crawler module for handling Crawl4AI operations.
"""

import asyncio
import aiohttp
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, urljoin
from collections import deque
from crawl4ai import AsyncWebCrawler
# Support both legacy and new Crawl4AI configs
try:
    from crawl4ai import CrawlerRunConfig, CacheMode  # >= v0.7 API
except Exception:  # pragma: no cover - older versions won't have these
    CrawlerRunConfig = None
    CacheMode = None

from bs4 import BeautifulSoup


class WebCrawler:
    """Handles web crawling operations using Crawl4AI."""
    
    def __init__(self, config: Dict[str, Any], progress_formatter=None):
        """
        Initialize the WebCrawler.
        
        Args:
            config: Configuration dictionary containing crawl4ai settings
            progress_formatter: Optional CLI progress formatter
        """
        self.config = config
        self.crawler = None
        self.visited_urls = set()
        self.queue = deque()
        self.failed_urls = []  # Track failed URLs with reasons
        self.raw_html_cache = {}  # Cache for raw HTML to avoid re-fetching
        self.progress_formatter = progress_formatter
        self.semantic_queue_callback = None  # Callback to get semantic queue for checkpointing
        
    async def __aenter__(self):
        """Async context manager entry."""
        # Suppress ALL Crawl4AI logging for clean progress bar
        import logging
        logging.getLogger('crawl4ai').setLevel(logging.ERROR)
        logging.getLogger('httpx').setLevel(logging.WARNING)
        logging.getLogger('httpcore').setLevel(logging.WARNING)
        
        # Import the config classes
        from crawl4ai import BrowserConfig
        
        # Create browser config with verbose disabled
        browser_config = BrowserConfig(
            headless=True,
            verbose=False  # Disable browser-level verbose logging
        )
        
        self.crawler = AsyncWebCrawler(
            config=browser_config,
            verbose=False  # Disable crawler-level verbose logging
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        # AsyncWebCrawler doesn't have aclose method, no cleanup needed
        pass
    
    def initialize_crawl(self, domains: List[Dict[str, Any]]) -> None:
        """
        Initialize the crawling queue with start URLs.
        
        Args:
            domains: List of domain configurations
        """
        for domain_config in domains:
            start_urls = domain_config.get('start_urls', [])
            for url in start_urls:
                if url not in self.visited_urls:
                    # Skip section URLs (with # fragments) if configured
                    if self.config.get('exclude_section_urls', True) and '#' in url:
                        continue
                    
                    self.visited_urls.add(url)
                    self.queue.append(url)
    
    def get_domain_config(self, url: str, domains: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Get domain configuration for a given URL.
        
        Args:
            url: URL to get configuration for
            domains: List of domain configurations
            
        Returns:
            Domain configuration or None if not found
        """
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        for domain_config in domains:
            if domain_config['domain'] == domain:
                return domain_config
        return None
    
    def get_default_domain_config(self, url: str) -> Dict[str, Any]:
        """
        Get default domain configuration for URLs without specific config.
        
        Args:
            url: URL to create config for
            
        Returns:
            Default domain configuration
        """
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        return {
            'domain': domain,
            'js_code': '',
            'html_elements_to_remove': [],
            'html_classes_to_remove': [],
            'html_classes_to_only_include': [],
            'comment_blocks_to_remove': []
        }
    
    async def crawl_page_two_phase(self, url: str, domain_config: Dict[str, Any], allowed_domains: List[str], all_domains: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Two-phase crawling: fetch raw HTML for links, then use Crawl4AI for content.
        
        Args:
            url: URL to crawl
            domain_config: Domain-specific configuration  
            allowed_domains: List of allowed domains for link extraction
            all_domains: List of all domain configurations
            
        Returns:
            Dictionary containing crawl results with extracted links
        """
        # Phase 1: Fetch raw HTML for link extraction
        raw_html = await self.fetch_raw_html(url)
        links = {'pages': [], 'pdfs': []}
        
        if raw_html:
            # Extract links from raw HTML (before JavaScript processing)
            links = self.extract_links(raw_html, url, allowed_domains, all_domains)
            if self.progress_formatter:
                self.progress_formatter.log_links_found(len(links['pages']), len(links['pdfs']))
        
        # Phase 2: Use Crawl4AI for content processing (with JavaScript cleaning)
        crawl_result = await self.crawl_page(url, domain_config)
        
        if crawl_result:
            # Add extracted links to the result
            crawl_result['links'] = links
            return crawl_result
        else:
            # If Crawl4AI failed but we got links, return basic result
            if links['pages'] or links['pdfs']:
                print(f"   🔗 Crawl4AI failed but extracted {len(links['pages'])} links from raw HTML")
                return {
                    'url': url,
                    'html': raw_html or '',
                    'domain_config': domain_config,
                    'links': links,
                    'crawl4ai_failed': True
                }
            return None
    
    async def crawl_page(self, url: str, domain_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Crawl a single page with retry mechanism.
        
        Args:
            url: URL to crawl
            domain_config: Domain-specific configuration
            
        Returns:
            Dictionary containing crawl results or None if failed
        """
        if not self.crawler:
            raise RuntimeError("Crawler not initialized. Use async context manager.")
        
        max_retries = self.config.get('max_retries', 3)
        retry_delay = self.config.get('retry_delay', 5)
        
        for attempt in range(max_retries):
            try:
                result = await self._crawl_page_attempt(url, domain_config)
                if result:
                    return result
            except Exception as e:
                error_msg = str(e)
                # Don't retry for specific non-recoverable errors
                if "net::ERR_NAME_NOT_RESOLVED" in error_msg or "Incoming markup is of an invalid type: None" in error_msg:
                    return None
                    
                if attempt < max_retries - 1:
                    print(f"   ⚠️ Attempt {attempt + 1}/{max_retries} failed: {error_msg[:100]}")
                    print(f"   🔄 Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    print(f"   ❌ All {max_retries} attempts failed for {url}")
                    # Track failed URL
                    self.failed_urls.append({
                        'url': url,
                        'error': error_msg,
                        'attempts': max_retries
                    })
                    # Don't raise, just return None to continue with other pages
                    return None
        
        return None
    
    async def _crawl_page_attempt(self, url: str, domain_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Single crawl attempt for a page.
        
        Args:
            url: URL to crawl
            domain_config: Domain-specific configuration
            
        Returns:
            Dictionary containing crawl results or None if failed
        """
        try:
            # Get crawl4ai settings
            crawl_settings = self.config.copy()
            crawl_settings.update({
                'url': url,
                'js_code': domain_config.get('js_code', ''),
                'bypass_cache': crawl_settings.get('bypass_cache', True)
            })
            
            # Add domain-specific wait/wait_for if specified (support alias 'wait')
            domain_wait = domain_config.get('wait_for') or domain_config.get('wait')
            if domain_wait:
                crawl_settings['wait_for'] = domain_wait
            
            # Optional parameters normalization
            if crawl_settings.get('delay_before_return_html') is not None:
                crawl_settings['delay_before_return_html'] = float(crawl_settings['delay_before_return_html'])
            if crawl_settings.get('js_only') is not None:
                crawl_settings['js_only'] = bool(crawl_settings['js_only'])
            # Support 'wait' alias at top-level config too
            if crawl_settings.get('wait') and not crawl_settings.get('wait_for'):
                crawl_settings['wait_for'] = crawl_settings['wait']
            # Optional page timeout (ms)
            if crawl_settings.get('page_timeout') is not None:
                try:
                    crawl_settings['page_timeout'] = int(crawl_settings['page_timeout'])
                except Exception:
                    pass
            
            # Remove config keys that aren't crawl4ai parameters
            crawl_params = {k: v for k, v in crawl_settings.items() 
                          if k in ['url', 'js_code', 'bypass_cache', 'delay_before_return_html', 
                                  'js_only', 'wait_for', 'page_timeout']}

            # Prefer new-style configuration if available (Crawl4AI >= 0.7)
            if CrawlerRunConfig is not None:
                run_config_kwargs = {
                    'js_code': crawl_params.get('js_code', ''),
                    'wait_for': crawl_params.get('wait_for'),
                    'delay_before_return_html': crawl_params.get('delay_before_return_html'),
                    'page_timeout': crawl_params.get('page_timeout'),  # milliseconds
                    'verbose': False  # CRITICAL: Disable verbose logging to suppress [FETCH]/[SCRAPE]/[COMPLETE]
                }
                # Cache mode mapping (default to BYPASS when bypass_cache True)
                if CacheMode is not None:
                    bypass = bool(crawl_params.get('bypass_cache', True))
                    run_config_kwargs['cache_mode'] = CacheMode.BYPASS if bypass else CacheMode.DEFAULT
                run_config = CrawlerRunConfig(**{k: v for k, v in run_config_kwargs.items() if v is not None})

                # js_only remains a top-level flag in some versions; pass through when set
                js_only = bool(crawl_params.get('js_only')) if 'js_only' in crawl_params else None
                # Optional logs routed to progress formatter
                if crawl_params.get('wait_for'):
                    wf = str(crawl_params['wait_for'])
                    if self.progress_formatter:
                        self.progress_formatter._write("⏳ Applying wait_for…")
                    # Suppress technical wait condition messages to avoid timing issues
                if crawl_params.get('page_timeout') is not None:
                    try:
                        pt = int(crawl_params['page_timeout'])
                        if self.progress_formatter:
                            self.progress_formatter._write(f"⏱️ page_timeout={pt}ms")
                        # Suppress technical timeout messages to avoid timing issues
                    except Exception:
                        pass
                # Your installed version exposes parameter name `config` for arun
                if js_only is not None:
                    result = await self.crawler.arun(url=url, config=run_config, js_only=js_only)
                else:
                    result = await self.crawler.arun(url=url, config=run_config)
            else:
                # Legacy parameter style (older Crawl4AI versions)
                if crawl_params.get('delay_before_return_html') is not None:
                    try:
                        dly = float(crawl_params['delay_before_return_html'])
                        # Delay logging suppressed for clean progress bar
                    except Exception:
                        pass
                # Legacy parameter application - logging suppressed for clean progress bar
                # Add verbose=False for legacy mode too
                crawl_params['verbose'] = False
                result = await self.crawler.arun(**crawl_params)
            
            # Check if we were redirected to a PDF by looking for our marker in the HTML
            if result.cleaned_html and 'PDF_REDIRECT:' in result.cleaned_html:
                # Extract the PDF URL from the marker
                import re
                pdf_match = re.search(r'PDF_REDIRECT:([^\s<]+)', result.cleaned_html)
                if pdf_match:
                    final_url = pdf_match.group(1)
                    # Suppressed redirect message to keep progress clean
                    return {
                        'url': url,
                        'html': result.cleaned_html,
                        'domain_config': domain_config,
                        'pdf_redirect': final_url
                    }
            
            # Also check if the result object has URL information (for older detection)
            final_url = getattr(result, 'url', url)
            if hasattr(result, 'response_url'):
                final_url = result.response_url
            
            # If the final URL is a PDF, add it to our PDF queue
            if final_url.lower().endswith('.pdf'):
                # Suppressed redirect message to keep progress clean
                # Create a minimal HTML with the PDF link for processing
                pdf_html = f'<html><body><a href="{final_url}">PDF Document: {final_url}</a></body></html>'
                return {
                    'url': url,
                    'html': pdf_html,
                    'domain_config': domain_config,
                    'pdf_redirect': final_url
                }
            
            if not result.cleaned_html or result.cleaned_html.strip() == '':
                print(f"Warning: No content received from {url}")
                return None
            
            # Always use the original requested URL, not any URL that might have been modified by JavaScript
            # This prevents saving files with fragment identifiers when the page redirects
            return {
                'url': url,  # Use the original URL we requested
                'html': result.cleaned_html,
                'domain_config': domain_config
            }
            
        except Exception as e:
            error_msg = str(e)
            if "net::ERR_NAME_NOT_RESOLVED" in error_msg:
                print(f"Domain not accessible: {url}")
            elif "Incoming markup is of an invalid type: None" in error_msg:
                print(f"No valid content received from {url}")
            else:
                print(f"Error crawling {url}: {e}")
            return None
    
    async def fetch_raw_html(self, url: str, timeout: int = 30) -> Optional[str]:
        """
        Fetch raw HTML using aiohttp for link extraction (before JavaScript processing).
        Uses caching to avoid re-fetching the same URL.
        
        Args:
            url: URL to fetch
            timeout: Request timeout in seconds
            
        Returns:
            Raw HTML content or None if failed
        """
        # Check cache first
        if url in self.raw_html_cache:
            if self.progress_formatter:
                self.progress_formatter.log_raw_html_fetch(url, cached=True)
            return self.raw_html_cache[url]
            
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        try:
                            # First try to read with response encoding
                            content = await response.text()
                        except UnicodeDecodeError:
                            try:
                                # Fallback to reading bytes and decoding with error handling
                                raw_bytes = await response.read()
                                # Try common encodings
                                for encoding in ['utf-8', 'iso-8859-1', 'windows-1252']:
                                    try:
                                        content = raw_bytes.decode(encoding)
                                        break
                                    except UnicodeDecodeError:
                                        continue
                                else:
                                    # If all encodings fail, use utf-8 with error replacement
                                    content = raw_bytes.decode('utf-8', errors='replace')
                            except Exception as e:
                                print(f"   ❌ Failed to decode content from {url}: {e}")
                                self.raw_html_cache[url] = None
                                return None
                        
                        # Cache the result
                        self.raw_html_cache[url] = content
                        if self.progress_formatter:
                            self.progress_formatter.log_raw_html_fetch(url, cached=False)
                        return content
                    else:
                        print(f"   ❌ HTTP {response.status} fetching raw HTML: {url}")
                        # Cache failed result as None to avoid retrying
                        self.raw_html_cache[url] = None
                        return None
                        
        except asyncio.TimeoutError:
            print(f"   ⏰ Timeout fetching raw HTML: {url}")
            self.raw_html_cache[url] = None  # Cache failure
            return None
        except Exception as e:
            print(f"   ⚠️ Error fetching raw HTML from {url}: {e}")
            self.raw_html_cache[url] = None  # Cache failure
            return None
    
    def extract_links(self, html: str, base_url: str, allowed_domains: List[str], all_domains: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Extract links from HTML content.
        
        Args:
            html: HTML content to extract links from
            base_url: Base URL for resolving relative links
            allowed_domains: List of allowed domains
            all_domains: List of all domain configurations
            
        Returns:
            Dictionary with 'pages' and 'pdfs' lists
        """
        new_urls = []
        pdf_urls = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # Get all configured domain names for checking
        configured_domains = [d['domain'] for d in all_domains]
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href:
                # Convert relative URLs to absolute
                if not href.startswith(('http://', 'https://', 'mailto:', 'tel:')):
                    href = urljoin(base_url, href)
                
                # Check if URL is valid
                parsed_href = urlparse(href)
                if (parsed_href.scheme in ('http', 'https') and 
                    href not in self.visited_urls):
                    
                    # Skip section URLs (with # fragments) if configured
                    if self.config.get('exclude_section_urls', True) and '#' in href:
                        continue
                    
                    # Check if URL should be excluded
                    if self._is_url_excluded(href, all_domains):
                        continue
                    
                    # Allow URLs from:
                    # 1. Originally allowed domains (explicitly requested)
                    # 2. Configured domains (have settings in YAML)
                    domain_allowed = (parsed_href.netloc in allowed_domains or 
                                    parsed_href.netloc in configured_domains)
                    
                    if domain_allowed:
                        # Only add to visited_urls when we actually crawl it, not when we discover it
                        # This prevents duplicate queueing
                        if href not in self.visited_urls:
                            # Check if this is a PDF URL
                            if href.lower().endswith('.pdf'):
                                pdf_urls.append(href)
                            else:
                                new_urls.append(href)
        
        return {'pages': new_urls, 'pdfs': pdf_urls}
    
    def _is_url_excluded(self, url: str, domains: List[Dict[str, Any]]) -> bool:
        """
        Check if URL should be excluded based on global and domain-specific exclude patterns.
        
        Args:
            url: URL to check
            domains: List of domain configurations
            
        Returns:
            True if URL should be excluded
        """
        # Get global exclude URLs from config
        global_exclude_urls = self.config.get('exclude_urls', [])
        
        # Check global exclude patterns
        for exclude_pattern in global_exclude_urls:
            if self._url_matches_pattern(url, exclude_pattern):
                return True
        
        # Get domain config for this URL
        domain_config = self.get_domain_config(url, domains)
        if domain_config:
            # Check domain-specific exclude URLs
            domain_exclude_urls = domain_config.get('exclude_urls', [])
            for exclude_pattern in domain_exclude_urls:
                if self._url_matches_pattern(url, exclude_pattern):
                    return True
        
        return False
    
    def _url_matches_pattern(self, url: str, pattern: str) -> bool:
        """
        Check if URL matches a pattern (supports wildcards).
        
        Args:
            url: URL to check
            pattern: Pattern to match against
            
        Returns:
            True if URL matches pattern
        """
        import fnmatch
        
        # Exact match
        if url == pattern:
            return True
        
        # Wildcard pattern matching
        if '*' in pattern:
            return fnmatch.fnmatch(url, pattern)
        
        # Substring match (if pattern starts with URL)
        if url.startswith(pattern):
            return True
        
        return False
    
    def _is_problematic_url(self, url: str) -> bool:
        """Check if URL is in the problematic URLs list (causes crashes)."""
        problematic_file = 'problematic_urls.txt'
        if os.path.exists(problematic_file):
            try:
                with open(problematic_file, 'r', encoding='utf-8') as f:
                    problematic_urls = [line.strip() for line in f if line.strip()]
                    return url in problematic_urls
            except Exception as e:
                print(f"Warning: Could not read problematic URLs file: {e}")
        return False
    
    def save_checkpoint(self, visited_urls: set, crawl_queue: deque, semantic_queue: list = None):
        """Save crawling checkpoint to resume later."""
        import json
        
        # Read existing semantic stats from checkpoint if it exists
        semantic_completed = 0
        semantic_pending = 0
        if os.path.exists('crawler_checkpoint.json'):
            try:
                with open('crawler_checkpoint.json', 'r', encoding='utf-8') as f:
                    existing = json.load(f)
                    semantic_completed = existing.get('semantic_completed', 0)
                    semantic_pending = existing.get('semantic_pending', 0)
            except:
                pass
        
        # Load existing checkpoint to preserve all fields
        existing_checkpoint = {}
        try:
            with open('crawler_checkpoint.json', 'r', encoding='utf-8') as f:
                existing_checkpoint = json.load(f)
        except:
            pass
        
        # Update only the crawler-specific fields, preserve semantic worker fields
        checkpoint = existing_checkpoint.copy()
        checkpoint.update({
            'visited_urls': list(visited_urls),
            'crawl_queue': list(crawl_queue),
            'semantic_queue': semantic_queue or [],
            'semantic_completed': semantic_completed,
            'semantic_pending': semantic_pending
        })
        with open('crawler_checkpoint.json', 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, indent=2)
    
    def load_checkpoint(self):
        """Load crawling checkpoint if exists."""
        import json
        import os
        if os.path.exists('crawler_checkpoint.json'):
            try:
                with open('crawler_checkpoint.json', 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                semantic_queue = checkpoint.get('semantic_queue', [])
                return set(checkpoint['visited_urls']), deque(checkpoint['crawl_queue']), semantic_queue
            except Exception as e:
                print(f"⚠️ Could not load checkpoint: {e}")
        return set(), deque(), []
    
    def save_failed_urls(self):
        """Save failed URLs to a text file."""
        if self.failed_urls:
            with open('failed_urls.txt', 'w', encoding='utf-8') as f:
                f.write(f"Failed URLs Report - Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")
                for failed in self.failed_urls:
                    f.write(f"URL: {failed['url']}\n")
                    f.write(f"Attempts: {failed['attempts']}\n")
                    f.write(f"Error: {failed['error']}\n")
                    f.write("-" * 40 + "\n")
            print(f"   📄 Failed URLs saved to failed_urls.txt ({len(self.failed_urls)} URLs)")
    
    async def crawl_all_streaming(self, domains: List[Dict[str, Any]]):
        """
        Crawl all pages from configured domains, yielding results as they're processed.
        
        Args:
            domains: List of domain configurations
            
        Yields:
            Crawl results as they're processed
        """
        allowed_domains = [d['domain'] for d in domains]
        
        # Initialize crawl queue
        self.initialize_crawl(domains)
        
        max_pages = self.config.get('max_pages', 100)
        pages_crawled = 0
        
        # DON'T load checkpoint here - let orchestrator control this
        # The orchestrator will set visited_urls and queue if resuming
        
        # Process queue with streaming
        while self.queue and pages_crawled < max_pages:
            url = self.queue.popleft()
            
            # Skip section URLs (with # fragments) if configured
            if self.config.get('exclude_section_urls', True) and '#' in url:
                continue
            
            # Check if URL should be excluded
            if self._is_url_excluded(url, domains):
                print(f"🚫 Excluding URL: {url}")
                continue
            
            # Check if URL is in problematic URLs list (causes crashes)
            if self._is_problematic_url(url):
                print(f"⚠️ Skipping problematic URL: {url}")
                continue
            
            # Mark URL as visited NOW that we're actually crawling it
            self.visited_urls.add(url)
            
            # Log crawling start through progress formatter
            if self.progress_formatter:
                self.progress_formatter.log_crawling_start(url)
            
            # Get domain config - use specific config if available, otherwise use default
            domain_config = self.get_domain_config(url, domains)
            if not domain_config:
                domain_config = self.get_default_domain_config(url)
                if self.progress_formatter:
                    self.progress_formatter.log_domain_config(domain_config['domain'])
            else:
                if self.progress_formatter:
                    has_js = bool(domain_config.get('js_code'))
                    has_wait = bool(domain_config.get('wait_for'))
                    self.progress_formatter.log_domain_config(domain_config['domain'], has_js, has_wait)
            
            # Crawl the page using two-phase approach (raw HTML for links, Crawl4AI for content)
            result = await self.crawl_page_two_phase(url, domain_config, allowed_domains, domains)
            if result:
                # Add queue size info for orchestrator
                result['queue_size'] = len(self.queue)
                result['pages_crawled'] = pages_crawled
                
                # Only count HTML pages toward max_pages limit (not PDFs)
                is_pdf_redirect = 'pdf_redirect' in result or result.get('crawl4ai_failed', False)
                if not is_pdf_redirect:
                    pages_crawled += 1
                    # Progress logging handled by orchestrator
                else:
                    pass
                
                # Save checkpoint every page
                if pages_crawled > 0:
                    # Get semantic queue data from callback if available
                    semantic_queue = []
                    if self.semantic_queue_callback:
                        try:
                            semantic_queue = self.semantic_queue_callback()
                        except Exception as e:
                            print(f"⚠️ Could not get semantic queue for checkpoint: {e}")
                    self.save_checkpoint(self.visited_urls, self.queue, semantic_queue)
                
                # Get links from the two-phase result (already extracted from raw HTML)
                if pages_crawled < max_pages:
                    links = result.get('links', {'pages': [], 'pdfs': []})
                    new_urls = links['pages']
                    pdf_urls = links['pdfs']
                    
                    if new_urls and self.progress_formatter:
                        self.progress_formatter.log_new_urls_discovered(new_urls)
                    
                    if pdf_urls:
                        # PDF URLs will be handled by orchestrator, no need to print here
                        pass
                        if len(pdf_urls) > 3:
                            print(f"      ... and {len(pdf_urls) - 3} more")
                    
                    # Only add URLs that haven't been visited and aren't already in queue
                    for url in new_urls:
                        if url not in self.visited_urls and url not in self.queue:
                            self.queue.append(url)
                    # Add PDF URLs to result for processing
                    result['pdf_urls'] = pdf_urls
                
                # Ensure pdf_urls is in result even if no new URLs found
                if 'pdf_urls' not in result:
                    result['pdf_urls'] = []
                
                # Yield result immediately for processing
                yield result
            else:
                print(f"   ❌ Failed to crawl: {url}")
        
        if pages_crawled >= max_pages:
            print(f"⚠️  Reached maximum HTML page limit ({max_pages})")

    async def crawl_all(self, domains: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Crawl all pages from configured domains.
        
        Args:
            domains: List of domain configurations
            
        Returns:
            List of crawl results
        """
        results = []
        allowed_domains = [d['domain'] for d in domains]
        
        # Initialize crawl queue
        self.initialize_crawl(domains)
        
        max_pages = self.config.get('max_pages', 100)  # Default to 100 if not specified
        pages_crawled = 0
        
        while self.queue and pages_crawled < max_pages:
            url = self.queue.popleft()
            
            # Skip section URLs (with # fragments) if configured
            if self.config.get('exclude_section_urls', True) and '#' in url:
                continue
            
            # Check if URL should be excluded
            if self._is_url_excluded(url, domains):
                print(f"🚫 Excluding URL: {url}")
                continue
            
            # Log crawling start through progress formatter
            if self.progress_formatter:
                self.progress_formatter.log_crawling_start(url)
            
            # Get domain config - use specific config if available, otherwise use default
            domain_config = self.get_domain_config(url, domains)
            if not domain_config:
                domain_config = self.get_default_domain_config(url)
                if self.progress_formatter:
                    self.progress_formatter.log_domain_config(domain_config['domain'])
            else:
                if self.progress_formatter:
                    has_js = bool(domain_config.get('js_code'))
                    has_wait = bool(domain_config.get('wait_for'))
                    self.progress_formatter.log_domain_config(domain_config['domain'], has_js, has_wait)
            
            # Use two-phase crawling
            result = await self.crawl_page_two_phase(url, domain_config, allowed_domains, domains)
            if result:
                results.append(result)
                # Only count HTML pages toward max_pages limit (not PDFs)
                is_pdf_redirect = 'pdf_redirect' in result or result.get('crawl4ai_failed', False)
                if not is_pdf_redirect:
                    pages_crawled += 1
                    # Progress logging handled by orchestrator
                else:
                    print(f"   📄 Successfully processed PDF redirect: {url}")
                    sys.stdout.flush()
                
                # Get links from the two-phase result (already extracted)
                if pages_crawled < max_pages:
                    links = result.get('links', {'pages': [], 'pdfs': []})
                    new_urls = links['pages']
                    pdf_urls = links['pdfs']
                    
                    if new_urls and self.progress_formatter:
                        self.progress_formatter.log_new_urls_discovered(new_urls)
                    
                    if pdf_urls:
                        # PDF URLs will be handled by orchestrator, no need to print here
                        pass
                        if len(pdf_urls) > 3:
                            print(f"      ... and {len(pdf_urls) - 3} more")
                    
                    # Only add URLs that haven't been visited and aren't already in queue
                    for url in new_urls:
                        if url not in self.visited_urls and url not in self.queue:
                            self.queue.append(url)
                    # Add PDF URLs to result for processing
                    result['pdf_urls'] = pdf_urls
            else:
                print(f"   ❌ Failed to crawl: {url}")
        
        if pages_crawled >= max_pages:
            print(f"⚠️  Reached maximum HTML page limit ({max_pages})")
        
        return results