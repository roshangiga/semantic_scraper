"""
Web crawler module for handling Crawl4AI operations.
"""

import asyncio
import os
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
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the WebCrawler.
        
        Args:
            config: Configuration dictionary containing crawl4ai settings
        """
        self.config = config
        self.crawler = None
        self.visited_urls = set()
        self.queue = deque()
        self.failed_urls = []  # Track failed URLs with reasons
        
    async def __aenter__(self):
        """Async context manager entry."""
        self.crawler = AsyncWebCrawler(
            verbose=self.config.get('verbose', True)
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
                }
                # Cache mode mapping (default to BYPASS when bypass_cache True)
                if CacheMode is not None:
                    bypass = bool(crawl_params.get('bypass_cache', True))
                    run_config_kwargs['cache_mode'] = CacheMode.BYPASS if bypass else CacheMode.DEFAULT
                run_config = CrawlerRunConfig(**{k: v for k, v in run_config_kwargs.items() if v is not None})

                # js_only remains a top-level flag in some versions; pass through when set
                js_only = bool(crawl_params.get('js_only')) if 'js_only' in crawl_params else None
                if crawl_params.get('delay_before_return_html') is not None:
                    try:
                        dly = float(crawl_params['delay_before_return_html'])
                        print(f"   ⏳ Applying delay_before_return_html={dly:.2f}s (run_config)")
                    except Exception:
                        pass
                if crawl_params.get('wait_for'):
                    wf = str(crawl_params['wait_for'])
                    print(f"   ⏳ Applying wait_for='{wf[:60]}' (run_config)")
                if crawl_params.get('page_timeout') is not None:
                    try:
                        pt = int(crawl_params['page_timeout'])
                        print(f"   ⏱️  Applying page_timeout={pt}ms (run_config)")
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
                        print(f"   ⏳ Applying delay_before_return_html={dly:.2f}s (legacy)")
                    except Exception:
                        pass
                if crawl_params.get('wait_for'):
                    wf = str(crawl_params['wait_for'])
                    print(f"   ⏳ Applying wait_for='{wf[:60]}' (legacy)")
                if crawl_params.get('page_timeout') is not None:
                    try:
                        pt = int(crawl_params['page_timeout'])
                        print(f"   ⏱️  Applying page_timeout={pt}ms (legacy)")
                    except Exception:
                        pass
                result = await self.crawler.arun(**crawl_params)
            
            # Check if we were redirected to a PDF by looking for our marker in the HTML
            if result.cleaned_html and 'PDF_REDIRECT:' in result.cleaned_html:
                # Extract the PDF URL from the marker
                import re
                pdf_match = re.search(r'PDF_REDIRECT:([^\s<]+)', result.cleaned_html)
                if pdf_match:
                    final_url = pdf_match.group(1)
                    print(f"   📄 Page redirected to PDF: {final_url}")
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
                print(f"   📄 Page redirected to PDF: {final_url}")
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
                        self.visited_urls.add(href)
                        
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
    
    def save_checkpoint(self, visited_urls: set, crawl_queue: deque):
        """Save crawling checkpoint to resume later."""
        import json
        checkpoint = {
            'visited_urls': list(visited_urls),
            'crawl_queue': list(crawl_queue)
        }
        with open('crawler_checkpoint.json', 'w') as f:
            json.dump(checkpoint, f)
    
    def load_checkpoint(self):
        """Load crawling checkpoint if exists."""
        import json
        import os
        if os.path.exists('crawler_checkpoint.json'):
            try:
                with open('crawler_checkpoint.json', 'r') as f:
                    checkpoint = json.load(f)
                print(f"📥 Loaded checkpoint with {len(checkpoint['visited_urls'])} visited URLs")
                return set(checkpoint['visited_urls']), deque(checkpoint['crawl_queue'])
            except Exception as e:
                print(f"⚠️ Could not load checkpoint: {e}")
        return set(), deque()
    
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
        
        # Load checkpoint if exists
        if hasattr(self, 'visited_urls'):
            self.visited_urls, queue_from_checkpoint = self.load_checkpoint()
            if queue_from_checkpoint:
                self.queue = queue_from_checkpoint
        
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
            
            print(f"🌐 Crawling: {url} ({pages_crawled + 1}/{max_pages})")
            
            # Get domain config - use specific config if available, otherwise use default
            domain_config = self.get_domain_config(url, domains)
            if not domain_config:
                domain_config = self.get_default_domain_config(url)
                print(f"   📋 Using default configuration for domain: {domain_config['domain']}")
            else:
                print(f"   ⚙️  Using configured settings for domain: {domain_config['domain']}")
                if domain_config.get('js_code'):
                    print(f"   ⚡ JavaScript code will be executed")
                if domain_config.get('wait_for'):
                    print(f"   ⏳ Waiting for condition: {domain_config['wait_for'][:30]}...")
            
            # Crawl the page
            result = await self.crawl_page(url, domain_config)
            if result:
                pages_crawled += 1
                print(f"   ✅ Successfully crawled: {url}")
                
                # Save checkpoint every N pages
                checkpoint_interval = self.config.get('save_checkpoint_every', 10)
                if pages_crawled % checkpoint_interval == 0:
                    self.save_checkpoint(self.visited_urls, self.queue)
                    print(f"   💾 Checkpoint saved after {pages_crawled} pages")
                
                # Extract and queue new URLs (only if we haven't reached the limit)
                if pages_crawled < max_pages:
                    links = self.extract_links(
                        result['html'], 
                        url, 
                        allowed_domains,
                        domains
                    )
                    new_urls = links['pages']
                    pdf_urls = links['pdfs']
                    
                    if new_urls:
                        print(f"   🔗 Found {len(new_urls)} new URLs to crawl")
                        for new_url in new_urls[:3]:  # Show first 3 URLs
                            print(f"      - {new_url}")
                        if len(new_urls) > 3:
                            print(f"      ... and {len(new_urls) - 3} more")
                    
                    if pdf_urls:
                        print(f"   📄 Found {len(pdf_urls)} PDF URLs")
                        for pdf_url in pdf_urls[:3]:  # Show first 3 PDF URLs
                            print(f"      - {pdf_url}")
                        if len(pdf_urls) > 3:
                            print(f"      ... and {len(pdf_urls) - 3} more")
                    
                    self.queue.extend(new_urls)
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
            print(f"⚠️  Reached maximum page limit ({max_pages})")

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
            
            print(f"🌐 Crawling: {url} ({pages_crawled + 1}/{max_pages})")
            
            # Get domain config - use specific config if available, otherwise use default
            domain_config = self.get_domain_config(url, domains)
            if not domain_config:
                domain_config = self.get_default_domain_config(url)
                print(f"   📋 Using default configuration for domain: {domain_config['domain']}")
            else:
                print(f"   ⚙️  Using configured settings for domain: {domain_config['domain']}")
                if domain_config.get('js_code'):
                    print(f"   ⚡ JavaScript code will be executed")
                if domain_config.get('wait_for'):
                    print(f"   ⏳ Waiting for condition: {domain_config['wait_for'][:30]}...")
            
            result = await self.crawl_page(url, domain_config)
            if result:
                results.append(result)
                pages_crawled += 1
                print(f"   ✅ Successfully crawled: {url}")
                
                # Extract and queue new URLs (only if we haven't reached the limit)
                if pages_crawled < max_pages:
                    links = self.extract_links(
                        result['html'], 
                        url, 
                        allowed_domains,
                        domains
                    )
                    new_urls = links['pages']
                    pdf_urls = links['pdfs']
                    
                    if new_urls:
                        print(f"   🔗 Found {len(new_urls)} new URLs to crawl")
                        for new_url in new_urls[:3]:  # Show first 3 URLs
                            print(f"      - {new_url}")
                        if len(new_urls) > 3:
                            print(f"      ... and {len(new_urls) - 3} more")
                    
                    if pdf_urls:
                        print(f"   📄 Found {len(pdf_urls)} PDF URLs")
                        for pdf_url in pdf_urls[:3]:  # Show first 3 PDF URLs
                            print(f"      - {pdf_url}")
                        if len(pdf_urls) > 3:
                            print(f"      ... and {len(pdf_urls) - 3} more")
                    
                    self.queue.extend(new_urls)
                    # Add PDF URLs to result for processing
                    result['pdf_urls'] = pdf_urls
            else:
                print(f"   ❌ Failed to crawl: {url}")
        
        if pages_crawled >= max_pages:
            print(f"Reached maximum page limit ({max_pages})")
        
        return results