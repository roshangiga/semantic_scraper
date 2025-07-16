"""
Web crawler module for handling Crawl4AI operations.
"""

import asyncio
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, urljoin
from collections import deque
from crawl4ai import AsyncWebCrawler
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
        Crawl a single page.
        
        Args:
            url: URL to crawl
            domain_config: Domain-specific configuration
            
        Returns:
            Dictionary containing crawl results or None if failed
        """
        if not self.crawler:
            raise RuntimeError("Crawler not initialized. Use async context manager.")
        
        try:
            # Get crawl4ai settings
            crawl_settings = self.config.copy()
            crawl_settings.update({
                'url': url,
                'js_code': domain_config.get('js_code', ''),
                'bypass_cache': crawl_settings.get('bypass_cache', True)
            })
            
            # Add domain-specific wait_for if specified
            if domain_config.get('wait_for'):
                crawl_settings['wait_for'] = domain_config.get('wait_for')
            
            # Optional parameters
            if crawl_settings.get('delay_before_return_html'):
                crawl_settings['delay_before_return_html'] = float(crawl_settings['delay_before_return_html'])
            if crawl_settings.get('js_only'):
                crawl_settings['js_only'] = bool(crawl_settings['js_only'])
            if crawl_settings.get('wait_for'):
                crawl_settings['wait_for'] = crawl_settings['wait_for']
            
            # Remove config keys that aren't crawl4ai parameters
            crawl_params = {k: v for k, v in crawl_settings.items() 
                          if k in ['url', 'js_code', 'bypass_cache', 'delay_before_return_html', 
                                  'js_only', 'wait_for']}
            
            result = await self.crawler.arun(**crawl_params)
            
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