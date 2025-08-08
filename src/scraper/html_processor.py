"""
HTML processing module for cleaning and processing HTML content.
"""

import re
import tempfile
from typing import Dict, List, Any
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup


class HTMLProcessor:
    """Handles HTML cleaning and link processing operations."""
    
    def __init__(self, config: Dict[str, Any], global_html_cleaning: Dict[str, Any] = None):
        """
        Initialize the HTMLProcessor.
        
        Args:
            config: Configuration dictionary containing link processing settings
            global_html_cleaning: Global HTML cleaning settings
        """
        self.config = config
        self.global_html_cleaning = global_html_cleaning or {}
        self.image_extensions = tuple(config.get('exclude_image_extensions', []))
        self.exclude_section_urls = config.get('exclude_section_urls', True)
        self.convert_relative_to_absolute = config.get('convert_relative_to_absolute', True)
    
    def clean_html_content(self, html_content: str, domain_config: Dict[str, Any]) -> str:
        """
        Clean HTML content by removing unwanted elements and comment blocks.
        
        Args:
            html_content: HTML content to clean
            domain_config: Domain-specific configuration
            
        Returns:
            Cleaned HTML content
        """
        # Combine global and domain-specific comment blocks
        global_comment_blocks = self.global_html_cleaning.get('comment_blocks_to_remove', [])
        domain_comment_blocks = domain_config.get('comment_blocks_to_remove', []) or []
        all_comment_blocks = global_comment_blocks + domain_comment_blocks
        
        # Remove comment blocks first
        for start_comment, end_comment in all_comment_blocks:
            start_escaped = re.escape(start_comment)
            end_escaped = re.escape(end_comment)
            pattern = f'{start_escaped}.*?{end_escaped}'
            html_content = re.sub(pattern, '', html_content, flags=re.DOTALL)
        
        # Parse with BeautifulSoup for element removal
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove elements hidden by CSS styles (non-inline) if enabled
        # THIS MUST RUN FIRST before removing <style> tags
        if self.global_html_cleaning.get('remove_css_hidden_elements', True):
            self._remove_css_hidden_elements(soup)
        
        # Combine global and domain-specific elements to remove
        global_elements = self.global_html_cleaning.get('html_elements_to_remove', [])
        domain_elements = domain_config.get('html_elements_to_remove', []) or []
        all_elements_to_remove = list(set(global_elements + domain_elements))
        
        # Remove unwanted HTML elements
        for element_tag in all_elements_to_remove:
            for element in soup.find_all(element_tag):
                element.decompose()
        
        # Combine global and domain-specific classes to remove
        global_classes = self.global_html_cleaning.get('html_classes_to_remove', [])
        domain_classes = domain_config.get('html_classes_to_remove', []) or []
        all_classes_to_remove = list(set(global_classes + domain_classes))
        
        # Remove elements by CSS class selectors
        for class_selector in all_classes_to_remove:
            for element in soup.select(class_selector):
                element.decompose()
        
        # Apply domain-specific "only include" filter if specified
        classes_to_only_include = domain_config.get('html_classes_to_only_include', [])
        if classes_to_only_include:
            self._apply_only_include_filter(soup, classes_to_only_include)
        
        # Remove empty elements by default (multiple passes to handle nested empty elements)
        default_empty_elements = [
            "div", "p", "span", "section", "article", "aside", "main",
            "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol", "li",
            "nav", "header", "footer", "figure", "figcaption", "blockquote",
            "pre", "code", "em", "strong", "small", "mark", "del", "ins",
            "sub", "sup", "i", "b", "u", "s", "q", "cite", "abbr", "dfn",
            "time", "var", "samp", "kbd", "address", "dt", "dd", "dl"
        ]
        
        changed = True
        while changed:
            changed = False
            for element_tag in default_empty_elements:
                for element in soup.find_all(element_tag):
                    # Check if element is empty
                    if not element.get_text(strip=True) and not element.find_all(['img', 'input', 'br', 'hr']):
                        element.decompose()
                        changed = True
        
        return str(soup)
    
    def _remove_css_hidden_elements(self, soup: BeautifulSoup) -> None:
        """
        Remove elements that are hidden by CSS styles in <style> tags.
        
        Args:
            soup: BeautifulSoup object to process
        """
        # Extract CSS rules from <style> tags
        css_rules = []
        for style_tag in soup.find_all('style'):
            if style_tag.string:
                css_rules.append(style_tag.string)
        
        # Join all CSS content
        css_content = ' '.join(css_rules)
        
        # Simple regex patterns to find hidden selectors
        # This is a basic implementation - CSS parsing can be complex
        hidden_patterns = [
            r'([^{]+)\s*\{\s*[^}]*display\s*:\s*none[^}]*\}',
            r'([^{]+)\s*\{\s*[^}]*visibility\s*:\s*hidden[^}]*\}',
        ]
        
        hidden_selectors = []
        for pattern in hidden_patterns:
            matches = re.findall(pattern, css_content, re.IGNORECASE)
            for match in matches:
                # Clean up the selector
                selector = match.strip().replace('\n', ' ')
                # Split by comma for multiple selectors
                selectors = [s.strip() for s in selector.split(',')]
                hidden_selectors.extend(selectors)
        
        # Remove elements matching hidden selectors
        for selector in hidden_selectors:
            try:
                # Basic selector cleanup - remove pseudo-classes and pseudo-elements
                clean_selector = re.sub(r':(hover|focus|active|visited|before|after|first-child|last-child|nth-child\([^)]+\))', '', selector)
                clean_selector = clean_selector.strip()
                
                if clean_selector:
                    elements = soup.select(clean_selector)
                    for element in elements:
                        element.decompose()
            except Exception:
                # If selector is invalid, skip it
                continue
    
    def _apply_only_include_filter(self, soup: BeautifulSoup, classes_to_only_include: List[str]) -> None:
        """
        Keep only content from specified classes, remove everything else.
        If none of the specified classes exist, ignore this rule.
        
        Args:
            soup: BeautifulSoup object to process
            classes_to_only_include: List of class selectors to keep exclusively
        """
        # Check if any of the specified classes exist
        elements_found = []
        for class_selector in classes_to_only_include:
            try:
                elements = soup.select(class_selector)
                elements_found.extend(elements)
            except Exception:
                # If selector is invalid, skip it
                continue
        
        # If no elements found with specified classes, ignore this rule
        if not elements_found:
            return
        
        # Get the body element or create one if it doesn't exist
        body = soup.find('body')
        if not body:
            body = soup
        
        # Collect all elements to keep (the specified classes and their descendants)
        elements_to_keep = set()
        for element in elements_found:
            # Add the element itself
            elements_to_keep.add(element)
            # Add all its descendants
            for descendant in element.find_all():
                elements_to_keep.add(descendant)
            # Add all its parents up to body
            parent = element.parent
            while parent and parent != body:
                elements_to_keep.add(parent)
                parent = parent.parent
        
        # Remove all elements not in the keep set
        elements_to_remove = []
        for element in body.find_all():
            if element not in elements_to_keep:
                elements_to_remove.append(element)
        
        # Remove elements that are not in the keep list
        for element in elements_to_remove:
            element.decompose()
    
    def process_links_in_html(self, html_content: str, base_url: str) -> str:
        """
        Process links in HTML content, converting them to markdown format.
        
        Args:
            html_content: HTML content to process
            base_url: Base URL for resolving relative links
            
        Returns:
            HTML content with processed links
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Replace anchor tags with markdown link format
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            text = a_tag.get_text(strip=True)
            
            if href and text:
                # Check if the text already contains markdown link format
                if self._is_markdown_link(text):
                    # Preserve existing markdown links
                    a_tag.replace_with(text)
                    continue
                
                # Convert relative URLs to absolute
                if (self.convert_relative_to_absolute and 
                    not href.startswith(('http://', 'https://', 'mailto:', 'tel:'))):
                    href = urljoin(base_url, href)
                
                # Skip URLs with section fragments if configured
                if self.exclude_section_urls and '#' in href:
                    a_tag.replace_with(text)
                    continue
                
                # Skip image URLs
                if href.lower().endswith(self.image_extensions):
                    a_tag.replace_with(text)
                    continue
                
                # Replace the anchor tag with markdown link format
                markdown_link = f"[{text}]({href})"
                a_tag.replace_with(markdown_link)
            else:
                # If no href or text, just replace with text
                a_tag.replace_with(text if text else '')
        
        return str(soup)
    
    def _is_markdown_link(self, text: str) -> bool:
        """
        Check if text already contains markdown link format.
        
        Args:
            text: Text to check
            
        Returns:
            True if text contains markdown links
        """
        import re
        
        # Pattern to match markdown links: [text](url)
        markdown_link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
        
        return bool(re.search(markdown_link_pattern, text))
    
    def create_temp_html_file(self, html_content: str) -> str:
        """
        Create a temporary HTML file for processing.
        
        Args:
            html_content: HTML content to write to file
            
        Returns:
            Path to the temporary file
        """
        with tempfile.NamedTemporaryFile(delete=False, suffix='.html') as temp_file:
            temp_file.write(html_content.encode('utf-8'))
            return temp_file.name
    
    def process_html(self, html_content: str, base_url: str, domain_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process HTML content through the full pipeline.
        
        Args:
            html_content: Raw HTML content
            base_url: Base URL for link resolution
            domain_config: Domain-specific configuration
            
        Returns:
            Dictionary containing processed HTML and temp file path
        """
        # Step 1: Clean HTML content
        cleaned_html = self.clean_html_content(html_content, domain_config)
        
        # Step 2: Process links
        processed_html = self.process_links_in_html(cleaned_html, base_url)
        
        # Step 3: Create temporary file for conversion
        temp_file_path = self.create_temp_html_file(processed_html)
        
        return {
            'cleaned_html': cleaned_html,
            'processed_html': processed_html,
            'temp_file_path': temp_file_path
        }
    
    def sanitize_filename(self, url: str) -> str:
        """
        Convert URL to a safe filename.
        
        Args:
            url: URL to convert
            
        Returns:
            Safe filename
        """
        # Remove protocol
        filename = url.replace('https://', '').replace('http://', '')
        
        # Replace invalid characters for Windows filenames
        invalid_chars = r'[<>:"|?*\\/]'
        filename = re.sub(invalid_chars, '_', filename)
        
        # Replace multiple underscores with single underscore
        filename = re.sub(r'_+', '_', filename)
        
        # Remove leading/trailing underscores
        filename = filename.strip('_')
        
        # Ensure filename isn't too long (Windows has 260 char limit for full path)
        if len(filename) > 200:
            filename = filename[:200]
        
        return filename