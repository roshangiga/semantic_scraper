import asyncio
import os
import re
from collections import deque
from urllib.parse import urlparse, urljoin
import tempfile

from crawl4ai import AsyncWebCrawler
from docling.document_converter import DocumentConverter
from bs4 import BeautifulSoup  # For extracting links from HTML

# Installation:
# pip install crawl4ai docling beautifulsoup4
# Then run: crawl4ai-setup  # To setup the browser for Crawl4AI

# Domains to crawl
DOMAINS = {
    # 'myt.mu',
    # 'devices.myt.mu',
    'esimtravel.myt.mu',
}

# Starting URLs
START_URLS = [f'https://{domain}/' for domain in DOMAINS]

# JS code to remove unwanted elements
JS_REMOVE_CODE = """
      // Click "View More" buttons first, then expand Vue.js panels
      setTimeout(() => {
        // Click "View More" buttons first
        document.querySelectorAll('p.text-font-text.text-esim_blue_light').forEach(viewMore => {
          if (viewMore.textContent.includes('View More') && viewMore.click) {
            viewMore.click();
          }
        });
      
        // Click panel headers to expand
        setTimeout(() => {document.querySelectorAll('.p-panel-header').forEach(header => {
          if (header.click) header.click();
        })}, 1500);

      }, 2000);
"""

# HTML Cleaning Configuration - Easy to manage exclusion lists
HTML_ELEMENTS_TO_REMOVE = [
    'header', 'footer', 'nav', 'aside', 'style'
]

HTML_CLASSES_TO_REMOVE = [
    '.sidebar', '.navbar', '.header', '.footer'
]

# Comment blocks to remove (from start comment to end comment)
COMMENT_BLOCKS_TO_REMOVE = [
    ('<!-- Cookie -->', '<!-- Cookie -->'),
    ('<!-- Footer Content -->', '<!-- End Footer Content -->'),
    ('<!-- Copyright Footer -->', '<!-- End Copyright Footer -->'),
    ('<!-- start banner -->', '<!-- end banner -->'),
    ('<!-- start scroll progress -->', '<!-- end scroll progress -->'),
    ('<!-- start header -->', '<!-- start section -->'),
]

# Empty elements to remove (elements with no content or only whitespace)
EMPTY_ELEMENTS_TO_REMOVE = [
    'div', 'p', 'span', 'section', 'article', 'aside', 'main', 
    'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li'
]

def sanitize_filename(url):
    """Convert URL to a safe filename by removing/replacing invalid characters."""
    # Remove protocol
    filename = url.replace('https://', '').replace('http://', '')
    
    # Replace invalid characters for Windows filenames
    # Invalid chars: < > : " | ? * \ /
    invalid_chars = r'[<>:"|?*\\/]'
    filename = re.sub(invalid_chars, '_', filename)
    
    # Replace multiple underscores with single underscore
    filename = re.sub(r'_+', '_', filename)
    
    # Remove leading/trailing underscores and add .md extension
    filename = filename.strip('_') + '.md'
    
    # Ensure filename isn't too long (Windows has 260 char limit for full path)
    if len(filename) > 200:
        filename = filename[:200] + '.md'
    
    return filename

def clean_html_content(html_content):
    """Clean HTML content by removing unwanted elements and comment blocks."""
    import re
    
    # Remove comment blocks first
    for start_comment, end_comment in COMMENT_BLOCKS_TO_REMOVE:
        # Escape special regex characters in comments
        start_escaped = re.escape(start_comment)
        end_escaped = re.escape(end_comment)
        # Remove everything between start and end comments (including the comments)
        pattern = f'{start_escaped}.*?{end_escaped}'
        html_content = re.sub(pattern, '', html_content, flags=re.DOTALL)
    
    # Parse with BeautifulSoup for element removal
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove unwanted HTML elements
    for element_tag in HTML_ELEMENTS_TO_REMOVE:
        for element in soup.find_all(element_tag):
            element.decompose()
    
    # Remove elements by CSS class selectors
    for class_selector in HTML_CLASSES_TO_REMOVE:
        for element in soup.select(class_selector):
            element.decompose()
    
    # Remove empty elements (multiple passes to handle nested empty elements)
    changed = True
    while changed:
        changed = False
        for element_tag in EMPTY_ELEMENTS_TO_REMOVE:
            for element in soup.find_all(element_tag):
                # Check if element is empty (no text content and no child elements with content)
                if not element.get_text(strip=True) and not element.find_all(['img', 'input', 'br', 'hr']):
                    element.decompose()
                    changed = True
    
    return str(soup)

async def crawl_and_save():
    crawler = AsyncWebCrawler(verbose=True)
    doc_converter = DocumentConverter()

    queue = deque(START_URLS)
    visited = set(START_URLS)

    # Create output directories
    os.makedirs('crawled_pages', exist_ok=True)
    os.makedirs('crawled_html', exist_ok=True)

    while queue:
        url = queue.popleft()
        print(f"Crawling: {url}")

        try:
            # Crawl the page with Crawl4AI to get cleaned HTML, ignoring unwanted parts via JS
            result = await crawler.arun(
                url=url,
                js_code=JS_REMOVE_CODE,  # Remove headers, footers, etc.
                bypass_cache=True,
            )

            cleaned_html = result.cleaned_html
            
            # Check if we got valid HTML content
            if not cleaned_html or cleaned_html.strip() == '':
                print(f"Warning: No content received from {url}, skipping...")
                continue

            # Clean HTML content using organized exclusion lists
            cleaned_html = clean_html_content(cleaned_html)

            # Extract links using BeautifulSoup to discover more pages
            soup = BeautifulSoup(cleaned_html, 'html.parser')
            for a in soup.find_all('a', href=True):
                link = urljoin(url, a['href'])
                parsed_link = urlparse(link)
                if parsed_link.scheme in ('http', 'https') and parsed_link.netloc in DOMAINS and link not in visited:
                    visited.add(link)
                    queue.append(link)

            # Save cleaned HTML to temp file for Docling
            with tempfile.NamedTemporaryFile(delete=False, suffix='.html') as temp_file:
                temp_file.write(cleaned_html.encode('utf-8'))
                temp_path = temp_file.name

            # Create a copy of HTML with links converted to markdown format
            soup = BeautifulSoup(cleaned_html, 'html.parser')
            # Image extensions to exclude
            image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp', '.ico')
            
            # Replace anchor tags with markdown link format
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                text = a_tag.get_text(strip=True)
                
                if href and text:
                    # Convert relative URLs to absolute
                    if not href.startswith(('http://', 'https://', 'mailto:', 'tel:')):
                        href = urljoin(url, href)
                    
                    # Skip URLs with section fragments (containing #)
                    if '#' in href:
                        a_tag.replace_with(text)
                        continue
                    
                    # Skip image URLs
                    if href.lower().endswith(image_extensions):
                        # Replace with just the text for images
                        a_tag.replace_with(text)
                        continue
                    
                    # Replace the anchor tag with markdown link format
                    markdown_link = f"[{text}]({href})"
                    a_tag.replace_with(markdown_link)
            
            # Save the modified HTML to temp file for Docling
            modified_html = str(soup)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.html') as temp_file_modified:
                temp_file_modified.write(modified_html.encode('utf-8'))
                temp_path_modified = temp_file_modified.name
            
            # Use Docling to convert the modified HTML to Markdown
            conv_result = doc_converter.convert(temp_path_modified)
            markdown = conv_result.document.export_to_markdown()
            
            # Cleanup modified temp file
            os.unlink(temp_path_modified)

            # Cleanup temp file
            os.unlink(temp_path)

            # Save both HTML and Markdown files
            filename = sanitize_filename(url)
            
            # Save cleaned HTML for inspection
            html_filename = filename.replace('.md', '.html')
            html_save_path = os.path.join('crawled_html', html_filename)
            with open(html_save_path, 'w', encoding='utf-8') as f:
                f.write(cleaned_html)
            print(f"Saved HTML: {html_save_path}")
            
            # Save Markdown with links
            save_path = os.path.join('crawled_pages', filename)
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(markdown)
            print(f"Saved Markdown: {save_path}")

        except Exception as e:
            error_msg = str(e)
            if "net::ERR_NAME_NOT_RESOLVED" in error_msg:
                print(f"Domain not accessible: {url} - skipping...")
            elif "Incoming markup is of an invalid type: None" in error_msg:
                print(f"No valid content received from {url} - skipping...")
            else:
                print(f"Error crawling {url}: {e}")
            continue

if __name__ == '__main__':
    asyncio.run(crawl_and_save())