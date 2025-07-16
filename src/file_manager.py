"""
File management module for handling file operations.
"""

import os
import shutil
from pathlib import Path
from typing import Dict, Any, List


class FileManager:
    """Handles file operations and directory management."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the FileManager.
        
        Args:
            config: Configuration dictionary containing file management settings
        """
        self.config = config
        self.html_output_dir = config.get('html_output_dir', 'crawled_html')
        self.pages_output_dir = config.get('pages_output_dir', 'crawled_pages')
        self.pdf_output_dir = config.get('pdf_output_dir', 'crawled_pdf')
        self.filename_template = config.get('filename_template', '{sanitized_url}')
        self.delete_existing_folders = config.get('delete_existing_folders', False)
        self.use_domain_subfolders = config.get('use_domain_subfolders', True)
    
    def setup_directories(self) -> None:
        """Set up output directories."""
        directories = [self.html_output_dir, self.pages_output_dir, self.pdf_output_dir]
        
        for directory in directories:
            if self.delete_existing_folders and os.path.exists(directory):
                print(f"Deleting existing directory: {directory}")
                self._delete_directory_with_retry(directory)
            
            os.makedirs(directory, exist_ok=True)
            print(f"Created directory: {directory}")
    
    def _delete_directory_with_retry(self, directory: str, max_retries: int = 3) -> None:
        """
        Delete directory with retry logic for Windows file locking issues.
        
        Args:
            directory: Directory path to delete
            max_retries: Maximum number of retry attempts
        """
        import time
        
        for attempt in range(max_retries):
            try:
                # Try to make all files writable first
                for root, dirs, files in os.walk(directory):
                    for file in files:
                        file_path = os.path.join(root, file)
                        try:
                            os.chmod(file_path, 0o777)
                        except (OSError, PermissionError):
                            pass
                
                # Now try to delete the directory
                shutil.rmtree(directory)
                return
                
            except PermissionError as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed, retrying in 1 second...")
                    time.sleep(1)
                else:
                    print(f"Warning: Could not delete {directory} after {max_retries} attempts.")
                    print(f"Error: {e}")
                    print("Please close any programs using files in this directory and try again.")
                    # Don't raise the error, just continue
                    return
            except Exception as e:
                print(f"Unexpected error deleting {directory}: {e}")
                return
    
    def generate_filename(self, url: str, file_extension: str) -> str:
        """
        Generate filename for a given URL.
        
        Args:
            url: URL to generate filename for
            file_extension: File extension (e.g., '.html', '.md', '.docx')
            
        Returns:
            Generated filename
        """
        # For now, we'll use a simple sanitization
        # This can be extended to use the template system
        sanitized_url = self._sanitize_url_for_filename(url)
        
        # Apply template (simple implementation)
        filename = self.filename_template.format(sanitized_url=sanitized_url)
        
        # Add extension if not present
        if not filename.endswith(file_extension):
            filename += file_extension
        
        return filename
    
    def _sanitize_url_for_filename(self, url: str) -> str:
        """
        Sanitize URL for use as filename.
        
        Args:
            url: URL to sanitize
            
        Returns:
            Sanitized filename
        """
        # Remove protocol
        filename = url.replace('https://', '').replace('http://', '')
        
        # Replace invalid characters for Windows filenames
        invalid_chars = r'[<>:"|?*\\/]'
        import re
        filename = re.sub(invalid_chars, '_', filename)
        
        # Replace multiple underscores with single underscore
        filename = re.sub(r'_+', '_', filename)
        
        # Remove leading/trailing underscores
        filename = filename.strip('_')
        
        # Ensure filename isn't too long
        if len(filename) > 200:
            filename = filename[:200]
        
        return filename
    
    def _get_domain_from_url(self, url: str) -> str:
        """
        Extract domain from URL.
        
        Args:
            url: URL to extract domain from
            
        Returns:
            Domain name
        """
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc
    
    def _get_output_path(self, base_dir: str, url: str, filename: str) -> str:
        """
        Get the output path including domain subdirectory if enabled.
        
        Args:
            base_dir: Base output directory
            url: URL to extract domain from
            filename: Filename to save
            
        Returns:
            Full output path
        """
        if self.use_domain_subfolders:
            domain = self._get_domain_from_url(url)
            output_dir = os.path.join(base_dir, domain)
            os.makedirs(output_dir, exist_ok=True)
            return os.path.join(output_dir, filename)
        else:
            return os.path.join(base_dir, filename)
    
    def save_html(self, url: str, content: str) -> str:
        """
        Save HTML content to file.
        
        Args:
            url: URL the content came from
            content: HTML content to save
            
        Returns:
            Path to saved file
        """
        filename = self.generate_filename(url, '.html')
        file_path = self._get_output_path(self.html_output_dir, url, filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Saved HTML: {file_path}")
        return file_path
    
    def save_markdown(self, url: str, content: str) -> str:
        """
        Save Markdown content to file.
        
        Args:
            url: URL the content came from
            content: Markdown content to save
            
        Returns:
            Path to saved file
        """
        filename = self.generate_filename(url, '.md')
        file_path = self._get_output_path(self.pages_output_dir, url, filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Saved Markdown: {file_path}")
        return file_path
    
    def save_docx(self, url: str, content: bytes) -> str:
        """
        Save DOCX content to file.
        
        Args:
            url: URL the content came from
            content: DOCX content to save (as bytes)
            
        Returns:
            Path to saved file
        """
        filename = self.generate_filename(url, '.docx')
        file_path = self._get_output_path(self.pages_output_dir, url, filename)
        
        with open(file_path, 'wb') as f:
            f.write(content)
        
        print(f"Saved DOCX: {file_path}")
        return file_path
    
    def save_pdf_content(self, pdf_url: str, original_filename: str, content: str, output_format: str = 'markdown') -> str:
        """
        Save PDF extracted content to file.
        
        Args:
            pdf_url: URL of the PDF file
            original_filename: Original PDF filename
            content: Extracted content to save
            output_format: Format of the content ('markdown', 'html', etc.)
            
        Returns:
            Path to saved file
        """
        # Create filename with original PDF name reference
        extension = '.md' if output_format.lower() in ['markdown', 'md'] else f'.{output_format}'
        filename = self._sanitize_url_for_filename(original_filename).replace('.pdf', '').replace('.PDF', '') + extension
        file_path = self._get_output_path(self.pdf_output_dir, pdf_url, filename)
        
        # Add header with source information
        if output_format.lower() in ['markdown', 'md']:
            header = f"# Source PDF: {original_filename}\n# URL: {pdf_url}\n\n---\n\n"
            content = header + content
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Saved PDF content: {file_path}")
        return file_path
    
    def save_processed_html(self, url: str, content: str) -> str:
        """
        Save processed HTML content to file.
        
        Args:
            url: URL the content came from
            content: Processed HTML content to save
            
        Returns:
            Path to saved file
        """
        filename = self.generate_filename(url, '_processed.html')
        file_path = self._get_output_path(self.pages_output_dir, url, filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Saved processed HTML: {file_path}")
        return file_path
    
    def save_content(self, url: str, content: Any, output_format: str) -> str:
        """
        Save content in specified format.
        
        Args:
            url: URL the content came from
            content: Content to save
            output_format: Format to save ('html', 'markdown', 'docx')
            
        Returns:
            Path to saved file
            
        Raises:
            ValueError: If output format is not supported
        """
        format_map = {
            'html': self.save_processed_html,
            'markdown': self.save_markdown,
            'md': self.save_markdown,
            'docx': self.save_docx
        }
        
        if output_format.lower() not in format_map:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        return format_map[output_format.lower()](url, content)
    
    def get_output_stats(self) -> Dict[str, int]:
        """
        Get statistics about output files.
        
        Returns:
            Dictionary containing file counts by type
        """
        stats = {
            'html_files': 0,
            'markdown_files': 0,
            'docx_files': 0,
            'total_files': 0
        }
        
        # Count HTML files
        if os.path.exists(self.html_output_dir):
            html_files = [f for f in os.listdir(self.html_output_dir) if f.endswith('.html')]
            stats['html_files'] = len(html_files)
        
        # Count other files
        if os.path.exists(self.pages_output_dir):
            for filename in os.listdir(self.pages_output_dir):
                if filename.endswith('.md'):
                    stats['markdown_files'] += 1
                elif filename.endswith('.docx'):
                    stats['docx_files'] += 1
        
        stats['total_files'] = stats['html_files'] + stats['markdown_files'] + stats['docx_files']
        return stats