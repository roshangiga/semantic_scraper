"""
File management module for handling file operations.
"""

import os
import shutil
import hashlib
import sys
from pathlib import Path
from typing import Dict, Any, List, Set


class FileManager:
    """Handles file operations and directory management."""
    
    def __init__(self, config: Dict[str, Any], markdown_processing_config: Dict[str, Any] = None):
        """
        Initialize the FileManager.
        
        Args:
            config: Configuration dictionary containing file management settings
            markdown_processing_config: Configuration for markdown processing
        """
        self.config = config
        self.markdown_processing_config = markdown_processing_config or {}
        self.html_output_dir = config.get('html_output_dir', 'crawled_html')
        self.pages_output_dir = config.get('pages_output_dir', 'crawled_docling')
        self.pdf_output_dir = config.get('pdf_output_dir', 'crawled_pdf')
        self.semantic_output_dir = config.get('semantic_output_dir', 'crawled_semantic')
        self.report_output_dir = config.get('report_output_dir', 'crawled_report')
        self.filename_template = config.get('filename_template', '{sanitized_url}')
        self.delete_existing_folders = config.get('delete_existing_folders', False)
        self.use_domain_subfolders = config.get('use_domain_subfolders', True)
    
    def _log(self, msg: str) -> None:
        """Log via progress formatter if available, else print.
        """
        print(msg)
    
    def setup_directories(self) -> None:
        """Set up output directories."""
        directories = [self.html_output_dir, self.pages_output_dir, self.pdf_output_dir, self.semantic_output_dir, self.report_output_dir]
        
        for directory in directories:
            if self.delete_existing_folders and os.path.exists(directory):
                self._log(f"Deleting existing directory: {directory}")
                self._delete_directory_with_retry(directory)
            
            os.makedirs(directory, exist_ok=True)
            self._log(f"Created directory: {directory}")
    
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
                    self._log(f"Attempt {attempt + 1} failed, retrying in 1 second...")
                    time.sleep(1)
                else:
                    self._log(f"Warning: Could not delete {directory} after {max_retries} attempts.")
                    self._log(f"Error: {e}")
                    self._log("Please close any programs using files in this directory and try again.")
                    # Don't raise the error, just continue
                    return
            except Exception as e:
                self._log(f"Unexpected error deleting {directory}: {e}")
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
        
        # Verify file was saved successfully
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            print(f"│  ├─ ✔️ Saved HTML")
        else:
            print(f"│  ├─ ❌ Failed to save HTML")
        sys.stdout.flush()
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
        
        # Verify file was saved successfully
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            print(f"│  ├─ ✔️ Saved Markdown")
        else:
            print(f"│  ├─ ❌ Failed to save Markdown")
        sys.stdout.flush()
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
        
        self._log(f"Saved DOCX: {file_path}")
        return file_path
    
    def save_semantic_chunks(self, url: str, content: str) -> str:
        """
        Save semantic chunks content to file.
        
        Args:
            url: URL the content came from
            content: Semantic chunks content to save
            
        Returns:
            Path to saved file
        """
        filename = self.generate_filename(url, '.md')
        file_path = self._get_output_path(self.semantic_output_dir, url, filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        self._log(f"💾 Saved semantic chunks: {file_path}")
        return file_path
    
    def get_semantic_file_path(self, url: str) -> str:
        """
        Get the file path for semantic chunks without saving.
        
        Args:
            url: URL to get path for
            
        Returns:
            Path where semantic chunks would be saved
        """
        filename = self.generate_filename(url, '.md')
        return self._get_output_path(self.semantic_output_dir, url, filename)
    
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
        
        print(f"│  ├─ ✔️ Saved PDF content")
        sys.stdout.flush()
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
        
        self._log(f"Saved processed HTML: {file_path}")
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
            'pdf_files': 0,
            'total_files': 0
        }
        
        # Count HTML files (including in subdirectories if domain subfolders are used)
        if os.path.exists(self.html_output_dir):
            for root, dirs, files in os.walk(self.html_output_dir):
                for file in files:
                    if file.endswith('.html'):
                        stats['html_files'] += 1
        
        # Count other files (including in subdirectories if domain subfolders are used)
        if os.path.exists(self.pages_output_dir):
            for root, dirs, files in os.walk(self.pages_output_dir):
                for file in files:
                    if file.endswith('.md'):
                        stats['markdown_files'] += 1
                    elif file.endswith('.docx'):
                        stats['docx_files'] += 1
        
        # Count PDF files  
        if os.path.exists(self.pdf_output_dir):
            for root, dirs, files in os.walk(self.pdf_output_dir):
                for file in files:
                    if file.endswith('.md'):  # PDF content converted to markdown
                        stats['pdf_files'] += 1
        
        stats['total_files'] = stats['html_files'] + stats['markdown_files'] + stats['docx_files'] + stats['pdf_files']
        return stats
    
    def _get_file_content_hash(self, file_path: str) -> str:
        """
        Get hash of file content excluding the first line (Source: line).
        
        Args:
            file_path: Path to the file
            
        Returns:
            Hash of the file content
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        # Skip the first line if it's a Source: line
        if lines and lines[0].startswith('# Source:'):
            content_to_hash = ''.join(lines[1:])
        else:
            content_to_hash = ''.join(lines)
        
        # Generate hash of the content
        return hashlib.md5(content_to_hash.encode('utf-8')).hexdigest()
    
    def _is_blank_file(self, file_path: str) -> bool:
        """
        Check if a markdown file is blank (only contains Source: line).
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file is blank, False otherwise
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if not lines:
            return True
            
        # If first line is Source: line
        if lines[0].startswith('# Source:'):
            # Check if rest of file is empty or only whitespace
            remaining_content = ''.join(lines[1:]).strip()
            return len(remaining_content) == 0
        
        # Check if entire file is empty or only whitespace
        return len(''.join(lines).strip()) == 0
    
    def remove_duplicate_and_blank_files(self, directory: str = None) -> Dict[str, int]:
        """
        Remove duplicate and blank markdown files from the output directory.
        
        Args:
            directory: Directory to process (defaults to pages_output_dir)
            
        Returns:
            Dictionary with statistics about removed files
        """
        if directory is None:
            directory = self.pages_output_dir
        
        if not os.path.exists(directory):
            return {'duplicates_removed': 0, 'blank_files_removed': 0}
        
        stats = {'duplicates_removed': 0, 'blank_files_removed': 0}
        
        # Check if features are enabled
        remove_duplicate_files = self.markdown_processing_config.get('remove_duplicate_files', False)
        remove_blank_files = self.markdown_processing_config.get('remove_blank_files', False)
        
        if not remove_duplicate_files and not remove_blank_files:
            return stats
        
        # Get all markdown files
        markdown_files = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.md'):
                    markdown_files.append(os.path.join(root, file))
        
        # Remove blank files first
        if remove_blank_files:
            files_to_remove = []
            for file_path in markdown_files:
                if self._is_blank_file(file_path):
                    files_to_remove.append(file_path)
            
            for file_path in files_to_remove:
                try:
                    os.remove(file_path)
                    print(f"Removed blank file: {file_path}")
                    stats['blank_files_removed'] += 1
                    markdown_files.remove(file_path)
                except Exception as e:
                    print(f"Error removing blank file {file_path}: {e}")
        
        # Remove duplicate files
        if remove_duplicate_files:
            seen_hashes = {}
            files_to_remove = []
            
            for file_path in markdown_files:
                try:
                    file_hash = self._get_file_content_hash(file_path)
                    
                    if file_hash in seen_hashes:
                        # This is a duplicate
                        files_to_remove.append(file_path)
                    else:
                        seen_hashes[file_hash] = file_path
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")
            
            for file_path in files_to_remove:
                try:
                    os.remove(file_path)
                    print(f"Removed duplicate file: {file_path}")
                    stats['duplicates_removed'] += 1
                except Exception as e:
                    print(f"Error removing duplicate file {file_path}: {e}")
        
        if stats['duplicates_removed'] > 0 or stats['blank_files_removed'] > 0:
            print(f"\nCleanup complete: Removed {stats['duplicates_removed']} duplicate files and {stats['blank_files_removed']} blank files")
        
        return stats