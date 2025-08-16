"""
Document conversion module for handling Docling operations.
"""

import os
import logging
from typing import Dict, Any, Optional
from docling.document_converter import DocumentConverter as DoclingConverter

# Suppress docling INFO messages using rich logging
try:
    from rich.logging import RichHandler
    
    # Configure logging to suppress docling info messages
    docling_loggers = [
        'docling',
        'docling.pipeline.standard_pdf_pipeline', 
        'docling.datamodel.pipeline_options',
        'docling.pipeline.simple_pipeline',
        'docling.backend.pypdfium2_backend',
        'docling.pipeline'
    ]
    
    for logger_name in docling_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.ERROR)  # Only show errors
        
except ImportError:
    # Fallback if rich not available
    logging.getLogger('docling').setLevel(logging.ERROR)


class DocumentConverter:
    """Handles document conversion operations using Docling."""
    
    def __init__(self, config: Dict[str, Any], markdown_processing_config: Dict[str, Any] = None):
        """
        Initialize the DocumentConverter.
        
        Args:
            config: Configuration dictionary containing docling settings
            markdown_processing_config: Configuration for markdown post-processing
        """
        self.config = config
        self.markdown_processing_config = markdown_processing_config or {}
        self.converter = DoclingConverter()
    
    def convert_to_markdown(self, html_file_path: str) -> str:
        """
        Convert HTML file to Markdown format.
        
        Args:
            html_file_path: Path to HTML file to convert
            
        Returns:
            Markdown content as string
        """
        conv_result = self.converter.convert(html_file_path)
        
        # Get markdown settings from config
        markdown_config = self.config.get('markdown', {})
        
        # Apply configuration parameters
        markdown_params = {
            'include_annotations': markdown_config.get('include_annotations', True),
            'mark_annotations': markdown_config.get('mark_annotations', False),
            'escape_underscores': markdown_config.get('escape_underscores', True),
            'image_placeholder': markdown_config.get('image_placeholder', '<!-- image -->'),
            'enable_chart_tables': markdown_config.get('enable_chart_tables', True)
        }
        
        # Filter out None values
        markdown_params = {k: v for k, v in markdown_params.items() if v is not None}
        
        markdown_content = conv_result.document.export_to_markdown(**markdown_params)
        
        # Apply post-processing to remove excluded sections
        processed_content = self._post_process_markdown(markdown_content)
        
        # Remove duplicate sections (always enabled)
        processed_content = self._remove_duplicate_sections(processed_content)
        
        # Remove duplicate lines within the file (always enabled)
        processed_content = self._remove_duplicate_lines(processed_content)
        
        return processed_content
    
    def convert_to_html(self, html_file_path: str) -> str:
        """
        Convert HTML file to formatted HTML.
        
        Args:
            html_file_path: Path to HTML file to convert
            
        Returns:
            HTML content as string
        """
        conv_result = self.converter.convert(html_file_path)
        
        # Get HTML settings from config
        html_config = self.config.get('html', {})
        
        # Apply configuration parameters
        html_params = {
            'include_annotations': html_config.get('include_annotations', True),
            'formula_to_mathml': html_config.get('formula_to_mathml', True)
        }
        
        # Filter out None values
        html_params = {k: v for k, v in html_params.items() if v is not None}
        
        return conv_result.document.export_to_html(**html_params)
    
    def convert_to_docx(self, html_file_path: str) -> bytes:
        """
        Convert HTML file to DOCX format.
        
        Args:
            html_file_path: Path to HTML file to convert
            
        Returns:
            DOCX content as bytes
        """
        conv_result = self.converter.convert(html_file_path)
        return conv_result.document.export_to_word()
    
    def _post_process_markdown(self, markdown_content: str) -> str:
        """
        Post-process markdown content to remove excluded sections.
        
        Args:
            markdown_content: Original markdown content
            
        Returns:
            Processed markdown content with excluded sections removed
        """
        sections_to_ignore = self.markdown_processing_config.get('sections_to_ignore', [])
        
        if not sections_to_ignore:
            return markdown_content
        
        import re
        
        # Process each section to ignore
        for section_title in sections_to_ignore:
            # Pattern to match section headers at any level (# ## ### etc.)
            # and remove everything from that header to the next header at the same or higher level
            pattern = r'^(#{1,6})\s*' + re.escape(section_title) + r'.*?(?=^#{1,6}\s|\Z)'
            
            # Remove the section and its content
            markdown_content = re.sub(pattern, '', markdown_content, flags=re.MULTILINE | re.DOTALL | re.IGNORECASE)
        
        # Clean up multiple consecutive empty lines
        markdown_content = re.sub(r'\n\s*\n\s*\n+', '\n\n', markdown_content)
        
        return markdown_content.strip()
    
    def _remove_duplicate_sections(self, markdown_content: str) -> str:
        """
        Remove duplicate sections from markdown content.
        
        Args:
            markdown_content: Original markdown content
            
        Returns:
            Processed markdown content with duplicates removed
        """
        import re
        
        # Split content into lines
        lines = markdown_content.split('\n')
        
        # Track seen sections and their content
        seen_sections = {}
        result_lines = []
        current_section = None
        current_section_content = []
        current_section_level = 0
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Check if this is a header line
            header_match = re.match(r'^(#{1,6})\s+(.+)$', line)
            
            if header_match:
                # Process previous section if exists
                if current_section is not None:
                    section_key = (current_section, '\n'.join(current_section_content).strip())
                    if section_key not in seen_sections:
                        # Add the section header
                        result_lines.append('#' * current_section_level + ' ' + current_section)
                        # Add the section content
                        result_lines.extend(current_section_content)
                        seen_sections[section_key] = True
                
                # Start new section
                current_section_level = len(header_match.group(1))
                current_section = header_match.group(2).strip()
                current_section_content = []
            else:
                # Add line to current section content
                if current_section is not None:
                    current_section_content.append(line)
                else:
                    # Lines before any section
                    result_lines.append(line)
            
            i += 1
        
        # Process the last section
        if current_section is not None:
            section_key = (current_section, '\n'.join(current_section_content).strip())
            if section_key not in seen_sections:
                result_lines.append('#' * current_section_level + ' ' + current_section)
                result_lines.extend(current_section_content)
        
        # Join lines back together
        return '\n'.join(result_lines)
    
    def _remove_duplicate_lines(self, markdown_content: str) -> str:
        """
        Remove duplicate lines from markdown content, preserving the first occurrence.
        Excludes the first line (Source: line) from duplicate detection.
        
        Args:
            markdown_content: Original markdown content
            
        Returns:
            Processed markdown content with duplicate lines removed
        """
        lines = markdown_content.split('\n')
        
        if not lines:
            return markdown_content
        
        result_lines = []
        first_line = None
        
        # Check if first line is a Source: line
        if lines and lines[0].startswith('# Source:'):
            first_line = lines[0]
            lines = lines[1:]  # Process remaining lines
        
        # Track seen lines to remove duplicates
        seen_lines = set()
        
        for line in lines:
            # Normalize line for comparison (strip whitespace)
            normalized_line = line.strip()
            
            # Keep empty lines but don't deduplicate them
            if not normalized_line:
                result_lines.append(line)
            # Only add non-empty lines if we haven't seen them before
            elif normalized_line not in seen_lines:
                result_lines.append(line)
                seen_lines.add(normalized_line)
        
        # Reconstruct the content
        if first_line is not None:
            return first_line + '\n' + '\n'.join(result_lines)
        else:
            return '\n'.join(result_lines)
    
    def _simple_html_to_text(self, html_file_path: str) -> str:
        """
        Simple HTML to text conversion without Docling (fallback).
        
        Args:
            html_file_path: Path to HTML file
            
        Returns:
            Simple text content
        """
        from bs4 import BeautifulSoup
        
        # Try multiple encodings to handle problematic content
        html_content = None
        encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                with open(html_file_path, 'r', encoding=encoding) as f:
                    html_content = f.read()
                break
            except UnicodeDecodeError:
                continue
        
        if html_content is None:
            # Last resort: read as binary and decode with error handling
            with open(html_file_path, 'rb') as f:
                raw_content = f.read()
            html_content = raw_content.decode('utf-8', errors='ignore')
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text
        text = soup.get_text()
        
        # Break into lines and remove leading/trailing space
        lines = (line.strip() for line in text.splitlines())
        # Break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # Drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def _is_problematic_content(self, html_file_path: str) -> bool:
        """
        Check if HTML content contains patterns that might cause Docling to crash.
        
        Args:
            html_file_path: Path to HTML file
            
        Returns:
            True if content is potentially problematic
        """
        try:
            with open(html_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for patterns that commonly cause crashes
            problematic_patterns = [
                # Complex forms with many inputs
                content.count('<input') > 100,
                # Excessive script tags
                content.count('<script') > 50,
                # Large number of nested divs (potential memory issues)
                content.count('<div') > 500,
                # Very long single lines (can cause parsing issues)
                any(len(line) > 50000 for line in content.split('\n')[:10]),
                # Suspicious content patterns from the problematic URL
                '?device=fixedphone' in content or 'fixedphone' in content,
                # Check for potential infinite loops in CSS/JS
                'while(' in content or 'for(' in content,
            ]
            
            return any(problematic_patterns)
        except Exception:
            # If we can't read the file, assume it's problematic
            return True
    
    def convert_document(self, html_file_path: str, output_format: str) -> Any:
        """
        Convert document to specified format.
        
        Args:
            html_file_path: Path to HTML file to convert
            output_format: Target format ('markdown', 'html', 'docx')
            
        Returns:
            Converted content in requested format
            
        Raises:
            ValueError: If output format is not supported
        """
        format_map = {
            'markdown': self.convert_to_markdown,
            'md': self.convert_to_markdown,
            'html': self.convert_to_html,
            'docx': self.convert_to_docx
        }
        
        if output_format.lower() not in format_map:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        return format_map[output_format.lower()](html_file_path)
    
    def cleanup_temp_file(self, temp_file_path: str) -> None:
        """
        Clean up temporary file.
        
        Args:
            temp_file_path: Path to temporary file to delete
        """
        try:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        except OSError as e:
            print(f"Warning: Could not delete temporary file {temp_file_path}: {e}")
    
    def convert_with_cleanup(self, html_file_path: str, output_format: str, source_url: str = None) -> tuple:
        """
        Convert document and clean up temporary file.
        
        Args:
            html_file_path: Path to HTML file to convert
            output_format: Target format ('markdown', 'html', 'docx')
            source_url: Optional source URL to include in markdown header
            
        Returns:
            Tuple of (converted content, conversion_time_seconds, fallback_message)
            fallback_message is None if no fallback was used, otherwise contains the fallback reason
        """
        import time
        start_time = time.time()
        
        try:
            # Check if Docling is enabled
            if not self.config.get('enabled', True):
                # Use simple fallback conversion
                result = self._simple_html_to_text(html_file_path)
                if output_format.lower() in ['markdown', 'md'] and source_url:
                    result = f"# Source: {source_url}\n\n---\n\n" + result
                conversion_time = time.time() - start_time
                return result, conversion_time, "Docling disabled, using simple conversion"
            
            # Check file size - skip very large files that might cause crashes
            import os
            file_size = os.path.getsize(html_file_path)
            max_size_mb = self.config.get('max_file_size_mb', 10)
            if file_size > max_size_mb * 1024 * 1024:  # Configurable MB limit for Docling
                # Use simple fallback conversion
                result = self._simple_html_to_text(html_file_path)
                if output_format.lower() in ['markdown', 'md'] and source_url:
                    result = f"# Source: {source_url}\n\n---\n\n" + result
                conversion_time = time.time() - start_time
                return result, conversion_time, f"File too large for Docling ({file_size / 1024 / 1024:.1f}MB), using fallback"
            
            try:
                # Additional check for potentially problematic content patterns
                if self._is_problematic_content(html_file_path):
                    result = self._simple_html_to_text(html_file_path)
                    if output_format.lower() in ['markdown', 'md'] and source_url:
                        result = f"# Source: {source_url}\n\n---\n\n" + result
                    conversion_time = time.time() - start_time
                    return result, conversion_time, "Detected problematic content patterns, using fallback"
                
                result = self.convert_document(html_file_path, output_format)
                
                # Add URL header for markdown format
                if output_format.lower() in ['markdown', 'md'] and source_url:
                    url_header = f"# Source: {source_url}\n\n---\n\n"
                    result = url_header + result
                
                conversion_time = time.time() - start_time
                return result, conversion_time, None  # No fallback used
            except Exception as e:
                # If Docling fails, use fallback conversion
                try:
                    result = self._simple_html_to_text(html_file_path)
                    if output_format.lower() in ['markdown', 'md'] and source_url:
                        result = f"# Source: {source_url}\n\n---\n\n" + result
                    conversion_time = time.time() - start_time
                    return result, conversion_time, f"Docling conversion failed, using fallback: {str(e)[:100]}"
                except Exception as fallback_error:
                    # Last resort - return error message
                    error_msg = f"[Conversion failed: {str(e)[:100]}]"
                    conversion_time = time.time() - start_time
                    if output_format.lower() in ['markdown', 'md'] and source_url:
                        return f"# Source: {source_url}\n\n---\n\n{error_msg}", conversion_time, f"Both Docling and fallback failed: {str(fallback_error)[:50]}"
                    return error_msg, conversion_time, f"Both Docling and fallback failed: {str(fallback_error)[:50]}"
                    
        finally:
            self.cleanup_temp_file(html_file_path)