"""
Document conversion module for handling Docling operations.
"""

import os
from typing import Dict, Any, Optional
from docling.document_converter import DocumentConverter as DoclingConverter


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
    
    def convert_with_cleanup(self, html_file_path: str, output_format: str, source_url: str = None) -> Any:
        """
        Convert document and clean up temporary file.
        
        Args:
            html_file_path: Path to HTML file to convert
            output_format: Target format ('markdown', 'html', 'docx')
            source_url: Optional source URL to include in markdown header
            
        Returns:
            Converted content in requested format
        """
        try:
            result = self.convert_document(html_file_path, output_format)
            
            # Add URL header for markdown format
            if output_format.lower() in ['markdown', 'md'] and source_url:
                url_header = f"# Source: {source_url}\n\n---\n\n"
                result = url_header + result
            
            return result
        finally:
            self.cleanup_temp_file(html_file_path)