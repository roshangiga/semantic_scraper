"""
PDF processor module for downloading and extracting PDF content.
"""

import os
import requests
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, unquote
from docling.document_converter import DocumentConverter as DoclingConverter


class PDFProcessor:
    """Handles PDF downloading and content extraction operations."""
    
    def __init__(self, config: Dict[str, Any], markdown_processing_config: Dict[str, Any] = None, docling_config: Dict[str, Any] = None):
        """
        Initialize the PDFProcessor.
        
        Args:
            config: Configuration dictionary containing link processing settings
            markdown_processing_config: Configuration for markdown post-processing
            docling_config: Configuration for docling conversion
        """
        self.config = config
        self.markdown_processing_config = markdown_processing_config or {}
        self.docling_config = docling_config or {}
        self.process_pdf_links = config.get('process_pdf_links', True)
        self.converter = DoclingConverter()
    
    def is_pdf_url(self, url: str) -> bool:
        """
        Check if URL points to a PDF file.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL likely points to a PDF
        """
        # Check by extension
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        return path.endswith('.pdf')
    
    def get_pdf_filename(self, url: str) -> str:
        """
        Extract filename from PDF URL.
        
        Args:
            url: PDF URL
            
        Returns:
            Filename extracted from URL
        """
        parsed_url = urlparse(url)
        path = unquote(parsed_url.path)
        filename = os.path.basename(path)
        
        # If no filename found, create one from URL
        if not filename or not filename.endswith('.pdf'):
            filename = 'document.pdf'
        
        return filename
    
    def download_pdf(self, url: str) -> Optional[str]:
        """
        Download PDF from URL to temporary file in current directory.
        
        Args:
            url: URL of PDF to download
            
        Returns:
            Path to temporary file or None if download failed
        """
        try:
            # Download PDF
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()
            
            # Create temporary filename in current directory
            import uuid
            temp_filename = f"temp_pdf_{uuid.uuid4().hex[:8]}.pdf"
            temp_path = os.path.join(os.getcwd(), temp_filename)
            
            # Save to temporary file in current directory
            with open(temp_path, 'wb') as temp_file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_file.write(chunk)
            
            return temp_path
            
        except requests.RequestException as e:
            print(f"   ❌ Error downloading PDF from {url}: {e}")
            return None
        except Exception as e:
            print(f"   ❌ Unexpected error downloading PDF: {e}")
            return None
    
    def extract_pdf_content(self, pdf_path: str, output_format: str = 'markdown') -> Optional[str]:
        """
        Extract content from PDF file.
        
        Args:
            pdf_path: Path to PDF file
            output_format: Output format ('markdown', 'html', etc.)
            
        Returns:
            Extracted content or None if extraction failed
        """
        try:
            # Try to convert PDF using Docling
            conv_result = self.converter.convert(pdf_path)
            
            if output_format.lower() in ['markdown', 'md']:
                # Get markdown settings from docling config
                markdown_config = self.docling_config.get('markdown', {})
                markdown_params = {
                    'include_annotations': markdown_config.get('include_annotations', True),
                    'mark_annotations': markdown_config.get('mark_annotations', False),
                    'escape_underscores': markdown_config.get('escape_underscores', True),
                    'image_placeholder': markdown_config.get('image_placeholder', '<!-- image -->'),
                    'enable_chart_tables': markdown_config.get('enable_chart_tables', True)
                }
                # Filter out None values
                markdown_params = {k: v for k, v in markdown_params.items() if v is not None}
                content = conv_result.document.export_to_markdown(**markdown_params)
            elif output_format.lower() == 'html':
                html_config = self.docling_config.get('html', {})
                html_params = {
                    'include_annotations': html_config.get('include_annotations', True),
                    'formula_to_mathml': html_config.get('formula_to_mathml', True)
                }
                # Filter out None values
                html_params = {k: v for k, v in html_params.items() if v is not None}
                content = conv_result.document.export_to_html(**html_params)
            else:
                print(f"   ⚠️  Unsupported output format: {output_format}")
                return None
            
            return content
            
        except PermissionError as e:
            print(f"   ❌ Permission error with Docling models: {e}")
            print(f"   ⚠️  Try running as administrator or check Hugging Face cache permissions")
            return None
        except Exception as e:
            error_msg = str(e)
            if "WinError 1314" in error_msg or "required privilege" in error_msg:
                print(f"   ❌ Windows permission error: {e}")
                print(f"   ⚠️  Try running as administrator or check file permissions")
                return None
            else:
                print(f"   ❌ Error extracting PDF content: {e}")
                return None
    
    
    def cleanup_temp_file(self, temp_path: str) -> None:
        """
        Clean up temporary PDF file.
        
        Args:
            temp_path: Path to temporary file
        """
        try:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        except OSError as e:
            print(f"Warning: Could not delete temporary file {temp_path}: {e}")
    
    def process_pdf_url(self, url: str, output_formats: List[str]) -> Dict[str, Any]:
        """
        Process a PDF URL: download and extract content.
        
        Args:
            url: URL of PDF to process
            output_formats: List of output formats to generate
            
        Returns:
            Dictionary with results including filename and content for each format
        """
        results = {
            'url': url,
            'filename': self.get_pdf_filename(url),
            'content': {},
            'success': False
        }
        
        if not self.process_pdf_links:
            return results
        
        # Download PDF
        temp_path = self.download_pdf(url)
        if not temp_path:
            return results
        
        try:
            # Extract content for each format
            for format in output_formats:
                content = self.extract_pdf_content(temp_path, format)
                if content:
                    results['content'][format] = content
                    results['success'] = True
        finally:
            # Clean up temporary file
            self.cleanup_temp_file(temp_path)
        
        return results