"""
Crawler orchestrator module for coordinating all crawling operations.
"""

import asyncio
import yaml
from typing import Dict, List, Any
from pathlib import Path

from .web_crawler import WebCrawler
from .html_processor import HTMLProcessor
from .document_converter import DocumentConverter
from .file_manager import FileManager
from .pdf_processor import PDFProcessor


class CrawlerOrchestrator:
    """Orchestrates the entire crawling and conversion process."""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize the CrawlerOrchestrator.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.web_crawler = None
        self.html_processor = HTMLProcessor(
            self.config.get('link_processing', {}), 
            self.config.get('html_cleaning', {})
        )
        self.document_converter = DocumentConverter(
            self.config.get('crawler', {}).get('docling', {}),
            self.config.get('markdown_processing', {})
        )
        self.file_manager = FileManager(
            self.config.get('crawler', {}).get('file_manager', {}),
            self.config.get('markdown_processing', {})
        )
        self.pdf_processor = PDFProcessor(
            self.config.get('link_processing', {}),
            self.config.get('markdown_processing', {}),
            self.config.get('crawler', {}).get('docling', {})
        )
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Configuration file not found: {config_path}")
            return {}
        except yaml.YAMLError as e:
            print(f"Error parsing configuration file: {e}")
            return {}
    
    def get_domains_config(self) -> List[Dict[str, Any]]:
        """
        Get domains configuration.
        
        Returns:
            List of domain configurations
        """
        return self.config.get('domains', [])
    
    def get_output_formats(self) -> List[str]:
        """
        Get configured output formats.
        
        Returns:
            List of output formats
        """
        return self.config.get('crawler', {}).get('output_formats', ['html', 'markdown'])
    
    def get_crawl4ai_config(self) -> Dict[str, Any]:
        """
        Get Crawl4AI configuration.
        
        Returns:
            Crawl4AI configuration
        """
        return self.config.get('crawler', {}).get('crawl4ai', {})
    
    async def crawl_and_convert(self, output_formats: List[str] = None) -> Dict[str, Any]:
        """
        Perform the complete crawling and conversion process.
        
        Args:
            output_formats: List of output formats to generate
            
        Returns:
            Dictionary containing process results and statistics
        """
        if output_formats is None:
            output_formats = self.get_output_formats()
        
        # Display configuration before starting
        self._display_startup_config(output_formats)
        
        # Setup directories
        self.file_manager.setup_directories()
        
        # Get domain configurations
        domains = self.get_domains_config()
        if not domains:
            print("❌ No domains configured")
            return {'error': 'No domains configured'}
        
        # Initialize web crawler with combined config
        crawl_config = self.get_crawl4ai_config()
        # Add link processing settings to crawler config
        link_config = self.config.get('link_processing', {})
        crawl_config.update({
            'exclude_urls': link_config.get('exclude_urls', []),
            'exclude_section_urls': link_config.get('exclude_section_urls', True)
        })
        
        results = {'processed_pages': [], 'errors': [], 'stats': {}}
        
        print(f"\n🚀 Starting crawl process...")
        print(f"📊 Max pages per domain: {crawl_config.get('max_pages', 100)}")
        print(f"⏱️  Delay before HTML capture: {crawl_config.get('delay_before_return_html', 2.5)}s")
        print(f"🔄 Bypass cache: {crawl_config.get('bypass_cache', True)}")
        print(f"🚫 Exclude section URLs (#): {crawl_config.get('exclude_section_urls', True)}")
        print(f"🔁 Max retries per page: {crawl_config.get('max_retries', 3)}")
        print(f"⏰ Retry delay: {crawl_config.get('retry_delay', 5)}s")
        print("-" * 60)
        
        async with WebCrawler(crawl_config) as crawler:
            # Process pages as they are crawled (streaming approach)
            page_count = 0
            try:
                async for crawl_result in crawler.crawl_all_streaming(domains):
                    page_count += 1
                    try:
                        print(f"🔄 [{page_count}] Processing: {crawl_result['url']}")
                        await self._process_single_page(crawl_result, output_formats)
                        results['processed_pages'].append(crawl_result['url'])
                        print(f"✅ [{page_count}] Completed: {crawl_result['url']}")
                    except Exception as e:
                        error_info = {
                            'url': crawl_result['url'],
                            'error': str(e)
                        }
                        results['errors'].append(error_info)
                        print(f"❌ [{page_count}] Error processing {crawl_result['url']}: {e}")
            except Exception as e:
                print(f"\n⚠️ Crawler stopped unexpectedly after {page_count} pages")
                print(f"   Error: {str(e)[:200]}")
                # Continue with cleanup even if crawler crashes
            
            # Save failed URLs if any
            if hasattr(crawler, 'failed_urls') and crawler.failed_urls:
                crawler.save_failed_urls()
                results['failed_urls'] = len(crawler.failed_urls)
            
            if page_count == 0:
                print("⚠️  No pages were successfully crawled")
                return results
        
        # Get final statistics
        results['stats'] = self.file_manager.get_output_stats()
        
        # Clean up duplicate and blank files if enabled
        cleanup_stats = self.file_manager.remove_duplicate_and_blank_files()
        if cleanup_stats['duplicates_removed'] > 0 or cleanup_stats['blank_files_removed'] > 0:
            results['cleanup_stats'] = cleanup_stats
        
        # Clean up checkpoint file on successful completion
        self._cleanup_checkpoint()
        
        print(f"\n🎉 Crawling completed!")
        print(f"✅ Successfully processed: {len(results['processed_pages'])} pages")
        if results['errors']:
            print(f"❌ Processing errors: {len(results['errors'])} pages")
        if results.get('failed_urls', 0) > 0:
            print(f"🔄 Failed after retries: {results['failed_urls']} pages (saved to failed_urls.txt)")
        
        return results
    
    def _cleanup_checkpoint(self):
        """Clean up checkpoint file after successful completion."""
        import os
        if os.path.exists('crawler_checkpoint.json'):
            try:
                os.remove('crawler_checkpoint.json')
                print("   🗑️ Cleanup: Removed checkpoint file")
            except Exception as e:
                print(f"   ⚠️ Could not remove checkpoint file: {e}")
    
    def _display_startup_config(self, output_formats: List[str]) -> None:
        """Display configuration information before starting crawl."""
        print("\n" + "=" * 60)
        print("🔧 CRAWLER CONFIGURATION")
        print("=" * 60)
        
        # Output formats
        print(f"📄 Output formats: {', '.join(output_formats)}")
        
        # File settings
        file_config = self.config.get('crawler', {}).get('file_manager', {})
        print(f"📁 HTML directory: {file_config.get('html_output_dir', 'crawled_html')}")
        print(f"📁 Pages directory: {file_config.get('pages_output_dir', 'crawled_pages')}")
        print(f"📂 Use domain subfolders: {file_config.get('use_domain_subfolders', True)}")
        print(f"🗑️  Delete existing folders: {file_config.get('delete_existing_folders', False)}")
        
        # Domains
        domains = self.get_domains_config()
        print(f"\n🌐 Configured domains: {len(domains)}")
        for i, domain in enumerate(domains, 1):
            print(f"   {i}. {domain['domain']}")
            print(f"      📊 Start URLs: {len(domain.get('start_urls', []))}")
            if domain.get('js_code'):
                print(f"      ⚡ JavaScript: Custom code defined")
            if domain.get('wait_for'):
                print(f"      ⏳ Wait condition: {domain['wait_for'][:50]}...")
            if domain.get('html_classes_to_only_include'):
                print(f"      🎯 Only include: {domain['html_classes_to_only_include']}")
        
        # Global settings
        html_cleaning = self.config.get('html_cleaning', {})
        if html_cleaning:
            print(f"\n🧹 HTML cleaning:")
            print(f"   🔍 Remove CSS hidden: {html_cleaning.get('remove_css_hidden_elements', True)}")
            print(f"   🏷️  Remove elements: {len(html_cleaning.get('html_elements_to_remove', []))}")
            print(f"   🎨 Remove classes: {len(html_cleaning.get('html_classes_to_remove', []))}")
            print(f"   💬 Remove comments: {len(html_cleaning.get('comment_blocks_to_remove', []))}")
        
        # Markdown processing
        markdown_processing = self.config.get('markdown_processing', {})
        if markdown_processing:
            print(f"\n📝 Markdown processing:")
            sections_to_ignore = markdown_processing.get('sections_to_ignore', [])
            if sections_to_ignore:
                print(f"   🚫 Ignore sections: {len(sections_to_ignore)}")
                for section in sections_to_ignore[:3]:  # Show first 3 sections
                    print(f"      - \"{section}\"")
                if len(sections_to_ignore) > 3:
                    print(f"      ... and {len(sections_to_ignore) - 3} more")
            print(f"   🔄 Remove duplicate lines: {markdown_processing.get('remove_duplicate_lines', False)}")
            print(f"   📑 Remove duplicate files: {markdown_processing.get('remove_duplicate_files', False)}")
            print(f"   📄 Remove blank files: {markdown_processing.get('remove_blank_files', False)}")
        
        print("=" * 60)
    
    async def _process_single_page(self, crawl_result: Dict[str, Any], output_formats: List[str]) -> None:
        """
        Process a single crawled page.
        
        Args:
            crawl_result: Result from web crawler
            output_formats: List of output formats to generate
        """
        url = crawl_result['url']
        html_content = crawl_result['html']
        domain_config = crawl_result['domain_config']
        
        # Check if this was a PDF redirect
        if 'pdf_redirect' in crawl_result:
            pdf_url = crawl_result['pdf_redirect']
            print(f"   📥 Page redirected to PDF: {pdf_url}")
            
            # Process the PDF if enabled
            if self.config.get('link_processing', {}).get('process_pdf_links', False):
                try:
                    print(f"   📥 Downloading and processing PDF...")
                    # Download and process the PDF - pass list of formats
                    pdf_formats = [fmt for fmt in output_formats if fmt.lower() in ['markdown', 'md']]
                    if pdf_formats:
                        pdf_result = self.pdf_processor.process_pdf_url(pdf_url, pdf_formats)
                        if pdf_result['success']:
                            for format, content in pdf_result['content'].items():
                                # Save with original URL as reference
                                self.file_manager.save_pdf_content(
                                    pdf_url,
                                    pdf_result['filename'],
                                    content,
                                    format
                                )
                            print(f"   ✅ PDF processed and saved to crawled_pdf/")
                            # Successfully processed PDF, no need for placeholder
                            return
                        else:
                            print(f"   ⚠️ No content extracted from PDF")
                            # Save placeholder only if PDF extraction failed
                            placeholder_content = f"# Source: {url}\n\n---\n\n[PDF Document could not be extracted: {pdf_url}]({pdf_url})"
                            self.file_manager.save_markdown(url, placeholder_content)
                except Exception as e:
                    print(f"   ❌ Error processing PDF: {e}")
                    # Save placeholder on error
                    placeholder_content = f"# Source: {url}\n\n---\n\n[PDF Document (error during extraction): {pdf_url}]({pdf_url})"
                    self.file_manager.save_markdown(url, placeholder_content)
            else:
                print(f"   ⚠️ PDF processing is disabled in config")
                # Save placeholder when PDF processing is disabled
                placeholder_content = f"# Source: {url}\n\n---\n\n[PDF Document (processing disabled): {pdf_url}]({pdf_url})"
                self.file_manager.save_markdown(url, placeholder_content)
            return
        
        # Process HTML for document conversion (this includes domain-specific cleaning)
        processed_result = self.html_processor.process_html(
            html_content, 
            url, 
            domain_config
        )
        
        # Save domain-cleaned HTML
        self.file_manager.save_html(url, processed_result['processed_html'])
        
        # Convert to requested formats
        for output_format in output_formats:
            if output_format.lower() == 'html':
                # Save processed HTML
                self.file_manager.save_content(
                    url, 
                    processed_result['processed_html'], 
                    'html'
                )
            else:
                # Convert using Docling
                converted_content = self.document_converter.convert_with_cleanup(
                    processed_result['temp_file_path'], 
                    output_format,
                    url
                )
                
                # Save converted content
                self.file_manager.save_content(url, converted_content, output_format)
        
        # Process PDF URLs if any were found
        if 'pdf_urls' in crawl_result and crawl_result['pdf_urls']:
            await self._process_pdf_urls(crawl_result['pdf_urls'], output_formats)
    
    async def _process_pdf_urls(self, pdf_urls: List[str], output_formats: List[str]) -> None:
        """
        Process PDF URLs by downloading and extracting content.
        
        Args:
            pdf_urls: List of PDF URLs to process
            output_formats: List of output formats to generate
        """
        for pdf_url in pdf_urls:
            try:
                # Process PDF
                result = self.pdf_processor.process_pdf_url(pdf_url, output_formats)
                
                if result['success']:
                    # Save content for each format
                    for format_name, content in result['content'].items():
                        self.file_manager.save_pdf_content(
                            pdf_url, 
                            result['filename'], 
                            content, 
                            format_name
                        )
                else:
                    print(f"   ❌ Failed to process PDF: {pdf_url}")
                    
            except Exception as e:
                print(f"   ❌ Error processing PDF {pdf_url}: {e}")
    
    async def crawl_domain(self, domain: str, output_formats: List[str] = None) -> Dict[str, Any]:
        """
        Crawl a specific domain.
        
        Args:
            domain: Domain to crawl
            output_formats: List of output formats to generate
            
        Returns:
            Dictionary containing process results
        """
        # Filter domains configuration for specific domain
        domains = self.get_domains_config()
        domain_config = [d for d in domains if d['domain'] == domain]
        
        if not domain_config:
            return {'error': f'Domain {domain} not found in configuration'}
        
        # Temporarily update domains configuration
        original_domains = self.config.get('domains', [])
        self.config['domains'] = domain_config
        
        try:
            return await self.crawl_and_convert(output_formats)
        finally:
            # Restore original configuration
            self.config['domains'] = original_domains
    
    def validate_config(self) -> List[str]:
        """
        Validate configuration.
        
        Returns:
            List of validation errors
        """
        errors = []
        
        # Check if domains are configured
        domains = self.get_domains_config()
        if not domains:
            errors.append("No domains configured")
        
        # Check each domain configuration
        for i, domain in enumerate(domains):
            if 'domain' not in domain:
                errors.append(f"Domain {i}: Missing 'domain' field")
            
            if 'start_urls' not in domain:
                errors.append(f"Domain {i}: Missing 'start_urls' field")
            elif not isinstance(domain['start_urls'], list):
                errors.append(f"Domain {i}: 'start_urls' must be a list")
        
        # Check output formats
        output_formats = self.get_output_formats()
        valid_formats = ['html', 'markdown', 'md', 'docx']
        for fmt in output_formats:
            if fmt.lower() not in valid_formats:
                errors.append(f"Invalid output format: {fmt}")
        
        return errors
    
    def print_config_summary(self) -> None:
        """Print a summary of the current configuration."""
        print("\n=== Configuration Summary ===")
        
        # Domains
        domains = self.get_domains_config()
        print(f"Configured domains: {len(domains)}")
        for domain in domains:
            print(f"  - {domain['domain']} ({len(domain.get('start_urls', []))} start URLs)")
        
        # Output formats
        output_formats = self.get_output_formats()
        print(f"Output formats: {', '.join(output_formats)}")
        
        # File settings
        file_config = self.config.get('crawler', {}).get('file_manager', {})
        print(f"HTML output directory: {file_config.get('html_output_dir', 'crawled_html')}")
        print(f"Pages output directory: {file_config.get('pages_output_dir', 'crawled_pages')}")
        print(f"Delete existing folders: {file_config.get('delete_existing_folders', False)}")
        
        # Validation
        errors = self.validate_config()
        if errors:
            print(f"\nConfiguration errors: {len(errors)}")
            for error in errors:
                print(f"  - {error}")
        else:
            print("\nConfiguration is valid")
        
        print("=" * 30)