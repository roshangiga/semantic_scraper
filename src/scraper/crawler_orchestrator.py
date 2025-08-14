"""
Crawler orchestrator module for coordinating all crawling operations.
"""

import asyncio
import yaml
import os
import sys
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path

from .web_crawler import WebCrawler
from .html_processor import HTMLProcessor
from .document_converter import DocumentConverter
from .file_manager import FileManager
from .pdf_processor import PDFProcessor
from .report_generator import CrawlReportGenerator


def print_immediate(*args, **kwargs):
    """Print with immediate flush to ensure real-time output."""
    print(*args, **kwargs)
    sys.stdout.flush()


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
        
        # Initialize semantic processor for separate process handling
        self.semantic_processor = None
        self._init_semantic_processor()
        self.semantic_queue_count = 0
        
        # Initialize report generator
        self.report_generator = CrawlReportGenerator(
            self.config.get('crawler', {}).get('file_manager', {}).get('report_output_dir', 'crawled_report')
        )
        
        # Initialize RAG uploader
        self.rag_uploader = None
        self._init_rag_uploader()
        
        # Set up streaming RAG upload if enabled
        if (self.rag_uploader and self.rag_uploader.is_enabled() and 
            self.rag_uploader.streaming and self.semantic_processor):
            self.semantic_processor.set_completion_callback(self._stream_to_rag)
            print("🔗 Streaming RAG callback configured")
        
        # Initialize progress formatter
        
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
    
    def _init_semantic_processor(self):
        """Initialize semantic processor for sequential processing."""
        chunking_config = self.config.get('contextual_chunking', {})
        if chunking_config.get('enabled', False):
            try:
                from ..semantic.sequential_processor import SequentialSemanticProcessor
                
                provider = chunking_config.get('provider', 'gemini').lower()
                self.semantic_processor = SequentialSemanticProcessor(self.config)
                
                # Get the actual model name being used  
                if provider == 'openai':
                    model_name = chunking_config.get('openai_model', 'gpt-4o-mini')
                    status_msg = f"Initialized semantic processor ({provider.upper()}: {model_name})"
                else:
                    model_name = chunking_config.get('gemini_model', 'gemini-2.5-flash')
                    status_msg = f"Initialized semantic processor ({provider.capitalize()}: {model_name})"
                
                print(f"🧠 {status_msg}")  # This runs before progress formatter is created
                
            except ImportError as e:
                provider = chunking_config.get('provider', 'gemini').lower()
                if provider == 'openai':
                    print(f"⚠️  Failed to import semantic processor: {e}")
                    print("    Install openai: pip install openai")
                else:
                    print(f"⚠️  Failed to import semantic processor: {e}")
                    print("    Install google-generativeai: pip install google-generativeai")
            except Exception as e:
                print(f"⚠️  Failed to initialize semantic processor: {e}")
                self.semantic_processor = None
    
    def _init_rag_uploader(self):
        """Initialize RAG uploader if configured."""
        rag_config = self.config.get('rag_upload', {})
        if rag_config.get('enabled', False):
            try:
                from ..rag_clients.rag_uploader import RAGUploader
                self.rag_uploader = RAGUploader(rag_config)
                if self.rag_uploader.is_enabled():
                    client_name = rag_config.get('client', 'ragflow')
                    print(f"📤 RAG upload enabled: {client_name}")
                else:
                    self.rag_uploader = None
            except Exception as e:
                print(f"⚠️  Failed to initialize RAG uploader: {e}")
                self.rag_uploader = None
    
    def _stream_to_rag(self, semantic_output_path: str):
        """Callback function to stream completed semantic chunks to RAG."""
        
        if not (self.rag_uploader and self.rag_uploader.is_enabled() and self.rag_uploader.streaming):
            print(f"⚠️ RAG uploader not ready for streaming")
            return
            
        try:
            # Wait a moment for file to be fully written
            import time
            time.sleep(0.1)
            
            # Check if file exists and is a JSON file
            if not os.path.exists(semantic_output_path):
                print(f"⚠️ Semantic file not found for streaming: {semantic_output_path}")
                return
            
            if not semantic_output_path.endswith('.json'):
                print(f"⚠️ Expected JSON file for streaming, got: {semantic_output_path}")
                return
            
            # Check file size to ensure it's not empty
            file_size = os.path.getsize(semantic_output_path)
            if file_size == 0:
                print(f"⚠️ Semantic file is empty: {semantic_output_path}")
                return
            
            chunks_uploaded = self.rag_uploader.upload_single_file_streaming(semantic_output_path)
            
            if chunks_uploaded > 0:
                print(f"│  ├─ 📤 RAG: uploaded {chunks_uploaded} chunks")
                
        except Exception as e:
            print(f"❌ Streaming RAG upload failed for {semantic_output_path}: {e}")
            import traceback
            traceback.print_exc()
    
    def is_contextual_chunking_enabled(self) -> bool:
        """Check if contextual chunking is enabled and available."""
        return (self.semantic_processor is not None and 
                self.config.get('contextual_chunking', {}).get('enabled', False))
    
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
        
        # Record start time
        start_time = datetime.now()
        
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
        
        # Initialize progress formatter
        max_pages = crawl_config.get('max_pages', 100)
        
        # Set progress formatter for file manager
        
        # Get domain names for display
        domain_names = [d['domain'] for d in domains]
        print(f"🚀 Starting crawl of {len(domain_names)} domains with max {max_pages} pages total")
        
        # After the progress bar is initialized, route messages via formatter to
        # ensure they render above the bar (avoid inline wrapping issues)
        print(f"📊 Max pages per domain: {crawl_config.get('max_pages', 100)}")
        print(f"⏱️  Delay before HTML capture: {crawl_config.get('delay_before_return_html', 2.5)}s")
        print(f"🔄 Bypass cache: {crawl_config.get('bypass_cache', True)}")
        print(f"🚫 Exclude section URLs (#): {crawl_config.get('exclude_section_urls', True)}")
        print(f"🔁 Max retries per page: {crawl_config.get('max_retries', 3)}")
        print(f"⏰ Retry delay: {crawl_config.get('retry_delay', 5)}s")
        print("-" * 60)
        
        async with WebCrawler(crawl_config, None) as crawler:
            # Process pages as they are crawled (streaming approach)
            page_count = 0
            try:
                async for crawl_result in crawler.crawl_all_streaming(domains):
                    page_count += 1
                    try:
                        # Start page processing with enhanced progress info
                        current_domain = crawl_result.get('domain_config', {}).get('domain', 'unknown')
                        queue_size = crawl_result.get('queue_size', 0)
                        print_immediate(f"\n┌─ 🗺️  Processing Page {page_count} of ∞")
                        print_immediate(f"│  ┌─ 📊 Queue Status: {queue_size} URLs remaining")
                        print_immediate(f"│  ├─ 🌐 Domain: {current_domain}")
                        print_immediate(f"│  └─   URL: {crawl_result['url']}")
                        print_immediate(f"│")
                        
                        await self._process_single_page(crawl_result, output_formats)
                        results['processed_pages'].append(crawl_result['url'])
                        
                        # Check if it was a PDF redirect
                        is_pdf = 'pdf_redirect' in crawl_result
                        # Check for completed semantic tasks and display them
                        self._display_semantic_results()
                        
                        print_immediate(f"│  └─ ✅  Page {page_count} complete")
                        print_immediate(f"└─ {'═' * 50}")  # Enhanced separator between pages
                    except Exception as e:
                        error_info = {
                            'url': crawl_result['url'],
                            'error': str(e)
                        }
                        results['errors'].append(error_info)
                        print_immediate(f"│  └─ ❌ Error: {str(e)[:80]}...")
                        print_immediate(f"└─ {'═' * 50}")
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
        
        # Wait for all semantic chunking tasks to complete and stop worker
        if self.is_contextual_chunking_enabled():
            print("   🔄 Finalizing semantic chunking...")
            self.semantic_processor.wait_and_stop()
            
            # Upload to RAG if enabled (batch mode only - streaming already happened)
            if (self.rag_uploader and self.rag_uploader.is_enabled() and 
                not self.rag_uploader.streaming):
                try:
                    # Get the semantic output directory with timestamp
                    semantic_dir = self.config.get('crawler', {}).get('file_manager', {}).get('semantic_output_dir', 'crawled_semantic')
                    # Find the latest timestamp directory
                    import os
                    if os.path.exists(semantic_dir):
                        # Get all timestamp directories
                        timestamp_dirs = [d for d in os.listdir(semantic_dir) 
                                        if os.path.isdir(os.path.join(semantic_dir, d))]
                        if timestamp_dirs:
                            # Sort and get the latest
                            timestamp_dirs.sort()
                            latest_timestamp = timestamp_dirs[-1]
                            semantic_timestamped_dir = os.path.join(semantic_dir, latest_timestamp)
                            
                            print("   📤 Starting batch RAG upload...")
                            chunks_uploaded = self.rag_uploader.upload_from_directory(semantic_timestamped_dir)
                            if chunks_uploaded > 0:
                                results['rag_chunks_uploaded'] = chunks_uploaded
                except Exception as e:
                    print(f"   ❌ Failed to upload to RAG system: {e}")
            elif (self.rag_uploader and self.rag_uploader.is_enabled() and 
                  self.rag_uploader.streaming):
                print("   🚀 RAG upload completed via real-time streaming")
        
        # Record end time
        end_time = datetime.now()
        
        # Generate comprehensive report
        try:
            report_file = self.report_generator.generate_report(
                results, self.config, start_time, end_time
            )
            print(f"\n📊 Comprehensive report generated: {report_file}")
        except Exception as e:
            print(f"\n⚠️ Failed to generate report: {e}")
        
        # Clean up checkpoint file on successful completion
        self._cleanup_checkpoint()
        
        # Show final summary using progress formatter
        # Show final summary
        print(f"\n🎉 Crawling completed!")
        print(f"✅ Successfully processed: {len(results['processed_pages'])} pages")
        if results['errors']:
            print(f"❌ Processing errors: {len(results['errors'])} pages")
        if results.get('failed_urls', 0) > 0:
            print(f"🔄 Failed after retries: {results['failed_urls']} pages (saved to failed_urls.txt)")
        if results.get('rag_chunks_uploaded', 0) > 0:
            print(f"📤 RAG chunks uploaded: {results['rag_chunks_uploaded']}")
        elif (self.rag_uploader and self.rag_uploader.is_enabled() and 
              self.rag_uploader.streaming):
            print(f"🚀 RAG upload completed via streaming")
            
        duration = end_time - start_time
        print(f"⏱️ Total time: {duration}")
        
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
    
    def _display_semantic_results(self):
        """Check for and display completed semantic chunking results."""
        if not self.semantic_processor:
            return
            
        # Check for newly completed tasks
        for task in self.semantic_processor.completed_tasks:
            if hasattr(task, 'success_info') and not hasattr(task, 'displayed'):
                info = task.success_info
                print_immediate(f"│  ├─ ✅  Semantic chunking completed")
                task.displayed = True  # Mark as displayed
        
        # Check for newly failed tasks
        for task in self.semantic_processor.failed_tasks:
            if hasattr(task, 'error_info') and not hasattr(task, 'displayed'):
                info = task.error_info
                print_immediate(f"│  ├─ ❌ Semantic chunking failed")
                if info['stderr']:
                    error_msg = info['stderr'][:100] + "..." if len(info['stderr']) > 100 else info['stderr']
                    print_immediate(f"│  └─ ⚠️  Error: {error_msg}")
                task.displayed = True  # Mark as displayed
    
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
        print(f"📁 Pages directory: {file_config.get('pages_output_dir', 'crawled_docling')}")
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
            print(f"   🔄 Remove duplicate lines: True (always enabled)")
            print(f"   📑 Remove duplicate files: {markdown_processing.get('remove_duplicate_files', False)}")
            print(f"   📄 Remove blank files: {markdown_processing.get('remove_blank_files', False)}")
        
        # RAG upload  
        if self.rag_uploader and self.rag_uploader.is_enabled():
            rag_config = self.config.get('rag_upload', {})
            print(f"\n📤 RAG upload: Enabled")
            print(f"   🔌 Client: {rag_config.get('client', 'ragflow').upper()}")
            print(f"   🏷️  Naming: timestamp_domain (e.g., 20250814_112841_devices.myt.mu)")
            streaming_mode = "Real-time" if rag_config.get('streaming', True) else "Batch"
            print(f"   🚀 Mode: {streaming_mode}")
        else:
            print(f"\n📤 RAG upload: Disabled")
        
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
            print_immediate(f"│  ├─ 📥 Redirected to PDF document")
            
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
                                saved_path = self.file_manager.save_pdf_content(
                                    pdf_url,
                                    pdf_result['filename'],
                                    content,
                                    format
                                )
                                
                                # Start semantic chunking for PDF markdown content
                                if (self.is_contextual_chunking_enabled() and 
                                    format.lower() in ['markdown', 'md']):
                                    try:
                                        semantic_output_path = self.semantic_processor.get_semantic_output_path(saved_path)
                                        self.semantic_processor.add_task(saved_path, semantic_output_path, pdf_url)
                                    except Exception as e:
                                        print(f"   ⚠️ Error launching semantic chunking for PDF {pdf_url}: {e}")
                                        
                            print_immediate(f"│  ├─ ✅ PDF processed successfully")
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
                saved_path = self.file_manager.save_content(url, converted_content, output_format)
                
                # Start semantic chunking in separate process if enabled and format is markdown
                if (self.is_contextual_chunking_enabled() and 
                    output_format.lower() in ['markdown', 'md']):
                    try:
                        semantic_output_path = self.semantic_processor.get_semantic_output_path(saved_path)
                        self.semantic_processor.add_task(saved_path, semantic_output_path, url)
                        # Increment queue counter and show status
                        self.semantic_queue_count += 1
                        filename = Path(saved_path).name
                        print_immediate(f"│  ├─ 🧠 Queued for semantic processing (Queue: {self.semantic_queue_count})")
                    except Exception as e:
                        print_immediate(f"   ❌ Semantic chunking error for {url}: {e}")
        
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
                        saved_path = self.file_manager.save_pdf_content(
                            pdf_url, 
                            result['filename'], 
                            content, 
                            format_name
                        )
                        
                        # Start semantic chunking for PDF markdown content
                        if (self.is_contextual_chunking_enabled() and 
                            format_name.lower() in ['markdown', 'md']):
                            try:
                                semantic_output_path = self.semantic_processor.get_semantic_output_path(saved_path)
                                self.semantic_processor.add_task(saved_path, semantic_output_path, pdf_url)
                            except Exception as e:
                                print(f"   ⚠️ Error launching semantic chunking for PDF {pdf_url}: {e}")
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
        print(f"Pages output directory: {file_config.get('pages_output_dir', 'crawled_docling')}")
        print(f"Report output directory: {file_config.get('report_output_dir', 'crawled_report')}")
        print(f"Delete existing folders: {file_config.get('delete_existing_folders', False)}")
        
        # Contextual chunking
        chunking_config = self.config.get('contextual_chunking', {})
        is_enabled = chunking_config.get('enabled', False)
        print(f"Contextual chunking: {'Enabled' if is_enabled else 'Disabled'}")
        if is_enabled:
            print(f"  Model: {chunking_config.get('gemini_model', 'gemini-1.5-pro')}")
            print(f"  Semantic output directory: {file_config.get('semantic_output_dir', 'crawled_semantic')}")
        
        # Validation
        errors = self.validate_config()
        if errors:
            print(f"\nConfiguration errors: {len(errors)}")
            for error in errors:
                print(f"  - {error}")
        else:
            print("\nConfiguration is valid")
        
        print("=" * 30)