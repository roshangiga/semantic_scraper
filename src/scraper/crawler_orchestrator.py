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

# Import rich console utilities
try:
    from ..console import (
        console, print_success, print_error, print_warning, 
        print_info, print_processing, print_panel, print_header,
        create_table, create_page_processing_tree, add_processing_step, print_processing_tree_final,
        stop_page_live,
    )
    RICH_AVAILABLE = True
except ImportError:
    # Fallback to regular print if rich not available
    RICH_AVAILABLE = False
    def print_error(msg): print(f"❌ {msg}")
    def print_success(msg): print(f"✅ {msg}")
    def print_warning(msg): print(f"⚠️ {msg}")
    def print_info(msg): print(f"ℹ️ {msg}")
    def print_processing(msg): print(f"🔄 {msg}")
    def print_header(msg): print(f"\n=== {msg} ===")
    def print_panel(title, content, style=None): print(f"\n{title}:\n{content}")
    def create_table(title=None): return None


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
        self.current_processing_tree = None  # Store current processing tree for callbacks
        self._seen_file_hashes = {}  # Track file hashes for duplicate detection
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
                # Use external processor - separate process avoids access violations
                from ..semantic.external_processor import ExternalSemanticProcessor
                
                provider = chunking_config.get('provider', 'gemini').lower()
                self.semantic_processor = ExternalSemanticProcessor(self.config)
                
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
    
    def _check_semantic_progress(self, processing_tree = None):
        """Check for completed semantic tasks and update display."""
        if self.is_contextual_chunking_enabled():
            newly_completed = self.semantic_processor.check_completed_tasks()
            if newly_completed > 0:
                status = self.semantic_processor.get_status()
                if processing_tree is not None:
                    try:
                        from ..console import add_processing_step
                        add_processing_step(processing_tree, "semantic_progress_panel", 
                                          f"{status['completed']},{status['failed']},{status['total']},")
                    except ImportError:
                        print(f"   🧠 Semantic progress: {status['completed']}/{status['total']} completed")
    
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
                # Use RAG success panel if current processing_tree is available
                if self.current_processing_tree is not None:
                    try:
                        from ..console import add_processing_step
                        add_processing_step(self.current_processing_tree, "rag_success", f"Uploaded {chunks_uploaded} chunks")
                    except ImportError:
                        print(f"│  ├─ 📤 RAG: uploaded {chunks_uploaded} chunks")
                else:
                    print(f"│  ├─ 📤 RAG: uploaded {chunks_uploaded} chunks")
                
        except Exception as e:
            # Use RAG error panel if current processing_tree is available
            if self.current_processing_tree is not None:
                try:
                    from ..console import add_processing_step
                    add_processing_step(self.current_processing_tree, "rag_error", str(e))
                except ImportError:
                    print(f"❌ Streaming RAG upload failed for {semantic_output_path}: {e}")
            else:
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
        
        # Setup directories and show in table
        self._display_directory_setup()
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
        
        # Get domain names for display
        domain_names = [d['domain'] for d in domains]
        
        # Display crawl settings in table
        self._display_crawl_settings(crawl_config)
        
        async with WebCrawler(crawl_config, None) as crawler:
            # Set up semantic queue callback for checkpointing
            crawler.semantic_queue_callback = self.get_semantic_queue_for_checkpoint
            
            # If checkpoint exists, load it
            import os
            if os.path.exists('crawler_checkpoint.json'):
                # Load checkpoint and restore semantic queue
                visited_urls, crawl_queue, semantic_queue_data = crawler.load_checkpoint()
                # Set the loaded data on the crawler
                if visited_urls:
                    crawler.visited_urls = visited_urls
                if crawl_queue:
                    crawler.queue = crawl_queue
                if semantic_queue_data and self.is_contextual_chunking_enabled():
                    # Simple processor doesn't support checkpoint restore (simpler, safer approach)
                    print(f"ℹ️ Found {len(semantic_queue_data)} previous semantic tasks - will rescan unprocessed files")
                elif self.is_contextual_chunking_enabled():
                    # If no semantic queue data but we have checkpoint, scan for unprocessed files
                    self._scan_and_queue_unprocessed_semantic_files()
            
            # Process pages as they are crawled (streaming approach)
            page_count = 0
            try:
                async for crawl_result in crawler.crawl_all_streaming(domains):
                    page_count += 1
                    try:
                        # Create and display tree for page processing
                        current_domain = crawl_result.get('domain_config', {}).get('domain', 'unknown')
                        queue_size = crawl_result.get('queue_size', 0)
                        
                        if RICH_AVAILABLE:
                            # Use rich tree display following Rich patterns
                            processing_tree = create_page_processing_tree(page_count, queue_size, current_domain, crawl_result['url'])
                            self.current_processing_tree = processing_tree  # Store for callbacks
                        else:
                            # Fallback to old style
                            processing_tree = None
                            print_immediate(f"\n┌─ 🗺️  Processing Page {page_count} of ∞")
                            print_immediate(f"│  ┌─ 📊 Queue Status: {queue_size} URLs remaining")
                            print_immediate(f"│  ├─ 🌐 Domain: {current_domain}")
                            print_immediate(f"│  └─   URL: {crawl_result['url']}")
                            print_immediate(f"│")
                        
                        await self._process_single_page(crawl_result, output_formats, processing_tree)
                        results['processed_pages'].append(crawl_result['url'])
                        
                        # Check if it was a PDF redirect
                        is_pdf = 'pdf_redirect' in crawl_result
                        # Check for completed semantic tasks and display them
                        self._display_semantic_results(processing_tree)
                        
                        if RICH_AVAILABLE:
                            # Print the complete tree at the end
                            print_processing_tree_final(processing_tree, page_count, current_domain)
                        else:
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
            finally:
                # Ensure the live panel is stopped and the terminal is restored
                if RICH_AVAILABLE:
                    try:
                        stop_page_live()
                    except Exception:
                        pass
            
            # Save failed URLs if any
            if hasattr(crawler, 'failed_urls') and crawler.failed_urls:
                crawler.save_failed_urls()
                results['failed_urls'] = len(crawler.failed_urls)
            
            if page_count == 0:
                print("⚠️  No pages were successfully crawled")
        
        # Get final statistics
        results['stats'] = self.file_manager.get_output_stats()
        
        # Clean up only blank files if enabled (duplicates handled per-file during processing)
        markdown_config = self.config.get('markdown_processing', {})
        if markdown_config.get('remove_blank_files', False):
            # Call with skip_duplicates=True since we handle duplicates per-file now
            cleanup_stats = self.file_manager.remove_duplicate_and_blank_files(skip_duplicates=True)
            if cleanup_stats['blank_files_removed'] > 0:
                results['cleanup_stats'] = cleanup_stats
                print(f"   🗑️ Removed {cleanup_stats['blank_files_removed']} blank files")
        
        # Wait for all semantic chunking tasks to complete
        if self.is_contextual_chunking_enabled():
            print("   🔄 Waiting for semantic chunking to complete...")
            # Process all remaining tasks sequentially (no threading)
            results = self.semantic_processor.process_all_remaining()
            print(f"   ✅ Semantic chunking completed: {results['completed']} succeeded, {results['failed']} failed")
            
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
        print_success("Crawling completed! 🎉")
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
    
    def get_semantic_queue_for_checkpoint(self):
        """Get current semantic queue state for checkpoint saving."""
        if not self.semantic_processor:
            return []
        
        # Get pending tasks from the new tracking system
        pending_tasks = []
        
        # Simple processor uses direct task list (no complex tracking)
        if hasattr(self.semantic_processor, 'pending_tasks'):
            for task in self.semantic_processor.pending_tasks:
                pending_tasks.append({
                    'markdown_file_path': task.markdown_file_path,
                    'semantic_output_path': task.semantic_output_path,
                    'source_url': task.source_url
                })
            
            print(f"📊 Semantic checkpoint: {len(pending_tasks)} pending tasks saved")
        
        return pending_tasks
    
    
    def _scan_and_queue_unprocessed_semantic_files(self):
        """Scan for markdown files that need semantic processing during resume."""
        if not self.semantic_processor:
            return
            
        import os
        from pathlib import Path
        
        # Get current output directories
        docling_dir = self.file_manager.current_pages_dir
        semantic_dir = self.file_manager.current_semantic_dir
        
        if not os.path.exists(docling_dir):
            return
            
        # Find all markdown files
        markdown_files = []
        for root, dirs, files in os.walk(docling_dir):
            for file in files:
                if file.endswith('.md'):
                    markdown_files.append(os.path.join(root, file))
        
        # Check which ones don't have corresponding semantic files
        unprocessed_count = 0
        for md_file in markdown_files:
            # Generate timestamped semantic output path using file manager  
            import os
            semantic_filename = os.path.basename(md_file).replace('.md', '.json')
            domain_folder = os.path.dirname(md_file).split(os.sep)[-1]
            semantic_domain_dir = os.path.join(self.file_manager.current_semantic_dir, domain_folder)
            os.makedirs(semantic_domain_dir, exist_ok=True)
            semantic_output_path = os.path.join(semantic_domain_dir, semantic_filename)
            
            # Check if semantic file already exists
            if not os.path.exists(semantic_output_path):
                # Read URL from markdown file
                try:
                    with open(md_file, 'r', encoding='utf-8') as f:
                        first_line = f.readline().strip()
                        if first_line.startswith('# Source:'):
                            source_url = first_line.replace('# Source:', '').strip()
                            
                            # Add to semantic queue
                            self.semantic_processor.add_task(md_file, semantic_output_path, source_url)
                            unprocessed_count += 1
                        
                except Exception as e:
                    print(f"⚠️ Could not process {md_file}: {e}")
        
        if unprocessed_count > 0:
            print(f"📥 Resume: Queued {unprocessed_count} unprocessed markdown files for semantic chunking")
    
    def _check_and_handle_duplicate(self, file_path: str, processing_tree = None) -> bool:
        """
        Check if a file is a duplicate and remove it if so.
        
        Args:
            file_path: Path to the file to check
            processing_tree: Optional processing tree for status updates
            
        Returns:
            True if file should be processed further, False if it's a duplicate
        """
        # Only check duplicates if enabled in config
        markdown_config = self.config.get('markdown_processing', {})
        if not markdown_config.get('remove_duplicate_files', False):
            return True
            
        try:
            import os
            import hashlib
            
            # Calculate hash of this file's content (excluding Source: line)
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Skip the first line if it's a Source: line
            if lines and lines[0].startswith('# Source:'):
                content_to_hash = ''.join(lines[1:])
            else:
                content_to_hash = ''.join(lines)
            
            file_hash = hashlib.md5(content_to_hash.encode('utf-8')).hexdigest()
            
            # Check if we've seen this hash before
            if not hasattr(self, '_seen_file_hashes'):
                self._seen_file_hashes = {}
            
            if file_hash in self._seen_file_hashes:
                # This is a duplicate - remove it
                original_file = self._seen_file_hashes[file_hash]
                try:
                    os.remove(file_path)
                    if processing_tree is not None:
                        try:
                            from ..console import add_processing_step
                            add_processing_step(processing_tree, "warning", f"Removed duplicate file (matches {os.path.basename(original_file)})")
                        except ImportError:
                            print(f"│  ├─ ⚠️ Removed duplicate file (matches {os.path.basename(original_file)})")
                    else:
                        print(f"   ⚠️ Removed duplicate file: {os.path.basename(file_path)} (matches {os.path.basename(original_file)})")
                    return False  # Don't process semantically
                except Exception as e:
                    print(f"   ❌ Error removing duplicate file {file_path}: {e}")
                    return True  # Process anyway if removal failed
            else:
                # Store this hash for future comparisons
                self._seen_file_hashes[file_hash] = file_path
                return True  # Process semantically
                
        except Exception as e:
            print(f"   ❌ Error checking for duplicate {file_path}: {e}")
            return True  # Process anyway if check failed

    def _display_semantic_results(self, processing_tree = None):
        """Check for and display completed semantic chunking results and progress."""
        if not self.semantic_processor:
            return
            
        # Display semantic queue progress if there are tasks
        status = self.semantic_processor.get_status()
        if status['total'] > 0:
            # Show progress panel periodically (every 5 completed/failed tasks or when starting)
            completed = status['completed']
            failed = status['failed']
            total = status['total']
            
            # Track last displayed progress to avoid spam
            if not hasattr(self, '_last_semantic_progress'):
                self._last_semantic_progress = 0
                self._semantic_progress_shown = False
            
            current_processed = completed + failed
            
            # Show progress panel every time there's a change
            should_show_progress = (
                current_processed != self._last_semantic_progress or
                (not self._semantic_progress_shown and status.get('pending', 0) > 0)
            )
            
            if should_show_progress and processing_tree is not None:
                try:
                    from ..console import add_processing_step
                    # Try to get the filename of currently processing task
                    current_filename = ""
                    # Show semantic processing status
                    current_filename = "Processing semantic chunking..."
                    
                    progress_message = f"{completed},{failed},{total},{current_filename}"
                    add_processing_step(processing_tree, "semantic_progress_panel", progress_message)
                    self._last_semantic_progress = current_processed
                    self._semantic_progress_shown = True
                except ImportError:
                    pass  # Fallback handled below
            
        # Semantic task completion is now handled in the worker process
    
    def _display_directory_setup(self) -> None:
        """Display directory setup information in a table."""
        if not RICH_AVAILABLE:
            return
            
        # Merged into startup config table; keep this as no-op to avoid duplicate UI
        return
    
    def _display_crawl_settings(self, crawl_config: Dict[str, Any]) -> None:
        """Display crawl configuration settings in a table."""
        if not RICH_AVAILABLE:
            # Fallback to old style
            print(f"📊 Max pages per domain: {crawl_config.get('max_pages', 100)}")
            print(f"⏱️  Delay before HTML capture: {crawl_config.get('delay_before_return_html', 2.5)}s")
            print(f"🔄 Bypass cache: {crawl_config.get('bypass_cache', True)}")
            print(f"🚫 Exclude section URLs (#): {crawl_config.get('exclude_section_urls', True)}")
            print(f"🔁 Max retries per page: {crawl_config.get('max_retries', 3)}")
            print(f"⏰ Retry delay: {crawl_config.get('retry_delay', 5)}s")
            print("-" * 60)
            return
        
        # Rich UI is merged in Configuration Summary; suppress duplicate output
        return
    
    def _display_startup_config(self, output_formats: List[str]) -> None:
        """Display configuration information before starting crawl."""
        if not RICH_AVAILABLE:
            # Suppress verbose banner in non-rich mode
            return
            # Suppress standalone header to keep UI concise
        
        # Build a single consolidated table
        file_config = self.config.get('crawler', {}).get('file_manager', {})
        domains = self.get_domains_config()
        markdown_processing = self.config.get('markdown_processing', {})
        rag_config = self.config.get('rag_upload', {})
        crawl_config = self.get_crawl4ai_config()
        
        table = create_table("Configuration Summary")
        table.add_column("Item", style="cyan", width=28, no_wrap=True, overflow="ellipsis")
        table.add_column("Value", style="bright_blue", width=40, no_wrap=True, overflow="ellipsis")
        table.add_column("Details", style="dim white", no_wrap=True, overflow="ellipsis")
        
        # Basic settings
        table.add_row("📄 Output formats", ', '.join(output_formats), "")
        table.add_row("📂 Use domain subfolders", str(file_config.get('use_domain_subfolders', True)), "")
        table.add_row("🗑️ Delete existing folders", str(file_config.get('delete_existing_folders', False)), "")
        
        # Directory setup (merged)
        from datetime import datetime as _dt
        _ts = _dt.now().strftime("%Y%m%d_%H%M%S")
        html_dir = file_config.get('html_output_dir', 'crawled_html')
        pages_dir = file_config.get('pages_output_dir', 'crawled_docling')
        pdf_dir = file_config.get('pdf_output_dir', 'crawled_pdf')
        semantic_dir = file_config.get('semantic_output_dir', 'crawled_semantic')
        use_subfolders = file_config.get('use_domain_subfolders', True)
        ts_suffix = f"\\{_ts}" if use_subfolders else ""
        table.add_row("📁 HTML directory", f"{html_dir}{ts_suffix}", "✅ Ready")
        table.add_row("📁 Pages directory", f"{pages_dir}{ts_suffix}", "✅ Ready")
        table.add_row("📑 PDF directory", f"{pdf_dir}{ts_suffix}", "✅ Ready")
        if self.is_contextual_chunking_enabled():
            table.add_row("🧠 Semantic directory", f"{semantic_dir}{ts_suffix}", "✅ Ready")
        
        # Configured domains (merged)
        if domains:
            table.add_row("🌐 Domains configured", str(len(domains)), "")
            for domain in domains:
                features = []
                if domain.get('js_code'):
                    features.append("⚡ JavaScript")
                if domain.get('wait_for'):
                    features.append("⏳ Wait condition")
                if domain.get('html_classes_to_only_include'):
                    features.append("🎯 Filtered content")
                table.add_row(
                    f"  └ {domain['domain']}",
                    f"{len(domain.get('start_urls', []))} URLs",
                    ", ".join(features) if features else "Standard"
                )
        
        # Requested options in single table
        table.add_row("🔄 Remove duplicate lines", "True", "always enabled")
        table.add_row("📑 Remove duplicate files", str(markdown_processing.get('remove_duplicate_files', False)), "")
        table.add_row("📄 Remove blank files", str(markdown_processing.get('remove_blank_files', False)), "")
        
        # RAG upload status
        if self.rag_uploader and self.rag_uploader.is_enabled():
            mode = "Real-time" if rag_config.get('streaming', True) else "Batch"
            client = rag_config.get('client', 'ragflow').upper()
            table.add_row("📤 RAG Upload", "Enabled", f"{client} • {mode}")
        else:
            table.add_row("📤 RAG Upload", "Disabled", "")

        # Crawl configuration (merged)
        table.add_row("⚙  Crawl: max pages per domain", str(crawl_config.get('max_pages', 100)), "")
        table.add_row("⚙  Crawl: HTML capture delay", f"{crawl_config.get('delay_before_return_html', 2.5)}s", "JS settle time")
        table.add_row("⚙  Crawl: bypass cache", str(crawl_config.get('bypass_cache', True)), "")
        table.add_row("⚙  Crawl: exclude sections (#)", str(crawl_config.get('exclude_section_urls', True)), "")
        table.add_row("⚙  Crawl: max retries", str(crawl_config.get('max_retries', 3)), "")
        table.add_row("⚙  Crawl: retry delay", f"{crawl_config.get('retry_delay', 5)}s", "")
        
        console.print(table)
    
    async def _process_single_page(self, crawl_result: Dict[str, Any], output_formats: List[str], processing_tree = None) -> None:
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
            if processing_tree is not None:
                add_processing_step(processing_tree, "info", "📥 Redirected to PDF document")
            else:
                print_immediate(f"│  ├─ 📥 Redirected to PDF document")
            
            # Process the PDF if enabled
            if self.config.get('link_processing', {}).get('process_pdf_links', False):
                try:
                    if processing_tree is not None:
                        add_processing_step(processing_tree, "processing", "📥 Downloading and processing PDF...")
                    else:
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
                                
                                # Display saved message in the processing tree
                                if processing_tree is not None:
                                    add_processing_step(processing_tree, "success", "✔️ Saved PDF content")
                                
                                # Start semantic chunking for PDF markdown content
                                if (self.is_contextual_chunking_enabled() and 
                                    format.lower() in ['markdown', 'md']):
                                    try:
                                        # Generate timestamped semantic output path using file manager
                                        import os
                                        semantic_filename = os.path.basename(saved_path).replace('.md', '.json')
                                        domain_folder = os.path.dirname(saved_path).split(os.sep)[-1]
                                        semantic_domain_dir = os.path.join(self.file_manager.current_semantic_dir, domain_folder)
                                        os.makedirs(semantic_domain_dir, exist_ok=True)
                                        semantic_output_path = os.path.join(semantic_domain_dir, semantic_filename)
                                        self.semantic_processor.add_task(saved_path, semantic_output_path, pdf_url)
                                    except Exception as e:
                                        print(f"   ⚠️ Error launching semantic chunking for PDF {pdf_url}: {e}")
                                        
                            if processing_tree is not None:
                                add_processing_step(processing_tree, "success", "✅ PDF processed successfully")
                            else:
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
        self.file_manager.save_html(url, processed_result['processed_html'], processing_tree)
        
        # Convert to requested formats
        for output_format in output_formats:
            if output_format.lower() == 'html':
                # Save processed HTML
                self.file_manager.save_content(
                    url, 
                    processed_result['processed_html'], 
                    'html',
                    None,  # no conversion time for HTML
                    processing_tree
                )
            else:
                # Convert using Docling
                converted_content, conversion_time, fallback_message = self.document_converter.convert_with_cleanup(
                    processed_result['temp_file_path'], 
                    output_format,
                    url
                )
                
                # Add fallback warning to console tree if fallback was used
                if fallback_message:
                    if RICH_AVAILABLE:
                        add_processing_step(processing_tree, "fallback_panel", fallback_message)
                    else:
                        print(f"│  ├─ ⚠️ {fallback_message}")  # Fallback if console not available
                
                # Save converted content
                saved_path = self.file_manager.save_content(url, converted_content, output_format, conversion_time, processing_tree)
                
                # Check if this file is a duplicate before processing semantically
                should_process_semantically = True
                if output_format.lower() in ['markdown', 'md']:
                    should_process_semantically = self._check_and_handle_duplicate(saved_path, processing_tree)
                
                # Start semantic chunking in separate process if enabled and format is markdown
                if (self.is_contextual_chunking_enabled() and 
                    output_format.lower() in ['markdown', 'md'] and 
                    should_process_semantically):
                    try:
                        # Generate timestamped semantic output path using file manager
                        import os
                        semantic_filename = os.path.basename(saved_path).replace('.md', '.json')
                        domain_folder = os.path.dirname(saved_path).split(os.sep)[-1]
                        semantic_domain_dir = os.path.join(self.file_manager.current_semantic_dir, domain_folder)
                        os.makedirs(semantic_domain_dir, exist_ok=True)
                        semantic_output_path = os.path.join(semantic_domain_dir, semantic_filename)
                        self.semantic_processor.add_task(saved_path, semantic_output_path, url)
                        # Increment queue counter and show status
                        self.semantic_queue_count += 1
                        # Get actual queue size from semantic processor for accurate display
                        total_pending = self.semantic_processor.get_queue_size()
                        filename = Path(saved_path).name
                        if processing_tree is not None:
                            # Show progress panel after adding task (includes queued message in title)
                            try:
                                status = self.semantic_processor.get_status()
                                progress_message = f"{status['completed']},{status['failed']},{status['total']},{filename}"
                                add_processing_step(processing_tree, "semantic_progress_panel", progress_message)
                            except:
                                pass
                        else:
                            print_immediate(f"│  ├─ 🧠 Queued for semantic processing")
                    except Exception as e:
                        print_immediate(f"   ❌ Semantic chunking error for {url}: {e}")
        
        # Process PDF URLs if any were found
        if 'pdf_urls' in crawl_result and crawl_result['pdf_urls']:
            await self._process_pdf_urls(crawl_result['pdf_urls'], output_formats)
        
        # Check for completed semantic tasks
        self._check_semantic_progress(processing_tree)
    
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
                                # Generate timestamped semantic output path using file manager
                                import os
                                semantic_filename = os.path.basename(saved_path).replace('.md', '.json')
                                domain_folder = os.path.dirname(saved_path).split(os.sep)[-1]
                                semantic_domain_dir = os.path.join(self.file_manager.current_semantic_dir, domain_folder)
                                os.makedirs(semantic_domain_dir, exist_ok=True)
                                semantic_output_path = os.path.join(semantic_domain_dir, semantic_filename)
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
        if not RICH_AVAILABLE:
            # Fallback to old style
            print("\n=== Configuration Summary ===")
            domains = self.get_domains_config()
            print(f"Configured domains: {len(domains)}")
            print("=" * 30)
            return
            
        print_header("⚙️ Configuration Summary")
        
        # Create summary table
        table = create_table("Configuration Overview")
        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="green")
        
        # Domains
        domains = self.get_domains_config()
        output_formats = self.get_output_formats()
        file_config = self.config.get('crawler', {}).get('file_manager', {})
        
        table.add_row("📁 Domains", str(len(domains)))
        table.add_row("📄 Output formats", ', '.join(output_formats))
        table.add_row("📂 HTML directory", file_config.get('html_output_dir', 'crawled_html'))
        table.add_row("📁 Pages directory", file_config.get('pages_output_dir', 'crawled_docling'))
        table.add_row("🗑️ Delete folders", str(file_config.get('delete_existing_folders', False)))
        
        # Contextual chunking
        chunking_config = self.config.get('contextual_chunking', {})
        is_enabled = chunking_config.get('enabled', False)
        table.add_row("🧠 Contextual chunking", "Enabled" if is_enabled else "Disabled")
        if is_enabled:
            table.add_row("🤖 Model", chunking_config.get('gemini_model', 'gemini-1.5-pro'))
        
        console.print(table)
        
        # Domain details
        if domains:
            domain_info = ""
            for domain in domains:
                domain_info += f"• {domain['domain']} ({len(domain.get('start_urls', []))} URLs)\n"
            print_panel("🌐 Configured Domains", domain_info.rstrip(), "blue")
        
        # Validation
        errors = self.validate_config()
        if errors:
            error_info = "\n".join(f"• {error}" for error in errors)
            print_panel("❌ Configuration Errors", error_info, "red")
        else:
            print_success("Configuration is valid")