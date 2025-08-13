#!/usr/bin/env python3
"""
Report generator for creating comprehensive crawl reports.
"""

import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List
import platform


class CrawlReportGenerator:
    """Generates comprehensive crawl reports."""
    
    def __init__(self, report_dir: str = "crawled_report"):
        """
        Initialize the report generator.
        
        Args:
            report_dir: Directory to save reports
        """
        self.report_dir = Path(report_dir)
        
    def generate_report(
        self,
        crawl_results: Dict[str, Any],
        config: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> str:
        """
        Generate a comprehensive crawl report.
        
        Args:
            crawl_results: Results from the crawler
            config: Configuration used
            start_time: When crawling started
            end_time: When crawling ended
            
        Returns:
            Path to the generated report file
        """
        # Ensure report directory exists
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate timestamp for unique reports
        timestamp = start_time.strftime('%Y%m%d_%H%M%S')
        
        # Generate different report formats
        report_files = []
        
        # 1. Main summary report (Markdown)
        summary_file = self._generate_summary_report(
            crawl_results, config, start_time, end_time, timestamp
        )
        report_files.append(summary_file)
        
        # 2. Detailed URLs report (JSON + Text)
        urls_file = self._generate_urls_report(crawl_results, timestamp)
        report_files.append(urls_file)
        
        # 3. Failed URLs report (if any failures)
        if crawl_results.get('errors') or crawl_results.get('failed_urls', 0) > 0:
            failed_file = self._generate_failed_urls_report(crawl_results, timestamp)
            report_files.append(failed_file)
        
        # 4. Configuration snapshot
        config_file = self._save_config_snapshot(config, timestamp)
        report_files.append(config_file)
        
        print(f"📊 Generated crawl reports:")
        for report_file in report_files:
            print(f"   📄 {report_file}")
        
        return str(summary_file)
    
    def _generate_summary_report(
        self,
        crawl_results: Dict[str, Any],
        config: Dict[str, Any],
        start_time: datetime,
        end_time: datetime,
        timestamp: str
    ) -> Path:
        """Generate the main summary report in Markdown format."""
        
        duration = end_time - start_time
        processed_pages = crawl_results.get('processed_pages', [])
        errors = crawl_results.get('errors', [])
        stats = crawl_results.get('stats', {})
        
        # Get system info
        system_info = {
            'platform': platform.system(),
            'platform_version': platform.version(),
            'python_version': platform.python_version(),
            'machine': platform.machine()
        }
        
        report_content = f"""# Crawl Report

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Executive Summary

- **Start Time:** {start_time.strftime('%Y-%m-%d %H:%M:%S')}
- **End Time:** {end_time.strftime('%Y-%m-%d %H:%M:%S')}
- **Total Duration:** {self._format_duration(duration)}
- **Pages Processed:** {len(processed_pages)}
- **Errors:** {len(errors)}
- **Success Rate:** {self._calculate_success_rate(processed_pages, errors):.1f}%

## Performance Metrics

- **Average Time per Page:** {self._calculate_avg_time_per_page(duration, processed_pages):.2f} seconds
- **Pages per Minute:** {self._calculate_pages_per_minute(duration, processed_pages):.1f}

## File Generation Statistics

"""
        
        if stats:
            report_content += f"""- **HTML Files:** {stats.get('html_files', 0)}
- **Markdown Files:** {stats.get('markdown_files', 0)}
- **DOCX Files:** {stats.get('docx_files', 0)}
- **Total Files:** {stats.get('total_files', 0)}

"""
        
        # Add domain breakdown
        domains_config = config.get('domains', [])
        if domains_config:
            report_content += f"""## Domains Crawled

"""
            for i, domain in enumerate(domains_config, 1):
                domain_name = domain.get('domain', 'Unknown')
                start_urls = domain.get('start_urls', [])
                report_content += f"""{i}. **{domain_name}**
   - Start URLs: {len(start_urls)}
   - JavaScript: {'Yes' if domain.get('js_code') else 'No'}
   - Wait Condition: {'Yes' if domain.get('wait_for') else 'No'}

"""
        
        # Add configuration summary
        report_content += f"""## Configuration Summary

- **Output Formats:** {', '.join(config.get('crawler', {}).get('output_formats', []))}
- **Max Pages:** {config.get('crawler', {}).get('crawl4ai', {}).get('max_pages', 'Unlimited')}
- **Delete Existing Folders:** {config.get('crawler', {}).get('file_manager', {}).get('delete_existing_folders', False)}
- **Domain Subfolders:** {config.get('crawler', {}).get('file_manager', {}).get('use_domain_subfolders', True)}

"""
        
        # Add contextual chunking info
        chunking_config = config.get('contextual_chunking', {})
        if chunking_config.get('enabled', False):
            report_content += f"""### Contextual Chunking
- **Enabled:** Yes
- **Model:** {chunking_config.get('gemini_model', 'gemini-1.5-pro')}

"""
        
        # Add system information
        report_content += f"""## System Information

- **Platform:** {system_info['platform']} {system_info['platform_version']}
- **Python Version:** {system_info['python_version']}
- **Architecture:** {system_info['machine']}

## Error Summary

"""
        
        if errors:
            report_content += f"""Total Errors: {len(errors)}

### Error Types

"""
            # Group errors by type
            error_types = {}
            for error in errors:
                error_msg = str(error.get('error', 'Unknown error'))
                error_type = error_msg.split(':')[0] if ':' in error_msg else 'General Error'
                error_types[error_type] = error_types.get(error_type, 0) + 1
            
            for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
                report_content += f"- **{error_type}:** {count} occurrences\n"
            
            report_content += f"""
### Recent Errors (Last 10)

"""
            for error in errors[-10:]:
                url = error.get('url', 'Unknown URL')
                error_msg = error.get('error', 'Unknown error')
                report_content += f"- `{url}`: {error_msg}\n"
        else:
            report_content += "No errors occurred during crawling. ✅\n"
        
        # Add footer
        report_content += f"""

## Files Generated

This report was generated alongside the following output directories:
- **HTML Files:** `crawled_html/`
- **Markdown Files:** `crawled_docling/`
- **PDF Files:** `crawled_pdf/`
- **Semantic Chunks:** `crawled_semantic/`
- **Reports:** `crawled_report/`

---

*Report generated by Crawl4AI Docling Crawler v1.0*
"""
        
        # Save report
        report_file = self.report_dir / f"crawl_summary_{timestamp}.md"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        return report_file
    
    def _generate_urls_report(self, crawl_results: Dict[str, Any], timestamp: str) -> Path:
        """Generate detailed URLs report."""
        processed_pages = crawl_results.get('processed_pages', [])
        
        # Create detailed URLs report
        urls_content = f"# Crawled URLs Report\n\n"
        urls_content += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        urls_content += f"Total URLs Processed: {len(processed_pages)}\n\n"
        urls_content += "## Successfully Processed URLs\n\n"
        
        for i, url in enumerate(processed_pages, 1):
            urls_content += f"{i}. {url}\n"
        
        # Save URLs report
        urls_file = self.report_dir / f"crawled_urls_{timestamp}.md"
        with open(urls_file, 'w', encoding='utf-8') as f:
            f.write(urls_content)
        
        # Also save as JSON for programmatic access
        urls_data = {
            'timestamp': datetime.now().isoformat(),
            'total_urls': len(processed_pages),
            'processed_urls': processed_pages
        }
        
        json_file = self.report_dir / f"crawled_urls_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(urls_data, f, indent=2)
        
        return urls_file
    
    def _generate_failed_urls_report(self, crawl_results: Dict[str, Any], timestamp: str) -> Path:
        """Generate failed URLs report."""
        errors = crawl_results.get('errors', [])
        failed_count = crawl_results.get('failed_urls', 0)
        
        failed_content = f"# Failed URLs Report\n\n"
        failed_content += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        failed_content += f"Total Failed URLs: {failed_count}\n"
        failed_content += f"Processing Errors: {len(errors)}\n\n"
        
        if errors:
            failed_content += "## Processing Errors\n\n"
            for i, error in enumerate(errors, 1):
                url = error.get('url', 'Unknown URL')
                error_msg = error.get('error', 'Unknown error')
                failed_content += f"{i}. **URL:** {url}\n   **Error:** {error_msg}\n\n"
        
        # Save failed URLs report
        failed_file = self.report_dir / f"failed_urls_{timestamp}.md"
        with open(failed_file, 'w', encoding='utf-8') as f:
            f.write(failed_content)
        
        return failed_file
    
    def _save_config_snapshot(self, config: Dict[str, Any], timestamp: str) -> Path:
        """Save configuration snapshot."""
        config_file = self.report_dir / f"config_snapshot_{timestamp}.json"
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, default=str)
        
        return config_file
    
    def _format_duration(self, duration: timedelta) -> str:
        """Format duration in a human-readable way."""
        total_seconds = int(duration.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    def _calculate_success_rate(self, processed_pages: List[str], errors: List[Dict]) -> float:
        """Calculate success rate percentage."""
        total_attempts = len(processed_pages) + len(errors)
        if total_attempts == 0:
            return 100.0
        return (len(processed_pages) / total_attempts) * 100
    
    def _calculate_avg_time_per_page(self, duration: timedelta, processed_pages: List[str]) -> float:
        """Calculate average time per page."""
        if not processed_pages:
            return 0.0
        return duration.total_seconds() / len(processed_pages)
    
    def _calculate_pages_per_minute(self, duration: timedelta, processed_pages: List[str]) -> float:
        """Calculate pages per minute."""
        if duration.total_seconds() == 0:
            return 0.0
        return (len(processed_pages) / duration.total_seconds()) * 60