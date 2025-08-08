#!/usr/bin/env python3
"""
Wrapper script for the web crawler that handles process crashes and restarts.
"""

import subprocess
import sys
import time
import os
import json
from pathlib import Path

class CrawlerWrapper:
    """Wrapper that manages crawler restarts on crashes."""
    
    def __init__(self):
        self.max_restarts = 5
        self.restart_delay = 10
        self.crash_count = 0
        self.problematic_urls = set()
        self.crash_urls_file = 'problematic_urls.txt'
        
    def load_progress(self):
        """Load progress from previous runs."""
        if os.path.exists('crawler_progress.json'):
            try:
                with open('crawler_progress.json', 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {'restarts': 0, 'last_crash_url': None, 'crash_urls': []}
        return {'restarts': 0, 'last_crash_url': None, 'crash_urls': []}
    
    def save_progress(self, restarts, last_url=None, crash_urls=None):
        """Save progress information."""
        progress = {
            'restarts': restarts,
            'last_crash_url': last_url,
            'crash_urls': crash_urls or [],
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        with open('crawler_progress.json', 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2)
    
    def run_crawler(self, args):
        """Run the crawler with restart capability."""
        progress = self.load_progress()
        restart_count = progress.get('restarts', 0)
        crash_urls = progress.get('crash_urls', [])
        
        print(f"🚀 Starting crawler wrapper (restart #{restart_count})")
        if restart_count > 0:
            print(f"   📊 Previous restarts: {restart_count}")
            if progress.get('last_crash_url'):
                print(f"   🔗 Last processed URL: {progress['last_crash_url']}")
                
        # Check if we have a repeated crash URL
        last_url = progress.get('last_crash_url')
        if last_url and crash_urls.count(last_url) >= 2:
            print(f"⚠️  URL crashes repeatedly: {last_url}")
            print(f"   🚫 Adding to exclusion list to prevent infinite loops")
            self.add_to_exclusion_list(last_url)
        
        while restart_count < self.max_restarts:
            try:
                # Build command
                cmd = [sys.executable, '-X', 'utf8', 'main_new.py'] + args
                if restart_count > 0:
                    cmd.append('--resume')  # Resume from checkpoint on restart
                
                print(f"\n🔄 Running: {' '.join(cmd)}")
                print("-" * 60)
                
                # Run crawler and capture output to detect crash URL
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                    encoding='utf-8',
                    errors='replace'
                )
                
                output_lines = []
                try:
                    # Stream output in real-time
                    for line in process.stdout:
                        print(line, end='')
                        output_lines.append(line.strip())
                    
                    process.wait()
                    result_code = process.returncode
                except KeyboardInterrupt:
                    process.terminate()
                    raise
                
                if result_code == 0:
                    print("\n✅ Crawler completed successfully!")
                    self.cleanup_progress()
                    return 0
                elif result_code in [-1073741819, 3221225477]:  # 0xC0000005 access violation (signed and unsigned)
                    restart_count += 1
                    crash_url = self.extract_current_url_from_output(output_lines)
                    crash_urls.append(crash_url) if crash_url else None
                    
                    print(f"\n💥 Process crashed with access violation (attempt {restart_count}/{self.max_restarts})")
                    print(f"   Exit code: {result_code}")
                    if crash_url:
                        print(f"   🔗 Crash URL: {crash_url}")
                        
                        # Check if this URL has crashed before
                        if crash_urls.count(crash_url) >= 2:
                            print(f"   🚫 URL has crashed {crash_urls.count(crash_url)} times - adding to exclusion list")
                            self.add_to_exclusion_list(crash_url)
                    
                    if restart_count < self.max_restarts:
                        print(f"⏱️  Waiting {self.restart_delay} seconds before restart...")
                        time.sleep(self.restart_delay)
                        self.save_progress(restart_count, crash_url, crash_urls)
                        print(f"🔄 Restarting crawler (attempt {restart_count + 1}/{self.max_restarts})...")
                    else:
                        print(f"❌ Max restarts ({self.max_restarts}) reached. Giving up.")
                        if crash_url and crash_urls.count(crash_url) >= 1:
                            print(f"   🚫 Final crash URL added to exclusion list: {crash_url}")
                            self.add_to_exclusion_list(crash_url)
                        return 1
                else:
                    print(f"\n❌ Crawler failed with exit code: {result_code}")
                    return result_code
                    
            except KeyboardInterrupt:
                print("\n⚠️ User interrupted. Saving progress...")
                self.save_progress(restart_count)
                return 130
            except Exception as e:
                print(f"\n❌ Wrapper error: {e}")
                return 1
        
        return 1
    
    def add_to_exclusion_list(self, url):
        """Add URL to exclusion list."""
        with open(self.crash_urls_file, 'a', encoding='utf-8') as f:
            f.write(f"{url}\n")
    
    def extract_current_url_from_output(self, output_lines):
        """Extract the currently processing URL from crawler output."""
        for line in reversed(output_lines):
            if "🌐 Crawling:" in line:
                # Extract URL from line like "🌐 Crawling: https://example.com (1/100)"
                try:
                    start = line.find("https://")
                    if start != -1:
                        end = line.find(" (", start)
                        if end != -1:
                            return line[start:end]
                except:
                    pass
        return None
    
    def cleanup_progress(self):
        """Clean up progress file on successful completion."""
        for file in ['crawler_progress.json', 'crawler_checkpoint.json']:
            if os.path.exists(file):
                try:
                    os.remove(file)
                    print(f"   🗑️ Cleaned up: {file}")
                except:
                    pass

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Crawler wrapper with crash recovery',
        add_help=False  # Let the main crawler handle help
    )
    
    # Just pass through all arguments to main crawler
    args = sys.argv[1:]
    
    wrapper = CrawlerWrapper()
    return wrapper.run_crawler(args)

if __name__ == '__main__':
    exit(main())