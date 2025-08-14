#!/usr/bin/env python3
"""
Crawler wrapper with crash recovery system.
Automatically restarts crawler on crashes and tracks problematic URLs.
"""

import os
import sys
import time
import subprocess
import argparse
from datetime import datetime
from pathlib import Path


class CrawlerWrapper:
    """Wrapper for main crawler with crash recovery capabilities."""
    
    def __init__(self):
        self.problematic_urls_file = 'problematic_urls.txt'
        self.max_restart_attempts = 5
        self.restart_delay = 10  # seconds between restarts
        
    def load_problematic_urls(self):
        """Load previously identified problematic URLs."""
        if os.path.exists(self.problematic_urls_file):
            try:
                with open(self.problematic_urls_file, 'r', encoding='utf-8') as f:
                    urls = [line.strip() for line in f if line.strip()]
                print(f"[INFO] Loaded {len(urls)} problematic URLs to skip")
                return set(urls)
            except Exception as e:
                print(f"[WARN] Could not load problematic URLs: {e}")
        return set()
    
    def save_problematic_url(self, url):
        """Save a URL that caused a crash."""
        try:
            with open(self.problematic_urls_file, 'a', encoding='utf-8') as f:
                f.write(f"{url}\n")
            print(f"[EXCLUDE] Added problematic URL: {url}")
        except Exception as e:
            print(f"[WARN] Could not save problematic URL: {e}")
    
    def get_last_processed_url_from_checkpoint(self):
        """Extract the last processed URL from checkpoint file."""
        checkpoint_file = 'crawler_checkpoint.json'
        if os.path.exists(checkpoint_file):
            try:
                import json
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                visited_urls = checkpoint.get('visited_urls', [])
                if visited_urls:
                    return visited_urls[-1]  # Return the most recently visited URL
            except Exception as e:
                print(f"[WARN] Could not read checkpoint: {e}")
        return None
    
    def run_crawler(self, args):
        """Run the main crawler script."""
        cmd = [sys.executable, 'main_new.py'] + args
        
        print(f"[START] Starting crawler: {' '.join(cmd)}")
        print(f"[TIME] Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 60)
        
        try:
            # Run the crawler with real-time output streaming
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace',  # Replace problematic characters
                bufsize=1,
                universal_newlines=True
            )
            
            # Stream output in real-time
            print("[DEBUG] Starting output streaming...")
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    # Handle Unicode issues for Windows terminal
                    try:
                        print(output.strip())
                    except UnicodeEncodeError:
                        # Fallback: encode as ASCII with replacement characters
                        safe_output = output.strip().encode('ascii', 'replace').decode('ascii')
                        print(safe_output)
                    sys.stdout.flush()  # Force immediate output
            
            # Get the final return code
            return_code = process.poll()
            return return_code
            
        except KeyboardInterrupt:
            print("\n[WARN] Crawler interrupted by user")
            return 130  # Standard exit code for keyboard interrupt
        except Exception as e:
            print(f"[ERROR] Error running crawler: {e}")
            return 1
    
    def run_with_recovery(self, args):
        """Run crawler with automatic crash recovery."""
        problematic_urls = self.load_problematic_urls()
        
        attempt = 0
        while attempt < self.max_restart_attempts:
            attempt += 1
            
            if attempt > 1:
                print(f"\n[RESTART] Restart attempt {attempt}/{self.max_restart_attempts}")
                print(f"[WAIT] Waiting {self.restart_delay} seconds before restart...")
                time.sleep(self.restart_delay)
            
            # Run the crawler
            exit_code = self.run_crawler(args)
            
            if exit_code == 0:
                print("\n[SUCCESS] Crawler completed successfully!")
                return 0
            elif exit_code == 130:  # Keyboard interrupt
                print("\n[WARN] Crawler stopped by user")
                return exit_code
            else:
                print(f"\n[ERROR] Crawler crashed with exit code: {exit_code}")
                
                # Try to identify the problematic URL from checkpoint
                last_url = self.get_last_processed_url_from_checkpoint()
                if last_url and last_url not in problematic_urls:
                    print(f"[SUSPECT] Suspected problematic URL: {last_url}")
                    self.save_problematic_url(last_url)
                    problematic_urls.add(last_url)
                
                if attempt < self.max_restart_attempts:
                    print(f"[RESTART] Will restart in {self.restart_delay} seconds... (attempt {attempt + 1}/{self.max_restart_attempts})")
                else:
                    print(f"[FAILED] Maximum restart attempts ({self.max_restart_attempts}) reached")
                    print("[STOP] Giving up - check problematic_urls.txt for URLs that may be causing crashes")
                    return exit_code
        
        return 1


def main():
    """Main entry point for crawler wrapper."""
    print("[WRAPPER] Crawler Wrapper with Crash Recovery")
    print("=" * 50)
    
    # Parse arguments to pass through to main crawler
    parser = argparse.ArgumentParser(description='Web Crawler Wrapper with Crash Recovery')
    parser.add_argument('--no-recovery', action='store_true', 
                       help='Run without crash recovery (single attempt)')
    parser.add_argument('--max-attempts', type=int, default=5,
                       help='Maximum restart attempts (default: 5)')
    parser.add_argument('--restart-delay', type=int, default=10,
                       help='Delay between restarts in seconds (default: 10)')
    
    # Parse known args and pass the rest to main crawler
    known_args, unknown_args = parser.parse_known_args()
    
    wrapper = CrawlerWrapper()
    wrapper.max_restart_attempts = known_args.max_attempts
    wrapper.restart_delay = known_args.restart_delay
    
    if known_args.no_recovery:
        print("[WARN] Running without crash recovery")
        exit_code = wrapper.run_crawler(unknown_args)
    else:
        print(f"[RESTART] Crash recovery enabled (max {known_args.max_attempts} attempts)")
        exit_code = wrapper.run_with_recovery(unknown_args)
    
    return exit_code


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n[WARN] Wrapper interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n[CRASH] Wrapper crashed: {e}")
        sys.exit(1)