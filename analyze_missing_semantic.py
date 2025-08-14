#!/usr/bin/env python3
"""
Analyze which files are missing from semantic processing and why.
"""

import os
import json
from pathlib import Path

def get_base_filename(filepath):
    """Get base filename without path and extension."""
    return Path(filepath).stem

def analyze_missing_files():
    """Find which files are missing from semantic processing."""
    
    # Paths
    docling_dir = "D:/GIT/Craw4Ai_Docling/crawled_docling/20250814_212627/myt.mu"
    semantic_dir = "D:/GIT/Craw4Ai_Docling/crawled_semantic/20250814_212627/myt.mu"
    
    # Get all .md files from docling
    docling_files = []
    if os.path.exists(docling_dir):
        for file in os.listdir(docling_dir):
            if file.endswith('.md'):
                docling_files.append(file)
    
    # Get all .json files from semantic
    semantic_files = []
    if os.path.exists(semantic_dir):
        for file in os.listdir(semantic_dir):
            if file.endswith('.json'):
                semantic_files.append(file)
    
    # Create sets of base filenames
    docling_bases = {get_base_filename(f) for f in docling_files}
    semantic_bases = {get_base_filename(f) for f in semantic_files}
    
    # Find missing files
    missing_files = docling_bases - semantic_bases
    
    print(f"Total .md files in docling: {len(docling_files)}")
    print(f"Total .json files in semantic: {len(semantic_files)}")
    print(f"Missing files from semantic: {len(missing_files)}")
    print("=" * 60)
    
    # Analyze each missing file
    categories = {
        'empty_or_source_only': [],
        'pdf_placeholder': [],
        'very_short': [],
        'normal_content': [],
        'encoding_issues': []
    }
    
    for missing_base in missing_files:
        md_file = f"{missing_base}.md"
        full_path = os.path.join(docling_dir, md_file)
        
        if not os.path.exists(full_path):
            continue
            
        try:
            # Try reading with different encodings
            content = None
            encodings = ['utf-8', 'iso-8859-1', 'windows-1252', 'cp1252']
            
            for encoding in encodings:
                try:
                    with open(full_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            
            if content is None:
                categories['encoding_issues'].append((md_file, "Could not decode with any encoding"))
                continue
            
            lines = content.strip().split('\n')
            non_empty_lines = [line.strip() for line in lines if line.strip()]
            
            # Check if only source header
            if len(lines) <= 4 and any(line.startswith('# Source:') for line in lines):
                if len(non_empty_lines) <= 3:  # Source, ---, maybe empty
                    categories['empty_or_source_only'].append((md_file, f"{len(non_empty_lines)} non-empty lines"))
                    continue
            
            # Check for PDF placeholder
            if 'PDF Document could not be extracted' in content or '[PDF Document' in content:
                categories['pdf_placeholder'].append((md_file, "PDF extraction failed"))
                continue
            
            # Check content length
            content_lines = len(non_empty_lines)
            if content_lines <= 10:
                categories['very_short'].append((md_file, f"{content_lines} content lines"))
                continue
            
            # Has normal content
            categories['normal_content'].append((md_file, f"{content_lines} content lines, {len(content)} chars"))
            
        except Exception as e:
            categories['encoding_issues'].append((md_file, f"Error reading file: {e}"))
    
    # Print results
    for category, files in categories.items():
        if files:
            print(f"\n{category.replace('_', ' ').title()}: {len(files)} files")
            print("-" * 40)
            for filename, details in sorted(files):
                print(f"  {filename} - {details}")
    
    # Show some examples of normal content files that failed
    if categories['normal_content']:
        print(f"\nExamples of files with normal content that failed semantic processing:")
        print("-" * 60)
        for filename, details in sorted(categories['normal_content'])[:5]:
            full_path = os.path.join(docling_dir, filename)
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Show first few lines
                    lines = content.split('\n')[:10]
                    print(f"\n{filename} ({details}):")
                    for i, line in enumerate(lines, 1):
                        print(f"  {i:2d}: {line}")
                    if len(content.split('\n')) > 10:
                        print(f"  ... ({len(content.split('\n'))} total lines)")
            except Exception as e:
                print(f"  Error reading {filename}: {e}")

if __name__ == '__main__':
    analyze_missing_files()