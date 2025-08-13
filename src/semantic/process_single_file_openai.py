#!/usr/bin/env python3
"""
Standalone script to process a single file for semantic chunking using OpenAI.
This runs in a separate process for performance.
"""

import os
import sys
import argparse
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.semantic.providers.azure_client import OpenAIClient


def main():
    """Main function to process a single file."""
    parser = argparse.ArgumentParser(description='Process single file for semantic chunking using OpenAI')
    parser.add_argument('--input', required=True, help='Input markdown file path')
    parser.add_argument('--output', required=True, help='Output semantic chunks file path')
    parser.add_argument('--source-url', required=True, help='Source URL for the content')
    parser.add_argument('--model', default='gpt-4o', help='OpenAI model name')
    parser.add_argument('--endpoint', help='Azure OpenAI endpoint URL')
    parser.add_argument('--api-version', help='Azure OpenAI API version')
    
    args = parser.parse_args()
    
    try:
        # Initialize OpenAI client
        api_key = os.getenv('AZURE_OPENAI_API_KEY_41')
        if not api_key:
            print("Error: AZURE_OPENAI_API_KEY_41 environment variable not found", file=sys.stderr)
            sys.exit(1)
        
        client = OpenAIClient(
            api_key=api_key, 
            model_name=args.model,
            endpoint=args.endpoint,
            api_version=args.api_version
        )
        
        # Read input file
        if not os.path.exists(args.input):
            print(f"Error: Input file not found: {args.input}", file=sys.stderr)
            sys.exit(1)
        
        # Try multiple encodings to handle various file formats
        encodings = ['utf-8', 'iso-8859-1', 'windows-1252', 'cp1252']
        content = None
        
        for encoding in encodings:
            try:
                with open(args.input, 'r', encoding=encoding) as f:
                    content = f.read()
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            # If all encodings fail, use utf-8 with error replacement
            with open(args.input, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
        
        # Skip if file only contains source header (empty content)
        lines = content.strip().split('\n')
        if len(lines) <= 2 and lines[0].startswith('Source:'):
            print(f"Skipped: Empty content (only source header)")
            sys.exit(0)
        
        # Process for semantic chunks
        chunks = client.process_document_for_chunking(content)
        
        if not chunks:
            print("Warning: No chunks generated", file=sys.stderr)
            sys.exit(1)
        
        # Create chunked content
        chunked_content = []
        for i, chunk in enumerate(chunks, 1):
            chunk_content = chunk.get('content', '')
            keywords = chunk.get('keywords', [])
            
            # Add chunk separator and metadata
            chunked_content.append(f"<!-- CHUNK {i} -->")
            chunked_content.append(f"<!-- KEYWORDS: {', '.join(keywords)} -->")
            chunked_content.append("")
            chunked_content.append(chunk_content)
            chunked_content.append("")
            chunked_content.append(f"<!-- END CHUNK {i} -->")
            chunked_content.append("")
        
        # Combine all chunks
        final_content = '\n'.join(chunked_content)
        
        # Add source header
        final_content = f"Source: {args.source_url}\n\n{final_content}"
        
        # Ensure output directory exists
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to semantic directory
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(final_content)
        
        print(f"Success: Generated {len(chunks)} semantic chunks")
        sys.exit(0)
        
    except Exception as e:
        # Clean up error message to remove control characters
        error_msg = str(e)
        # Remove control characters and limit length
        error_msg = ''.join(char for char in error_msg if ord(char) >= 32 or char in ['\n', '\t'])
        if len(error_msg) > 200:
            error_msg = error_msg[:200] + "... [truncated]"
        
        # Check for specific API errors
        if "429" in error_msg and ("quota" in error_msg.lower() or "rate" in error_msg.lower()):
            print("Error: API rate limit or quota exceeded - please wait before retrying", file=sys.stderr)
        elif "401" in error_msg or "unauthorized" in error_msg.lower():
            print("Error: API authentication failed - check AZURE_OPENAI_API_KEY_41", file=sys.stderr)
        elif "404" in error_msg:
            print("Error: Model or endpoint not found - check Azure OpenAI configuration", file=sys.stderr)
        else:
            print(f"Error: {error_msg}", file=sys.stderr)
        
        sys.exit(1)


if __name__ == '__main__':
    main()