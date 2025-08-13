#!/usr/bin/env python3
"""
Standalone script to process a single file for semantic chunking.
This runs in a separate process for performance.
"""

import os
import sys
import json
import argparse
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.semantic.providers.gemini_client import GeminiClient
from src.semantic.providers.azure_client import OpenAIClient, AzureOpenAIClient


def main():
    """Main function to process a single file."""
    parser = argparse.ArgumentParser(description='Process single file for semantic chunking')
    parser.add_argument('--input', required=True, help='Input markdown file path')
    parser.add_argument('--output', required=True, help='Output semantic chunks file path')
    parser.add_argument('--source-url', required=True, help='Source URL for the content')
    parser.add_argument('--provider', default='gemini', choices=['gemini', 'openai', 'azure'], help='LLM provider to use')
    parser.add_argument('--model', help='Model name (defaults based on provider)')
    parser.add_argument('--azure-endpoint', help='Azure OpenAI endpoint URL')
    parser.add_argument('--azure-api-version', help='Azure OpenAI API version')
    parser.add_argument('--azure-deployment', help='Azure OpenAI deployment name')
    
    args = parser.parse_args()
    
    try:
        # Initialize client based on provider
        provider = args.provider.lower()
        if provider == 'openai':
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                print("Error: OPENAI_API_KEY environment variable not found", file=sys.stderr)
                sys.exit(1)
            
            model_name = args.model or 'gpt-4o-mini'
            client = OpenAIClient(api_key=api_key, model_name=model_name)
        elif provider == 'azure':
            api_key = os.getenv('OPENAI_API_KEY') or os.getenv('AZURE_OPENAI_API_KEY_41')
            if not api_key:
                print("Error: OPENAI_API_KEY (or AZURE_OPENAI_API_KEY_41) not found for Azure provider", file=sys.stderr)
                sys.exit(1)

            # For Azure, model_name should be the deployment name
            model_name = args.model or args.azure_deployment or 'gpt-4o-mini'

            if not args.azure_endpoint or not args.azure_api_version or not (args.azure_deployment or model_name):
                print("Error: Missing Azure settings. Provide --azure-endpoint, --azure-api-version, and --azure-deployment", file=sys.stderr)
                sys.exit(1)

            client = AzureOpenAIClient(
                api_key=api_key,
                model_name=model_name,
                endpoint=args.azure_endpoint,
                api_version=args.azure_api_version,
            )
        else:
            # Default to Gemini
            api_key = os.getenv('GEMINI_API_KEY')
            if not api_key:
                print("Error: GEMINI_API_KEY environment variable not found", file=sys.stderr)
                sys.exit(1)
            
            model_name = args.model or 'gemini-2.5-flash'
            client = GeminiClient(api_key=api_key, model_name=model_name)
        
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
        
        # Add source URL to the JSON structure
        final_data = {
            "source": args.source_url,
            "chunks": chunks
        }
        
        # Ensure output directory exists
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save as JSON to semantic directory
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)
        
        print(f"Success: Generated {len(chunks)} semantic chunks")
        sys.exit(0)
        
    except Exception as e:
        # Clean up error message to remove control characters
        error_msg = str(e)
        # Remove control characters and limit length
        error_msg = ''.join(char for char in error_msg if ord(char) >= 32 or char in ['\n', '\t'])
        if len(error_msg) > 200:
            error_msg = error_msg[:200] + "... [truncated]"
        
        # Check for specific API quota errors
        if "429" in error_msg and "quota" in error_msg.lower():
            print("Error: API quota exceeded - please wait before retrying", file=sys.stderr)
        elif "401" in error_msg or "unauthorized" in error_msg.lower():
            # Tailor the guidance based on provider and whether Azure OpenAI was used
            provider = args.provider.lower()
            if provider == 'openai':
                print("Error: API authentication failed - check OPENAI_API_KEY", file=sys.stderr)
            elif provider == 'azure':
                print("Error: API authentication failed - verify OPENAI_API_KEY and Azure OpenAI endpoint/api_version/deployment settings", file=sys.stderr)
            else:
                print("Error: API authentication failed - check GEMINI_API_KEY", file=sys.stderr)
        else:
            print(f"Error: {error_msg}", file=sys.stderr)
        
        sys.exit(1)


if __name__ == '__main__':
    main()