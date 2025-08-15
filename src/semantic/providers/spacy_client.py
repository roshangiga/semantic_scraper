#!/usr/bin/env python3
"""
SpaCy-based semantic chunking client using transformer models.
"""

import spacy
import json
import re
from typing import List, Dict, Any
import logging
from .base_client import BaseLLMClient


class SpacyClient(BaseLLMClient):
    """SpaCy client for semantic chunking using transformer models and NLP."""
    
    def __init__(self, api_key: str = None, model_name: str = "en_core_web_trf"):
        """
        Initialize the SpaCy client.
        
        Args:
            api_key: Not used for SpaCy (kept for compatibility)
            model_name: SpaCy model name (default: en_core_web_trf)
        """
        super().__init__(api_key, model_name)
        self.nlp = None
        self.max_chunk_words = 200  # If section is too large (~200 words), split
        self.min_chunk_words = 20   # If section is too small (~20 words), merge with previous
        self._load_model()
    
    def _load_model(self):
        """Load the SpaCy model."""
        try:
            logging.info(f"Loading SpaCy model: {self.model_name}")
            self.nlp = spacy.load(self.model_name)
            
            # Add sentence segmentation if not present
            if "sentencizer" not in self.nlp.pipe_names:
                self.nlp.add_pipe("sentencizer")
            
            logging.info(f"✅ SpaCy model loaded successfully")
        except OSError as e:
            logging.error(f"Failed to load SpaCy model {self.model_name}: {e}")
            logging.info("Trying to download the model...")
            try:
                import subprocess
                subprocess.run(["python", "-m", "spacy", "download", self.model_name], check=True)
                self.nlp = spacy.load(self.model_name)
                if "sentencizer" not in self.nlp.pipe_names:
                    self.nlp.add_pipe("sentencizer")
                logging.info(f"✅ SpaCy model downloaded and loaded successfully")
            except Exception as download_error:
                logging.error(f"Failed to download SpaCy model: {download_error}")
                raise
    
    def process_document_for_chunking(self, document_content: str) -> List[Dict[str, Any]]:
        """
        Process a markdown document for contextual chunking using SpaCy.
        
        TASK:
        1. Semantic Chunking: Group related headers and content together
        2. Maintain exact words used in main content
        3. Include all content, including non-english words
        4. Format FAQs properly
        5. Prefix with dash for multiple lines
        6. Enhance headers with service/product/plan/offer context
        7. Format steps as numbered list
        
        Args:
            document_content: The markdown content to process
            
        Returns:
            List of chunks with content and keywords in JSON format
        """
        if not self.nlp:
            raise ValueError("SpaCy model not loaded")
        
        try:
            # Clean document by removing headers to ignore
            cleaned_content = self._remove_ignored_headers(document_content)
            
            # Split into sections based on headers
            sections = self._split_into_sections(cleaned_content)
            
            # Group sections into semantic chunks based on hints
            grouped_chunks = self._group_sections_semantically(sections)
            
            # Format chunks according to all rules
            formatted_chunks = self._format_final_chunks(grouped_chunks)
            
            logging.info(f"✅ Processed document into {len(formatted_chunks)} semantic chunks")
            return formatted_chunks
            
        except Exception as e:
            logging.error(f"Error processing document with SpaCy: {e}")
            raise
    
    def _remove_ignored_headers(self, content: str) -> str:
        """
        Remove headers to ignore as specified in prompt:
        - [summary], [pagination], [page number]
        - Navigation elements, Sidebars, Footer content
        - Version information, Advertisement sections
        - Cookie notices/banners
        - URLs for images
        """
        # Headers to ignore patterns
        ignore_patterns = [
            r'\[summary\]',
            r'\[pagination\]',
            r'\[page\s*number\]',
            r'(?i)navigation\s+elements?',
            r'(?i)sidebars?',
            r'(?i)footer\s+content',
            r'(?i)version\s+information',
            r'(?i)advertisement\s+sections?',
            r'(?i)cookie\s+notices?',
            r'(?i)cookie\s+banners?',
            r'!\[.*?\]\(.*?\)',  # Image URLs in markdown
            r'https?://[^\s]+\.(?:jpg|jpeg|png|gif|svg|webp)'  # Direct image URLs
        ]
        
        cleaned = content
        for pattern in ignore_patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE | re.MULTILINE)
        
        # Remove multiple blank lines
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
        
        return cleaned.strip()
    
    def _split_into_sections(self, content: str) -> List[Dict[str, str]]:
        """Split content into sections based on markdown headers."""
        sections = []
        current_section = {'header': '', 'content': '', 'level': 0}
        
        lines = content.split('\n')
        for line in lines:
            # Check if line is a header
            header_match = re.match(r'^(#{1,6})\s+(.+)$', line)
            
            if header_match:
                # Save previous section if it has content
                if current_section['content'].strip():
                    sections.append(current_section)
                
                # Start new section
                level = len(header_match.group(1))
                header_text = header_match.group(2).strip()
                current_section = {
                    'header': header_text,
                    'content': '',
                    'level': level
                }
            else:
                # Add content to current section
                current_section['content'] += line + '\n'
        
        # Don't forget the last section
        if current_section['content'].strip():
            sections.append(current_section)
        
        return sections
    
    def _group_sections_semantically(self, sections: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Group sections semantically based on hints:
        - Pricing/offer/Specs/Product grouped with Terms of Conditions/Service
        - Description (Benefits/Rationale/use-cases/etc)
        - FAQs
        
        Chunk size rules:
        - If section too large (~200 words), split into multiple chunks
        - If section too small (~20 words), merge with previous chunk
        """
        grouped_chunks = []
        current_group = {'sections': [], 'word_count': 0, 'type': None}
        
        for section in sections:
            # Determine section type
            section_type = self._determine_section_type(section['header'], section['content'])
            
            # Process section with SpaCy to get word count
            doc = self.nlp(section['content'])
            word_count = len([token for token in doc if not token.is_space and not token.is_punct])
            
            # Decide grouping based on type and size
            should_merge = self._should_merge_sections(
                current_group['type'], 
                section_type, 
                current_group['word_count'],
                word_count
            )
            
            if should_merge and current_group['sections']:
                # Merge with current group
                current_group['sections'].append(section)
                current_group['word_count'] += word_count
                
                # Check if group is getting too large
                if current_group['word_count'] >= self.max_chunk_words:
                    grouped_chunks.append(current_group)
                    current_group = {'sections': [], 'word_count': 0, 'type': None}
            else:
                # Save current group if it exists
                if current_group['sections']:
                    grouped_chunks.append(current_group)
                
                # Check if section itself is too large
                if word_count > self.max_chunk_words:
                    # Split large section
                    split_sections = self._split_large_section(section, doc)
                    for split_section in split_sections:
                        grouped_chunks.append({
                            'sections': [split_section],
                            'word_count': self._count_words(split_section['content']),
                            'type': section_type
                        })
                    current_group = {'sections': [], 'word_count': 0, 'type': None}
                else:
                    # Start new group
                    current_group = {
                        'sections': [section],
                        'word_count': word_count,
                        'type': section_type
                    }
        
        # Don't forget the last group
        if current_group['sections']:
            grouped_chunks.append(current_group)
        
        return grouped_chunks
    
    def _determine_section_type(self, header: str, content: str) -> str:
        """Determine the semantic type of a section."""
        header_lower = header.lower()
        content_lower = content.lower()
        
        # Check for specific section types based on hints
        if any(term in header_lower for term in ['pricing', 'offer', 'specs', 'product', 'terms', 'conditions', 'service']):
            return 'pricing_terms'
        elif any(term in header_lower for term in ['benefit', 'rationale', 'use-case', 'description', 'feature', 'about']):
            return 'description'
        elif 'faq' in header_lower or ('q:' in content_lower and 'a:' in content_lower):
            return 'faq'
        else:
            return 'general'
    
    def _should_merge_sections(self, current_type: str, new_type: str, 
                              current_words: int, new_words: int) -> bool:
        """Determine if sections should be merged based on type and size."""
        # If new section is too small, merge with previous
        if new_words < self.min_chunk_words:
            return True
        
        # If current group is empty, don't merge
        if not current_type:
            return False
        
        # If adding would exceed max size, don't merge
        if current_words + new_words > self.max_chunk_words:
            return False
        
        # Merge related types (as per hints)
        type_compatibility = {
            'pricing_terms': ['pricing_terms'],  # Keep pricing/terms together
            'description': ['description'],       # Keep descriptions together
            'faq': ['faq'],                      # Keep FAQs together
            'general': ['general']               # Keep general content together
        }
        
        return new_type in type_compatibility.get(current_type, [])
    
    def _split_large_section(self, section: Dict[str, str], doc) -> List[Dict[str, str]]:
        """Split a large section into smaller chunks."""
        sentences = list(doc.sents)
        chunks = []
        current_chunk_sentences = []
        current_word_count = 0
        
        for sent in sentences:
            sent_word_count = len([token for token in sent if not token.is_space and not token.is_punct])
            
            if current_word_count + sent_word_count > self.max_chunk_words and current_chunk_sentences:
                # Create chunk from accumulated sentences
                chunk_content = ' '.join([s.text for s in current_chunk_sentences])
                chunks.append({
                    'header': section['header'],
                    'content': chunk_content,
                    'level': section['level']
                })
                current_chunk_sentences = [sent]
                current_word_count = sent_word_count
            else:
                current_chunk_sentences.append(sent)
                current_word_count += sent_word_count
        
        # Add remaining sentences
        if current_chunk_sentences:
            chunk_content = ' '.join([s.text for s in current_chunk_sentences])
            chunks.append({
                'header': section['header'],
                'content': chunk_content,
                'level': section['level']
            })
        
        return chunks
    
    def _count_words(self, text: str) -> int:
        """Count words in text using SpaCy."""
        doc = self.nlp(text)
        return len([token for token in doc if not token.is_space and not token.is_punct])
    
    def _format_final_chunks(self, grouped_chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format chunks according to all specified rules."""
        formatted_chunks = []
        total_chunks = len(grouped_chunks)
        
        for i, group in enumerate(grouped_chunks, 1):
            # Combine and format content from all sections in group
            formatted_content = self._format_group_content(group['sections'])
            
            # Process with SpaCy for analysis
            doc = self.nlp(formatted_content)
            
            # Generate context (as per rule 2)
            context = self._generate_chunk_context(doc, group['sections'])
            
            # Extract keywords (as per rule 3)
            keywords = self._extract_chunk_keywords(doc, formatted_content)
            
            # Format final chunk content
            chunk_content = f"Chunk {i} of {total_chunks}\\n# Chunk Context:\\n{context}\\n# Chunk Content:\\n{formatted_content}"
            
            formatted_chunks.append({
                'content': chunk_content,
                'keywords': keywords
            })
        
        return formatted_chunks
    
    def _format_group_content(self, sections: List[Dict[str, str]]) -> str:
        """
        Format group content according to rules:
        1. Maintain exact words
        2. Include all content including non-english
        3. Format FAQs as Q: [question] A: [answer]
        4. Prefix with dash for multiple lines
        5. Enhance headers with service/product context
        6. Format steps as numbered list
        """
        formatted_parts = []
        
        for section in sections:
            # Process and enhance header (rule 6)
            if section['header']:
                enhanced_header = self._enhance_header_with_context(section['header'], section['content'])
                formatted_parts.append(f"# {enhanced_header}")
            
            # Format content based on type
            content_lines = section['content'].strip().split('\n')
            formatted_lines = []
            
            for line in content_lines:
                line = line.strip()
                if not line:
                    continue
                
                # Check for FAQ format (rule 4)
                if self._is_faq_content(line):
                    formatted_lines.append(self._format_faq_line(line))
                # Check for numbered steps (rule 7)
                elif re.match(r'^\d+[\.\)]\s', line):
                    formatted_lines.append(line)  # Already numbered
                # Multiple lines with dash prefix (rule 5)
                elif len(content_lines) > 1 and not line.startswith('-') and not self._is_faq_content(line):
                    formatted_lines.append(f"- {line}")
                else:
                    formatted_lines.append(line)
            
            formatted_parts.append('\n'.join(formatted_lines))
        
        return '\n\n'.join(formatted_parts)
    
    def _enhance_header_with_context(self, header: str, content: str) -> str:
        """
        Enhance headers with service/product/plan/offer context.
        Example: "Info" -> "Info E-SIM Travel"
        Example: "FAQs" -> "FAQs Mobile Prepaid"
        """
        # Process content to find product/service names
        doc = self.nlp(content[:500])  # Sample first 500 chars
        
        # Extract product/service entities
        products_services = []
        
        # Look for specific patterns
        patterns = [
            r'(?:E-SIM|e-sim|eSIM)\s+\w+',
            r'Mobile\s+(?:Prepaid|Postpaid)',
            r'my\.t\s+\w+',
            r'\w+\s+(?:Plan|Package|Service|Offer)',
            r'Fibre\s+\w+',
            r'Internet\s+\w+'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            products_services.extend(matches)
        
        # Also check entities
        for ent in doc.ents:
            if ent.label_ in ["PRODUCT", "ORG"]:
                products_services.append(ent.text)
        
        # Generic headers that need enhancement
        generic_headers = ['info', 'information', 'faqs', 'features', 'benefits', 
                          'pricing', 'terms', 'conditions', 'service', 'offer', 'specs']
        
        header_lower = header.lower()
        
        # If header is generic and we found products/services, enhance it
        if any(generic in header_lower for generic in generic_headers) and products_services:
            # Use the first found product/service
            product = products_services[0]
            # If content is minimal, don't include header (as per rule)
            if len(content.strip()) < 50:  # Minimal content threshold
                return header
            return f"{header} {product}"
        
        return header
    
    def _is_faq_content(self, line: str) -> bool:
        """Check if line is FAQ content."""
        patterns = [
            r'^Q\s*:\s*',
            r'^Question\s*:\s*',
            r'^A\s*:\s*',
            r'^Answer\s*:\s*'
        ]
        return any(re.match(pattern, line, re.IGNORECASE) for pattern in patterns)
    
    def _format_faq_line(self, line: str) -> str:
        """
        Format FAQ line as: Q: [complete question] A: [complete answer]
        Ensure KEEP the question, do not omit it.
        If series of Q and A, do not put header for each.
        """
        # Standardize format
        line = re.sub(r'^Question\s*:\s*', 'Q: ', line, flags=re.IGNORECASE)
        line = re.sub(r'^Answer\s*:\s*', 'A: ', line, flags=re.IGNORECASE)
        
        # Ensure complete question and answer are preserved
        return line
    
    def _generate_chunk_context(self, doc, sections: List[Dict[str, str]]) -> str:
        """
        Context Retrieval:
        - Give short succinct context to situate chunk within overall document
        - Define non-obvious key terms or abbreviations only if needed
        - Mention if Consumer only or Enterprise only (no need to mention Mauritius Telecom)
        - If URL is devices.myt.mu, include device clearly
        """
        context_parts = []
        
        # Determine audience type
        text_lower = doc.text.lower()
        if any(term in text_lower for term in ['enterprise', 'business', 'corporate', 'company']):
            context_parts.append("This section is for Enterprise only")
        else:
            context_parts.append("This section is for Consumer only")
        
        # Add section purpose based on headers
        if sections and sections[0]['header']:
            header = sections[0]['header']
            context_parts.append(f"covering {header}")
        
        # Define non-obvious abbreviations
        abbreviations = self._extract_non_obvious_abbreviations(doc)
        if abbreviations:
            definitions = []
            for abbr in abbreviations:
                # Try to find definition in context
                definition = self._find_abbreviation_definition(abbr, doc.text)
                if definition:
                    definitions.append(f"{abbr}: {definition}")
            if definitions:
                context_parts.append(f"Key terms - {', '.join(definitions)}")
        
        # Check for devices.myt.mu content
        if 'devices.myt.mu' in text_lower:
            devices = self._extract_device_names(doc)
            if devices:
                context_parts.append(f"Devices covered: {', '.join(devices)}")
        
        return '. '.join(context_parts) + '.'
    
    def _extract_non_obvious_abbreviations(self, doc) -> List[str]:
        """Extract non-obvious abbreviations that need definition."""
        abbreviations = []
        
        # Common abbreviations that don't need definition
        obvious = {'FAQ', 'URL', 'SMS', 'GB', 'MB', 'TV', 'HD', 'USD', 'EUR', 'IT'}
        
        for token in doc:
            if (token.text.isupper() and 
                len(token.text) > 1 and 
                len(token.text) <= 5 and
                token.text not in obvious and
                not token.is_stop):
                abbreviations.append(token.text)
        
        return list(set(abbreviations))[:3]
    
    def _find_abbreviation_definition(self, abbr: str, text: str) -> str:
        """Try to find the definition of an abbreviation in the text."""
        # Common patterns for definitions
        patterns = [
            rf'{abbr}\s*\(([^)]+)\)',  # ABBR (definition)
            rf'\(({abbr})\)',           # (ABBR) after definition
            rf'{abbr}\s*-\s*([^,\n]+)', # ABBR - definition
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_device_names(self, doc) -> List[str]:
        """Extract specific device names."""
        devices = []
        
        # Device patterns
        device_patterns = [
            r'iPhone\s*\d+\w*',
            r'Samsung\s*Galaxy\s*\w+',
            r'iPad\s*\w*',
            r'Huawei\s*\w+',
            r'Nokia\s*\w+',
            r'\w+\s*router',
            r'\w+\s*modem'
        ]
        
        text = doc.text
        for pattern in device_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            devices.extend(matches)
        
        # Also check entities
        for ent in doc.ents:
            if ent.label_ == "PRODUCT" and any(term in ent.text.lower() for term in ['phone', 'device', 'router', 'modem']):
                devices.append(ent.text)
        
        return list(set(devices))[:5]
    
    def _extract_chunk_keywords(self, doc, content: str) -> List[str]:
        """
        Keyword Generation (2 to max 10 keywords):
        3.1 Must reflect core offerings, product names, or specific service features
        3.2 Should refer to core product/service/app/feature, not generic categories
        3.3 At least one keyword must indicate audience type (Consumer only/Enterprise only)
        3.4 At least one keyword must be core product/service/app/feature name
        3.5 Not Mauritius/Mauritius Telecom, but include other locations
        3.6 If devices.myt.mu, include device names
        """
        keywords = []
        
        # 3.4 - Extract core product/service/app/feature names (MUST have at least one)
        core_products = []
        
        # Look for specific product patterns
        product_patterns = [
            r'my\.t\s+\w+\s*(?:app)?',  # my.t services/apps
            r'E-SIM\s*\w*',
            r'Mobile\s+(?:Prepaid|Postpaid)',
            r'Fibre\s+\w+',
            r'\w+\s+(?:Plan|Package|Service|Offer|App)',
            r'(?:Internet|Broadband)\s+\w+',
            r'\w+\s+Watch',  # e.g., Traffic Watch
        ]
        
        for pattern in product_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if len(match) > 3:  # Filter out too short matches
                    core_products.append(match.strip())
        
        # Add product entities
        for ent in doc.ents:
            if ent.label_ in ["PRODUCT", "ORG"]:
                # Skip Mauritius Telecom as per 3.5
                if 'mauritius' not in ent.text.lower() and 'telecom' not in ent.text.lower():
                    core_products.append(ent.text)
        
        # Add core products to keywords (ensuring we have at least one)
        if core_products:
            keywords.extend(core_products[:3])  # Add top 3 core products
        else:
            # If no specific product found, extract from content
            if 'service' in content.lower():
                keywords.append('service')
        
        # 3.3 - Add audience type (REQUIRED)
        text_lower = content.lower()
        if any(term in text_lower for term in ['enterprise', 'business', 'corporate']):
            keywords.append("Enterprise only")
        else:
            keywords.append("Consumer only")
        
        # 3.2 - Add specific features (not generic)
        feature_keywords = []
        
        # Look for specific features
        for ent in doc.ents:
            if ent.label_ in ["PRODUCT", "WORK_OF_ART", "EVENT"]:
                feature_keywords.append(ent.text)
        
        # Add specific app names
        app_matches = re.findall(r'my\.t\s+\w+\s+app', content, re.IGNORECASE)
        feature_keywords.extend(app_matches)
        
        keywords.extend(feature_keywords[:2])
        
        # 3.5 - Add location if not Mauritius
        for ent in doc.ents:
            if ent.label_ == "GPE":
                location = ent.text.lower()
                if 'mauritius' not in location:
                    keywords.append(ent.text)
        
        # 3.6 - If devices.myt.mu, include devices
        if 'devices.myt.mu' in text_lower:
            devices = self._extract_device_names(doc)
            keywords.extend(devices[:2])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_keywords = []
        for kw in keywords:
            kw_clean = kw.strip()
            if kw_clean and kw_clean.lower() not in seen:
                seen.add(kw_clean.lower())
                unique_keywords.append(kw_clean)
        
        # Ensure we have 2-10 keywords
        if len(unique_keywords) < 2:
            # Add the most relevant terms from content
            important_nouns = [token.text for token in doc 
                             if token.pos_ in ["NOUN", "PROPN"] 
                             and not token.is_stop 
                             and len(token.text) > 3][:2]
            unique_keywords.extend(important_nouns)
        
        return unique_keywords[:10]  # Maximum 10 keywords