class ContextualChunking:
    def __init__(self, document_path: str, document_content: str = None):
        """
        Initialize with document path and optionally load content
        
        Args:
            document_path: Path to the document to chunk
            document_content: Optional pre-loaded document content
        """
        if document_content is not None:
            self.document_content = document_content
        else:
            # Try multiple encodings to handle various file formats
            encodings = ['utf-8', 'iso-8859-1', 'windows-1252', 'cp1252']
            content = None
            
            for encoding in encodings:
                try:
                    with open(document_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            
            if content is None:
                # If all encodings fail, use utf-8 with error replacement
                with open(document_path, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.read()
            
            self.document_content = content
    
    def get_full_prompt(self):
        """
        Returns the prompt for contextual chunking with document content
        """
        prompt_template = """
TASK:

**Semantic Chunking:** 
1. Perform semantic chunking to given markdown document below. Group related headers and its content together.
2. Maintain the exact words used in the main content.
3. Include all content, including non-english words.
4. FAQs: Always format as "Q: [complete question] A: [complete answer]" with seperator between QA pairs. 
    - Ensure you KEEP the question. Do not omit it. 
    - If there is a series of Q and A, do not put header for each.
5. Prefix with a dash (-) when there are multiple lines.
6. Headers should contain explicitly the service/product/plan/offer it is about. If content is very minimal, do not include headers.
    - Headers can contain spaces.
    - Example: If product/service is "E-SIM Travel" then "Info" should be "Info E-SIM Travel".
    - Example: If product/service is "Mobile Prepaid" then "FAQs" should be "FAQs Mobile Prepaid".
7. Steps should be as numbered list.

    
Hint for grouping chunks:
    - Pricing/offer/Specs/Product should be grouped with Terms of Conditions/Service
    - Description (Benefits/Rationale/use-cases/etc)
    - FAQs


Chunk size:
    - If a section is too large (~200 words), split into multiple chunks
    - If a section is too small (~20 words), merge with the previous chunk


Headers to ignore:
    - [summary]
    - [pagination]
    - [page number]
    - Navigation elements
    - Sidebars
    - Footer content
    - Version information
    - Advertisement sections
    - Cookie notices, cookie banners
    - URLs for images

---

2. **Context Retrieval:** 

- Give a short succinct context to situate this chunk **within the overall document** for the purposes of improving search retrieval of the chunk. 
- Define some non-obvious key terms or abbreviations **only if needed**.
- Mention if it is for Consumer only or Enterprise only, but no need to mention it's from Mauritius Telecom. 
- If URL is devices.myt.mu, include the device or devices it is about clearly.
   
---

3. **Keyword Generation:** From the extracted chunk {{CHUNK_CONTENT}} , extract 2 to max 10 concise and specific keywords for search indexing or content categorization.

3.1. The keywords must reflect core offerings, product names, or specific service features mentioned in the content.

3.2. Each keyword should clearly refer to the core product/service/app/feature, not just generic categories (e.g., use "my.t Traffic Watch app" instead of "app download support").

3.3. At least one keyword must indicate the audience type, using "Consumer only" or "Enterprise only".

3.3. At least one keyword must be the core product/service/app/feature name.

3.4. We know its for Mauritius and Mauritius Telecom. Not a keyword. However if it is a specific location or another country, you may include it.

3.5. If URL is devices.myt.mu, include the device or devices it is about in keywords.

---

Return your response strictly as below in JSON format but with no tags, no json tags, no ```, no explanations nor extra text.

IMPORTANT: In JSON strings, use \\n for newlines, not actual line breaks. Use commas between JSON objects in the array.

Your response format should be as per template: 

[
    {
    "content": "Chunk 1 of <total>\\n# Chunk Context:\\n{{CHUNK_CONTEXT}}\\n# Chunk Content:\\n{{CHUNK_CONTENT}}",
    "keywords": ["keyword1", "keyword2", "..."]
    },
    
    {
    "content": "Chunk 2 of <total>\\n# Chunk Context:\\n{{CHUNK_CONTEXT}}\\n# Chunk Content:\\n{{CHUNK_CONTENT}}",
    "keywords": ["keyword1", "keyword2", "..."]
    },
    ...
]

Note: Chunk 1 of <total> not needed if there is only 1 chunk, else put it.

---

BELOW IS THE DOCUMENT:

<document> 

DOCUMENT_CONTENT_PLACEHOLDER

</document> 

        """
        
        return prompt_template.replace("DOCUMENT_CONTENT_PLACEHOLDER", self.document_content)