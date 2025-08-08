class KeywordGenerator:
    def __init__(self, chunk_content: str):
        """
        Initialize with chunk content for keyword generation
        
        Args:
            chunk_content: The content to extract keywords from
        """
        self.chunk_content = chunk_content
    
    def get_keyword_prompt(self):
        """
        Returns the prompt for keyword generation
        """
        prompt_template = """
**Keyword Generation:** From the extracted chunk below, extract 2 to max 10 concise and specific keywords for search indexing or content categorization.

1. The keywords must reflect core offerings, product names, or specific service features mentioned in the content.

2. Each keyword should clearly refer to the core product/service/app/feature, not just generic categories (e.g., use "my.t Traffic Watch app" instead of "app download support").

3. At least one keyword must indicate the audience type, using "For Consumer" or "For Enterprise only".

4. At least one keyword must be the core product/service/app/feature name.

5. For procedure docs, extract the procedure tools, system, target audience, staff applicable, and other keywords.

6. If URL is devices.myt.mu, include the device or devices it is about in keywords.

Return ONLY a JSON object with a keywords array, nothing else. Example:
{"keywords": ["keyword1", "keyword2", "keyword3"]}


CHUNK CONTENT:
{chunk_content}

        """
        
        return prompt_template.replace("{chunk_content}", self.chunk_content)