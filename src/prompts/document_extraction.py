class DocumentExtraction:
    def __init__(self, document_content: str, file_path: str = "", target_market: str = "Consumer"):
        """
        Initialize with document content and metadata
        
        Args:
            document_content: The extracted text content from the document
            file_path: Path to the document (for URL reference)
            target_market: Target market (Consumer or Enterprise)
        """
        self.document_content = document_content
        self.file_path = file_path
        self.target_market = target_market
    
    def get_extraction_prompt(self):
        """
        Returns the prompt for document content extraction
        """
        prompt_template = """Extract the content from this document in markdown.

=== IMPORTANT ===
1. Maintain the exact words used in the main content.
2. Include all content, including non-english words.
3. For multiple Offers/Plans/Services: Format as table.
4. FAQs: Always format as "Q: [complete question] A: [complete answer]" with seperator between QA pairs. 
    - Ensure you KEEP the question. Do not omit it. 
    - If there is a series of Q and A, do not put header for each.
5. Prefix with a dash (-) when there are multiple lines.
5. Exclude:
    - Navigation elements
    - Sidebars
    - Footer content
    - Version information
    - Advertisement sections
    - Cookie notices, cookie banners
    - URLs for images
6. First Header should be "summary". Be precise to include the service/product/plan/offer it is about. No need say it is for Mauritius Telecom since it is known. Start with "Contains". Include the page URL and target market: "{target_market}".
7. Headers should contain explicitly the service/product/plan/offer it is about. If content is very minimal, do not include headers.
    - Headers can contain spaces.
    - Example: If product/service is "E-SIM Travel" then "Info" should be "Info E-SIM Travel".
    - Example: If product/service is "Mobile Prepaid" then "FAQs" should be "FAQs Mobile Prepaid".
8. Steps should be as numbered list.
9. Include URLs for reference if they are important (only in Read More/Learn More). Do not put reference URLs in tables.

Format the output as clean markdown with proper code (for eg text messages) and headers.

DOCUMENT CONTENT:
{document_content}

FILE PATH: {file_path}
TARGET MARKET: {target_market}
"""
        
        return prompt_template.format(
            document_content=self.document_content,
            file_path=self.file_path,
            target_market=self.target_market
        )