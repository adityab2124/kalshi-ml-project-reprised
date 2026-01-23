import os
import re
from typing import Optional, Dict
from google import genai
from google.genai import types

def get_market_context(ticker_name: str, contract_details: str, enabled: bool = True) -> Optional[Dict[str, str]]:
    if not enabled: 
        return None
    
    api_key = os.getenv('GOOGLE_API_KEY')
    if not api_key:
        print("⚠️ GOOGLE_API_KEY not set.")
        return None
    
    try:
        # Initialize client
        client = genai.Client(api_key=api_key)
        
        # Use the ID directly from your list (no 'models/' prefix)
        model_id = 'gemini-2.0-flash'
        
        # Define the Search Tool properly
        google_search_tool = types.Tool(
            google_search=types.GoogleSearch()
        )
        
        prompt = (
            f"A Kalshi prediction market contract just spiked in price. "
            f"Market ticker: {ticker_name}. "
            f"Contract name/answer: '{contract_details}'. "
            f"Search Google and X/Twitter for breaking news from the last 60 minutes that would explain why '{contract_details}' or related topics are suddenly trending or mentioned. "
            f"If you find relevant news, provide a concise 1-sentence explanation and include the source URL. "
            f"If no recent news is found, say 'No recent news found explaining this spike.'"
        )

        # Call with the correct Tool and Config structure
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[google_search_tool],
                temperature=0.7
            )
        )
        
        summary = response.text.strip()
        
        # Extract URL from metadata
        url = "No URL found"
        try:
            # Check the grounding metadata for the first chunk's URI
            if response.candidates[0].grounding_metadata.grounding_chunks:
                url = response.candidates[0].grounding_metadata.grounding_chunks[0].web.uri
        except:
            # Fallback to regex if metadata is empty
            url_match = re.search(r'https?://[^\s]+', summary)
            if url_match: 
                url = url_match.group(0)

        return {'summary': summary, 'url': url}
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None
