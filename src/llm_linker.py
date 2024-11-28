import os
import time
from typing import List
import asyncio
import json
from dotenv import load_dotenv
import openai
from dataclasses import dataclass

from tqdm import tqdm

@dataclass
class SearchResults:
    artist_searches: List[str]
    album_searches: List[dict]
    song_searches: List[dict]

class MusicEntityExtractor:
    def __init__(self, api_key: str):
        """Initialize with your OpenAI API key"""
        # self.openai_model = "gpt-3.5-turbo"
        self.openai_model = "gpt-4o-mini"
        openai.api_key = api_key

    async def extract_searches(self, text:str) ->dict:
        client = openai.AsyncOpenAI()
        prompt = f"""Extract ONLY clearly identifiable music-related entities from this text. Return a JSON object with these keys:
            {{
                "artist_searches": [list of specific artist names only],
                "album_searches": [list of objects with "album_title" and "artist_name" if known],
                "song_searches": [list of objects with "song_title" and "artist_name" if known]
            }}
            
            Guidelines:
            - Only include actual artist names, song titles, and album names
            - Do not include generic descriptions
            - Do not include post markers
            - For artist names, resolve common abbreviations
            - If an album or song is mentioned with its artist, always pair them together
            - If unsure about whether something is a music entity, exclude it
            
            Text: {text}
            """
        try:
            response = await client.chat.completions.create(
                model=self.openai_model,
            messages=[
                {"role": "system", "content": "You are a precise music information extraction system. Only extract specific, verifiable music entities."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            response_format={ "type": "json_object" }
        )
        except Exception as e:
            print(f"Error extracting searches: {e}")
            return SearchResults(artist_searches=[], album_searches=[], song_searches=[])
                
        return SearchResults(**json.loads(response.choices[0].message.content))
        
    def extract_searches_batch(self, texts: List[str],batch_size:int=10) -> List[SearchResults]:
        """Extract music entities from multiple texts concurrently"""
        tasks = [self.extract_searches(text) for text in texts]
        # wait for all tasks to complete
        results = []
        for i in tqdm(range(0, len(texts), batch_size)):
            batch_tasks = tasks[i:i+batch_size]
            batch_results = asyncio.run(self.extract_batch(batch_tasks))
            results.extend(batch_results)
        return results

    async def extract_batch(self, batch_tasks)->List[SearchResults]:
        return await asyncio.gather(*batch_tasks)
    




if __name__ == "__main__":
    # tests
    load_dotenv()
    extractor = MusicEntityExtractor(os.getenv("OPENAI_API_KEY"))
    texts = [
        "I love the song 'Blinding Lights' by The Weeknd",
    ]*100
    # benchmark this
    start_time = time.time()
    print(extractor.extract_searches_batch(texts,batch_size=100))
    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")
   