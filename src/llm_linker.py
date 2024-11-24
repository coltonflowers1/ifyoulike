from typing import Dict, List, Optional, Tuple
import asyncio
from dotenv import load_dotenv
import os
import json
import openai
from search_tools import search_artist, search_song, search_album
import pandas as pd
from tqdm import tqdm
import re
import requests
from bs4 import BeautifulSoup

from spotify_resolver import extract_and_replace_spotify_links

class MusicEntityExtractor:
    def __init__(self, api_key: str):
        """Initialize with your OpenAI API key"""
        # self.openai_model = "gpt-3.5-turbo"
        self.openai_model = "gpt-4o-mini"
        openai.api_key = api_key

    async def extract_searches(self, text: str) -> dict:
        """Extract music entities and their relationships from text using GPT"""
        prompt = f"""Extract ONLY clearly identifiable music-related entities from this text. Return a JSON object with these keys:
        {{
            "artist_searches": [list of specific artist names only],
            "album_searches": [list of objects with "album_title" and "artist_name" if known],
            "song_searches": [list of objects with "song_title" and "artist_name" if known]
        }}
        
        Guidelines:
        - Only include actual artist names, song titles, and album names
        - Do not include generic descriptions (e.g., "industrial hardcore songs" is not a song title)
        - Do not include post markers like "IIL", "WEWIL", "TOMT"
        - For artist names, resolve common abbreviations (e.g., "5SOS" → "5 Seconds of Summer")
        - If an album or song is mentioned with its artist, always pair them together
        - If unsure about whether something is a music entity, exclude it
        
        Text: {text}
        """

        try:
            client = openai.AsyncOpenAI()
            response = await client.chat.completions.create(
                model=self.openai_model,
                messages=[
                    {"role": "system", "content": "You are a precise music information extraction system. Only extract specific, verifiable music entities."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                response_format={ "type": "json_object" }
            )
            
            return json.loads(response.choices[0].message.content)
            
        except Exception as e:
            print(f"Error extracting searches: {e}")
            return {
                "artist_searches": [],
                "album_searches": [],
                "song_searches": []
            }

async def process_text(text: str) -> Dict[str, Dict[str, List[Optional[dict]]]]:
    """Process text to extract music entities and find them in MusicBrainz"""
    # Initialize extractor
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    extractor = MusicEntityExtractor(api_key)
    
    # Get search parameters
    searches = await extractor.extract_searches(text)
    
    # Find matches in MusicBrainz
    matches = {
        "artists": [],
        "songs": [],
        "albums": []
    }
    
    # Search for artists
    for artist_name in searches["artist_searches"]:
        match = search_artist(artist_name)
        if match:
            matches["artists"].append(match)
    
    # Search for songs with artist/song swap if initial search fails
    for search in searches["song_searches"]:
        match = search_song(
            song_title=search["song_title"],
            artist_name=search.get("artist_name")
        )
        if not match and search.get("artist_name"):  # Try swapping if we have both fields
            # Try with swapped artist and song title
            swap_match = search_song(
                song_title=search["artist_name"],
                artist_name=search["song_title"]
            )
            if swap_match:
                match = swap_match
                # Update the original search object to reflect the swap
                search["song_title"], search["artist_name"] = search["artist_name"], search["song_title"]
                
                # Add the new artist to searches if not already present
                if search["song_title"] not in [a for a in searches["artist_searches"]]:
                    searches["artist_searches"].append(search["song_title"])
                    # Search for the new artist
                    artist_match = search_artist(search["song_title"])
                    if artist_match:
                        matches["artists"].append(artist_match)
        
        if match:
            matches["songs"].append(match)
    
    # Search for albums
    for search in searches["album_searches"]:
        match = search_album(
            album_title=search["album_title"],
            artist_name=search.get("artist_name")
        )
        if match:
            matches["albums"].append(match)
    
    return {
        "searches": searches,
        "matches": matches
    }

async def process_comments_file(csv_path: str) -> List[Dict]:
    """Process a CSV file containing a submission and its comments"""
    # Read the CSV
    df = pd.read_csv(csv_path)
    
    # Initialize extractor
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    extractor = MusicEntityExtractor(api_key)
    
    results = []
    
    # Process all rows with tqdm progress bar
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing entries"):
        try:
            # Combine title and body for submissions, or just use body for comments
            if row['type'] == 'submission':
                text = f"{row['title']} {row['body']}"
            else:
                text = row['body']
            
            # Skip empty or deleted content
            if pd.isna(text) or text.lower() in ['[deleted]', '[removed]', '']:
                continue
                
            # Extract and replace Spotify links
            modified_text, spotify_tracks = extract_and_replace_spotify_links(text)
            
            # Process the text
            result = await process_text(modified_text)
            
            results.append({
                "type": row['type'],
                "id": row['id'],
                "score": row['score'],
                "created_utc": row['created_utc'],
                "author": row['author'],
                "permalink": row['permalink'],
                "parent_id": row['parent_id'] if row['type'] == 'comment' else None,
                "title": row['title'] if row['type'] == 'submission' else None,
                "body": row['body'],
                "spotify_tracks": spotify_tracks,
                "results": result
            })
            
            tqdm.write(f"✓ Processed {row['type']}: {row['id']}")
            
        except Exception as e:
            tqdm.write(f"✗ Error processing {row['type']} {row['id']}: {e}")
    
    return results

async def main():
    # Process a specific comments file
    csv_path = "/Users/coltonflowers/Repos/ifyoulike-dataset/agf8cd_comments.csv"  # Update this path
    results = await process_comments_file(csv_path)
    
    # Save results to JSON file
    output_file = 'music_entity_results_with_comments.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print statistics
    submissions = [r for r in results if r['type'] == 'submission']
    comments = [r for r in results if r['type'] == 'comment']
    
    print("\nProcessing complete!")
    print(f"Total entries processed: {len(results)}")
    print(f"Submissions processed: {len(submissions)}")
    print(f"Comments processed: {len(comments)}")
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    asyncio.run(main())
