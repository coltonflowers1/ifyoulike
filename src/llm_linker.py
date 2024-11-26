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

    async def extract_searches_batch(self, texts: List[str]) -> List[dict]:
        """Extract music entities from multiple texts concurrently"""
        async def process_single_text(text: str) -> dict:
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

        # Process all texts concurrently
        tasks = [process_single_text(text) for text in texts]
        results = await asyncio.gather(*tasks)
        return results

async def process_text(text: str) -> Dict[str, Dict[str, List[Optional[dict]]]]:
    """Process text to extract music entities and find them in MusicBrainz"""
    # Initialize extractor
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    extractor = MusicEntityExtractor(api_key)
    
    # Get search parameters
    searches = await extractor.extract_searches(text)

    # Search for songs with artist/song swap if initial search fails
    return extract_searches(searches)

def extract_searches(searches):
    """
    Takes a dictionary of searches and returns a dictionary of matches.
    """
    
    song_matches,additional_artists_from_swapped_songs, songs_misidentified_as_artists = execute_song_searches(searches["song_searches"])

    album_matches, additional_artists_from_swapped_albums, albums_misidentified_as_artists = execute_album_searches(searches["album_searches"])

    # Search for artists
    artists_to_search = set(additional_artists_from_swapped_songs + additional_artists_from_swapped_albums + searches["artist_searches"]) - set(songs_misidentified_as_artists + albums_misidentified_as_artists)
    artist_matches = execute_artist_searches(artists_to_search)
    
    
    return {
        "matches": {
            "artists": artist_matches,
            "songs": song_matches,
            "albums": album_matches
        }
    }

def execute_artist_searches(artists_to_search):
    artist_matches = []
    for artist_name in artists_to_search:
        match = search_artist(artist_name)
        if match:
            artist_matches.append(match)
    return artist_matches

async def execute_album_searches(album_searches) -> Tuple[List[Dict], List[str], List[str]]:
    """
    Execute album searches and return matches and additionI al entities to search for.
    If an album is not found, try swapping the artist and album title.
    Returns:
        Tuple containing:
        - List of album matches
        - List of additional artists to search
        - List of albums that were initially misidentified as artists
    """
    album_matches = []
    additional_artists = []
    misidentified_albums = []
    for search in album_searches:
        match = await search_album(
            album_title=search["album_title"],
            artist_name=search.get("artist_name")
        )
        if not match and search.get("artist_name"):  # Try swapping if we have both fields
            # Try with swapped artist and album title
            swap_match = await search_album(
                album_title=search["artist_name"],
                artist_name=search["album_title"]
            )
            if swap_match:
                match = swap_match
                # Update the original search object to reflect the swap
                search["album_title"], search["artist_name"] = search["artist_name"], search["album_title"]
                
                additional_artists.append(search["album_title"])
                misidentified_albums.append(search["artist_name"])
        
        if match:
            album_matches.append(match)

    return album_matches, additional_artists, misidentified_albums
    

async def execute_song_searches(song_searches) -> Tuple[List[Dict], List[str], List[str]]:
    """
    Apply song searches to the matches.
    If a song is not found, try swapping the artist and song title.
    Returns:
        Tuple containing:
        - List of song matches
        - List of additional artists to search
        - List of songs that were initially misidentified as artists
    """
    song_matches = []
    additional_artists = []
    misidentified_songs = []
    for search in song_searches:
        match = await search_song(
            song_title=search["song_title"],
            artist_name=search.get("artist_name")
        )
        if not match and search.get("artist_name"):  # Try swapping if we have both fields
            # Try with swapped artist and song title
            swap_match = await search_song(
                song_title=search["artist_name"],
                artist_name=search["song_title"]
            )
            if swap_match:
                match = swap_match
                # Update the original search object to reflect the swap
                search["song_title"], search["artist_name"] = search["artist_name"], search["song_title"]
                additional_artists.append(search["song_title"])
                misidentified_songs.append(search["artist_name"])
        
        if match:
            song_matches.append(match)

    return song_matches, additional_artists, misidentified_songs

async def process_comments_file(csv_path: str, batch_size: int = 10) -> List[Dict]:
    """Process a CSV file containing a submission and its comments"""
    df = pd.read_csv(csv_path)
    load_dotenv()
    
    results = []
    extractor = MusicEntityExtractor(os.getenv("OPENAI_API_KEY"))
    
    # Process in batches
    for i in tqdm(range(0, len(df), batch_size), desc="Processing batches"):
        batch = df.iloc[i:i+batch_size]
        texts = []
        batch_metadata = []
        
        for _, row in batch.iterrows():
            if row['type'] == 'submission':
                text = f"{row['title']} {row['body']}"
            else:
                text = row['body']
            
            if not pd.isna(text) and text.lower() not in ['[deleted]', '[removed]', '']:
                modified_text, spotify_tracks = extract_and_replace_spotify_links(text)
                texts.append(modified_text)
                batch_metadata.append({
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
                })
        
        if texts:
            # Start all extractions concurrently
            extraction_tasks = [extractor.extract_searches(text) for text in texts]
            
            # Process extractions as they complete
            batch_results = []
            for extraction_task in asyncio.as_completed(extraction_tasks):
                searches = await extraction_task
                
                # Execute searches for this extraction immediately
                song_matches, additional_artists_from_songs, songs_misidentified_as_artists = execute_song_searches(searches["song_searches"])
                album_matches, additional_artists_from_albums, albums_misidentified_as_artists = execute_album_searches(searches["album_searches"])
                
                # Combine and deduplicate artists to search
                artists_to_search = set(
                    additional_artists_from_songs + 
                    additional_artists_from_albums + 
                    searches["artist_searches"]
                ) - set(songs_misidentified_as_artists + albums_misidentified_as_artists)
                
                artist_matches = execute_artist_searches(artists_to_search)
                
                batch_results.append({
                    "searches": searches,
                    "matches": {
                        "artists": artist_matches,
                        "songs": song_matches,
                        "albums": album_matches
                    }
                })
            
            # Match results back to metadata (maintaining original order)
            for metadata, result in zip(batch_metadata, batch_results):
                results.append({**metadata, "results": result})
                tqdm.write(f"✓ Processed {metadata['type']}: {metadata['id']}")
    
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
