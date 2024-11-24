import re
from typing import List, Dict, Tuple
import requests
from bs4 import BeautifulSoup

def extract_track_ids(text: str) -> List[str]:
    """Extract Spotify track IDs from URLs"""
    track_pattern = r'https://open\.spotify\.com/track/([a-zA-Z0-9]{22})'
    return re.findall(track_pattern, text)

def get_track_info(track_id: str) -> Dict[str, str]:
    """Scrape track information from Spotify's public page"""
    url = f"https://open.spotify.com/track/{track_id}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        return {
            'track_id': track_id,
            'url': url,
            'raw_title': soup.title.string if soup.title else None
        }
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return {'track_id': track_id, 'error': str(e)}

def extract_and_replace_spotify_links(text: str) -> Tuple[str, List[Dict[str, str]]]:
    """
    Extract Spotify links from text and replace them with their titles.
    Returns: (modified_text, list of track info dictionaries)
    """
    # Track pattern for markdown links containing Spotify URLs
    pattern = r'\[(.*?)\]\((https://open\.spotify\.com/track/[a-zA-Z0-9]{22})\)'
    
    tracks_info = []
    modified_text = text
    
    # Find all Spotify links
    matches = list(re.finditer(pattern, text))
    
    # Process each match in reverse to avoid messing up string indices
    for match in reversed(matches):
        markdown_text = match.group(1)
        spotify_url = match.group(2)
        track_id = extract_track_ids(spotify_url)[0]
        
        track_info = get_track_info(track_id)
        tracks_info.append({
            'markdown_text': markdown_text,
            'spotify_url': spotify_url,
            'raw_title': track_info.get('raw_title', markdown_text)
        })
        
        # Replace the markdown link with the raw title or markdown text if scraping failed
        start, end = match.span()
        replacement_text = track_info.get('raw_title', markdown_text)
        modified_text = modified_text[:start] + replacement_text + modified_text[end:]
    
    return modified_text, tracks_info

def main():
    # Test example with markdown links
    test_text = """
    [Looking Out For You - Joy Again](https://open.spotify.com/track/3jfZ9M23l0L7RxzYMTgBTv)
    [What Ever Happened - The Strokes](https://open.spotify.com/track/78Gzxi27GuNHTfkn2BylG4)
    [New Flesh](https://open.spotify.com/track/6HJxxqHWMdidwTVZmZWeHU)
    """

    modified_text, tracks = extract_and_replace_spotify_links(test_text)
    print("\nOriginal text:")
    print(test_text)
    print("\nModified text:")
    print(modified_text)
    print("\nTracks info:")
    for track in tracks:
        print(track)

if __name__ == "__main__":
    main()
