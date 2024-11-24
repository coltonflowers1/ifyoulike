import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from typing import Optional
import os
# source .env
from dotenv import load_dotenv

from search_tools import _get_top_match, test

load_dotenv()


# Replace with your Spotify API credentials
spotify_client = spotipy.Spotify(
    client_credentials_manager=SpotifyClientCredentials(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")
    )
)

def search_artist(artist_name: str) -> Optional[dict]:
    """
    Search for an artist using Spotify API
    
    Args:
        artist_name (str): Name of the artist to search for
    
    Returns:
        Optional[dict]: The best matching artist or None if no match found
    """
    results = spotify_client.search(q=artist_name, type='artist', limit=3)
    
    if not results['artists']['items']:
        return None
        
    matches = [{
        "name": a["name"],
        "id": a["id"],
        "score": 100 if i == 0 else 90 - (i * 10),  # Approximate scoring since Spotify doesn't provide scores
        "type": a["type"],
        "popularity": a["popularity"],
        "genres": a.get("genres", []),
        "followers": a["followers"]["total"]
    } for i, a in enumerate(results['artists']['items'])]
    
    return _get_top_match(matches)

def search_song(song_title: str, artist_name: Optional[str] = None, album_title: Optional[str] = None, limit: int = 3) -> Optional[dict]:
    """
    Search for a song using Spotify API
    """
    query = song_title
    if artist_name:
        query += f" artist:{artist_name}"
    if album_title:
        query += f" album:{album_title}"
        
    results = spotify_client.search(q=query, type='track', limit=limit)
    
    if not results['tracks']['items']:
        return None
    
    matches = [{
        "title": t["name"],
        "id": t["id"],
        "score": 100 if i == 0 else 90 - (i * 10),
        "artist": t["artists"][0]["name"],
        "length": t["duration_ms"],
        "album": t["album"]["name"],
        "popularity": t["popularity"],
        "preview_url": t.get("preview_url"),
        "external_url": t["external_urls"]["spotify"]
    } for i, t in enumerate(results['tracks']['items'])]
    
    return _get_top_match(matches)

def search_album(album_title: str, artist_name: Optional[str] = None, limit: int = 3) -> Optional[dict]:
    """
    Search for an album using Spotify API
    """
    query = album_title
    if artist_name:
        query += f" artist:{artist_name}"
        
    results = spotify_client.search(q=query, type='album', limit=limit)
    
    if not results['albums']['items']:
        return None
    
    matches = [{
        "title": a["name"],
        "id": a["id"],
        "score": 100 if i == 0 else 90 - (i * 10),
        "artist": a["artists"][0]["name"],
        "type": a["album_type"],
        "release_date": a["release_date"],
        "total_tracks": a["total_tracks"],
        "external_url": a["external_urls"]["spotify"]
    } for i, a in enumerate(results['albums']['items'])]
    
    return _get_top_match(matches)

if __name__ == "__main__":
    test(search_artist, search_song, search_album)