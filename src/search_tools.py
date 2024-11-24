from typing import Optional
from musicbrainz_client import MusicBrainzClient
import asyncio

mb_client = MusicBrainzClient(
    app_name="ifyoulike-dataset",
    app_version="0.1",
    contact="cflowers.flowers@gmail.com"

)
def _get_top_match(results: list) -> Optional[dict]:
    """
    Return only the top match based on score
    
    Args:
        results (list): List of results to filter
        
    Returns:
        Optional[dict]: The highest scoring match, or None if no matches
    """
    if not results:
        return None
    
    return max(results, key=lambda x: x["score"])

def search_artist(artist_name: str) -> Optional[dict]:
    """
    Search for an artist and return only the top match
    
    Args:
        artist_name (str): Name of the artist to search for
    
    Returns:
        Optional[dict]: The best matching artist or None if no match found
    """
    query = f'artist:"{artist_name}"'
    results = mb_client.search_artist(query, limit=3)
    
    if "artists" not in results:
        return None
        
    matches = [{
        "name": a["name"],
        "id": a["id"],
        "score": a.get("score", 0),
        "type": a.get("type", "Unknown"),
        "country": a.get("country", "Unknown"),
        "disambiguation": a.get("disambiguation", "")
    } for a in results["artists"]]
    
    return _get_top_match(matches)

def search_song(song_title: str, artist_name: Optional[str] = None, album_title: Optional[str] = None, limit: int = 3) -> list:
    """
    Search for a song (recording) using MusicBrainz's query syntax
    
    Args:
        song_title (str): Title of the song to search for
        artist_name (str, optional): Artist name to filter by
        album_title (str, optional): Album title to filter by
        limit (int, optional): Maximum number of results. Defaults to 3.
    
    Returns:
        list: List of matching recordings with their details
    """
    query_parts = [f'recording:"{song_title}"']
    
    if artist_name:
        query_parts.append(f'artist:"{artist_name}"')
    
    if album_title:
        query_parts.append(f'release:"{album_title}"')
    
    query = " AND ".join(query_parts)
    results = mb_client.search_recording(query, limit=limit)
    
    if "recordings" not in results:
        return []
    
    matches = [{
        "title": r["title"],
        "id": r["id"],
        "score": r.get("score", 0),
        "artist": r.get("artist-credit", [{}])[0].get("name", "Unknown"),
        "length": r.get("length", "Unknown"),
        "releases": [rel["title"] for rel in r.get("releases", [])],
        "first_release_date": r.get("first-release-date", "Unknown")
    } for r in results["recordings"]]

    return _get_top_match(matches)
def search_album(album_title: str, artist_name: Optional[str] = None, limit: int = 3) -> list:
    """
    Search for an album (release-group) using MusicBrainz's query syntax
    
    Args:
        album_title (str): Title of the album to search for
        artist_name (str, optional): Artist name to filter by
        limit (int, optional): Maximum number of results. Defaults to 3.
    
    Returns:
        list: List of matching albums with their details
    """
    query_parts = [f'releasegroup:"{album_title}"']
    
    if artist_name:
        query_parts.append(f'artist:"{artist_name}"')
    
    query = " AND ".join(query_parts)
    results = mb_client.search_release_group(query, limit=limit)
    
    if "release-groups" not in results:
        return []
    
    matches = [{
        "title": rg["title"],
        "id": rg["id"],
        "score": rg.get("score", 0),
        "artist": rg.get("artist-credit", [{}])[0].get("name", "Unknown"),
        "type": rg.get("primary-type", "Unknown"),
        "first_release_date": rg.get("first-release-date", "Unknown"),
        "disambiguation": rg.get("disambiguation", "")
    } for rg in results["release-groups"]]
    
    return _get_top_match(matches)
# have this take in the 3 search functions:

def test(search_artist, search_song, search_album):
       # Search for an artist
    artist_results = search_artist("Pink Floyd")
    print("\nArtist search results:")
    print(artist_results)

    # Search for a song by title only
    song_results = search_song("Money")
    print("\nSong search results (title only):")
    print(song_results)

    # Search for a song with artist context
    song_results = search_song("Money", artist_name="Pink Floyd")
    print("\nSong search results (with artist):")
    print(song_results)

    # Search for a song with both artist and album context
    song_results = search_song("Money", artist_name="Pink Floyd", album_title="Dark Side of the Moon")
    print("\nSong search results (with artist and album):")
    print(song_results)

    # Search for an album
    album_results = search_album("Dark Side of the Moon")
    print("\nAlbum search results:")
    print(album_results)

    # Search for an album with artist context
    album_results = search_album("Dark Side of the Moon", artist_name="Pink Floyd")
    print("\nAlbum search results (with artist):")
    print(album_results)

# First, let's add async versions of our search functions
async def search_artist_async(artist_name: str) -> Optional[dict]:
    """Async version of search_artist"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, search_artist, artist_name)

async def search_song_async(
    song_title: str, 
    artist_name: Optional[str] = None, 
    album_title: Optional[str] = None
) -> Optional[dict]:
    """Async version of search_song"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, 
        search_song, 
        song_title, 
        artist_name, 
        album_title
    )

async def search_album_async(
    album_title: str, 
    artist_name: Optional[str] = None
) -> Optional[dict]:
    """Async version of search_album"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, 
        search_album, 
        album_title, 
        artist_name
    )

if __name__ == "__main__":
    test(search_artist, search_song, search_album)