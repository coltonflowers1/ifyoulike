
from typing import Dict, List, Tuple
from musicBrainz.search_tools import search_artist, search_song, search_album
from llm_linker import SearchResults

def execute_searches(searches:SearchResults) -> dict:
    """
    Takes a dictionary of searches and returns a dictionary of matches.
    """
    # Execute song searches first to identify any misidentified artists
    song_matches, additional_artists_from_swapped_songs, songs_misidentified_as_artists = execute_song_searches(searches.song_searches)
    
    song_matches,additional_artists_from_swapped_songs, songs_misidentified_as_artists = execute_song_searches(searches.song_searches)

    album_matches, additional_artists_from_swapped_albums, albums_misidentified_as_artists = execute_album_searches(searches.album_searches)

    # Search for artists
    artists_to_search = set(additional_artists_from_swapped_songs + additional_artists_from_swapped_albums + searches.artist_searches) - set(songs_misidentified_as_artists + albums_misidentified_as_artists)

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

def execute_album_searches(album_searches) -> Tuple[List[Dict], List[str], List[str]]:
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
        match = search_album(
            album_title=search["album_title"],
            artist_name=search.get("artist_name")
        )
        if not match and search.get("artist_name"):  # Try swapping if we have both fields
            # Try with swapped artist and album title
            swap_match = search_album(
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
    

def execute_song_searches(song_searches) -> Tuple[List[Dict], List[str], List[str]]:
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
        match =search_song(
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
                additional_artists.append(search["song_title"])
                misidentified_songs.append(search["artist_name"])
        
        if match:
            song_matches.append(match)

    return song_matches, additional_artists, misidentified_songs