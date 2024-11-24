import json
import random
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from typing import List, Dict, Set
import logging
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()

class SpotifyPlaylistCreator:
    def __init__(self):
        """Initialize Spotify client with necessary permissions"""
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=os.getenv("SPOTIFY_CLIENT_ID"),
            client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
            scope="playlist-modify-public playlist-modify-private",
            redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI")
        ))
        self.user_id = self.sp.current_user()['id']
        
    def _get_top_tracks_from_artist(self, artist_id: str, limit: int = 3) -> List[str]:
        """Get top tracks from an artist"""
        try:
            results = self.sp.artist_top_tracks(artist_id)
            tracks = results['tracks'][:limit]
            return [track['id'] for track in tracks]
        except Exception as e:
            logging.warning(f"Error getting top tracks for artist {artist_id}: {e}")
            return []

    def _get_popular_tracks_from_album(self, album_id: str, limit: int = 2) -> List[str]:
        """Get most popular tracks from an album"""
        try:
            # Get all tracks from album
            tracks = []
            results = self.sp.album_tracks(album_id)
            tracks.extend(results['items'])
            
            # Get popularity scores for tracks
            track_ids = [track['id'] for track in tracks]
            popular_tracks = []
            
            # Spotify API limits: get popularity scores in batches
            for i in range(0, len(track_ids), 50):
                batch = track_ids[i:i+50]
                track_info = self.sp.tracks(batch)['tracks']
                popular_tracks.extend(sorted(
                    track_info,
                    key=lambda x: x['popularity'],
                    reverse=True
                )[:limit])
            
            return [track['id'] for track in popular_tracks[:limit]]
            
        except Exception as e:
            logging.warning(f"Error getting tracks from album {album_id}: {e}")
            return []

    def _extract_track_ids(self, results: List[Dict], 
                          sample_top_tracks: bool = True,
                          artist_limit: int = 3,
                          album_limit: int = 2) -> Set[str]:
        """
        Extract all unique Spotify track IDs from results
        
        Args:
            results: Analysis results
            sample_top_tracks: Whether to include top tracks from artists/albums
            artist_limit: Number of top tracks to include per artist
            album_limit: Number of popular tracks to include per album
        """
        track_ids = set()
        
        for entry in tqdm(results, desc="Processing entries"):
            # Direct Spotify tracks
            if 'spotify_tracks' in entry:
                for track in entry['spotify_tracks']:
                    if 'id' in track:
                        track_ids.add(track['id'])
            
            if 'results' in entry and 'matches' in entry['results']:
                matches = entry['results']['matches']
                
                # Process matched songs
                if 'songs' in matches:
                    for song in matches['songs']:
                        try:
                            query = f"track:{song['title']} artist:{song['artist']}"
                            search_result = self.sp.search(query, type='track', limit=1)
                            
                            if search_result['tracks']['items']:
                                track_ids.add(search_result['tracks']['items'][0]['id'])
                        except Exception as e:
                            logging.warning(f"Error searching for track {song['title']}: {e}")
                
                if sample_top_tracks:
                    # Sample from matched artists
                    if 'artists' in matches:
                        for artist in matches['artists']:
                            try:
                                # Search for artist on Spotify
                                query = f"artist:{artist['name']}"
                                search_result = self.sp.search(query, type='artist', limit=1)
                                
                                if search_result['artists']['items']:
                                    artist_id = search_result['artists']['items'][0]['id']
                                    top_tracks = self._get_top_tracks_from_artist(
                                        artist_id, 
                                        limit=artist_limit
                                    )
                                    track_ids.update(top_tracks)
                            except Exception as e:
                                logging.warning(f"Error processing artist {artist['name']}: {e}")
                    
                    # Sample from matched albums
                    if 'albums' in matches:
                        for album in matches['albums']:
                            try:
                                # Search for album on Spotify
                                query = f"album:{album['title']} artist:{album['artist']}"
                                search_result = self.sp.search(query, type='album', limit=1)
                                
                                if search_result['albums']['items']:
                                    album_id = search_result['albums']['items'][0]['id']
                                    popular_tracks = self._get_popular_tracks_from_album(
                                        album_id,
                                        limit=album_limit
                                    )
                                    track_ids.update(popular_tracks)
                            except Exception as e:
                                logging.warning(f"Error processing album {album['title']}: {e}")
        
        return track_ids

    def create_playlist_from_results(self, 
                                   json_path: str, 
                                   playlist_name: str,
                                   playlist_description: str = "",
                                   sample_top_tracks: bool = True,
                                   artist_limit: int = 3,
                                   album_limit: int = 2,
                                   remove_duplicates: bool = True) -> str:
        """
        Create a Spotify playlist from analysis results
        
        Args:
            json_path: Path to the JSON results file
            playlist_name: Name for the new playlist
            playlist_description: Description for the playlist
            sample_top_tracks: Whether to include top tracks from artists/albums
            artist_limit: Number of top tracks to include per artist
            album_limit: Number of popular tracks to include per album
            remove_duplicates: Whether to check for and remove duplicate tracks
            
        Returns:
            str: Playlist URL
        """
        # Load results
        with open(json_path, 'r') as f:
            results = json.load(f)
            
        # Get all track IDs
        track_ids = self._extract_track_ids(
            results,
            sample_top_tracks=sample_top_tracks,
            artist_limit=artist_limit,
            album_limit=album_limit
        )
        
        if not track_ids:
            logging.warning("No valid tracks found in results")
            return None
            
        if remove_duplicates:
            # Remove duplicates by normalized name + artist
            unique_tracks = {}
            for track_id in track_ids:
                try:
                    track_info = self.sp.track(track_id)
                    # Create a normalized key for comparison
                    key = (
                        track_info['name'].lower().strip(),
                        track_info['artists'][0]['name'].lower().strip()
                    )
                    # Keep track with highest popularity
                    if key not in unique_tracks or track_info['popularity'] > unique_tracks[key]['popularity']:
                        unique_tracks[key] = {
                            'id': track_id,
                            'popularity': track_info['popularity']
                        }
                except Exception as e:
                    logging.warning(f"Error getting track info for {track_id}: {e}")
                    continue
            
            # Update track list with deduplicated tracks
            track_ids = [track['id'] for track in unique_tracks.values()]
        
        # Randomize track order
        random.shuffle(track_ids)
        
        # Create playlist
        playlist = self.sp.user_playlist_create(
            user=self.user_id,
            name=playlist_name,
            description=playlist_description,
            public=True
        )
        
        # Add tracks in batches
        for i in tqdm(range(0, len(track_ids), 100), desc="Adding tracks"):
            batch = track_ids[i:i + 100]
            self.sp.playlist_add_items(playlist['id'], batch)
            
        print(f"Added {len(track_ids)} unique tracks to playlist")
        return playlist['external_urls']['spotify']

    def deduplicate_existing_playlist(self, playlist_id: str) -> None:
        """
        Remove duplicate tracks from an existing playlist
        """
        try:
            # Get all tracks
            tracks = []
            results = self.sp.playlist_tracks(playlist_id)
            tracks.extend(results['items'])
            while results['next']:
                results = self.sp.next(results)
                tracks.extend(results['items'])
            
            # Find duplicates using normalized names
            seen = {}
            duplicates = []
            
            for i, item in enumerate(tracks):
                if not item['track']:  # Skip any None/deleted tracks
                    continue
                    
                track = item['track']
                key = (
                    track['name'].lower().strip(),
                    track['artists'][0]['name'].lower().strip()
                )
                
                if key in seen:
                    # Keep track with highest popularity
                    if track['popularity'] > seen[key]['popularity']:
                        duplicates.append(seen[key]['position'])
                        seen[key] = {
                            'position': i,
                            'popularity': track['popularity']
                        }
                    else:
                        duplicates.append(i)
                else:
                    seen[key] = {
                        'position': i,
                        'popularity': track['popularity']
                    }
            
            # Remove duplicates if found
            if duplicates:
                print(f"Removing {len(duplicates)} duplicate tracks...")
                # Remove in batches to handle API limits
                for i in range(0, len(duplicates), 100):
                    batch = duplicates[i:i+100]
                    self.sp.playlist_remove_specific_occurrences_of_items(
                        playlist_id,
                        [{"uri": tracks[pos]['track']['uri'], "positions": [pos]} 
                         for pos in batch]
                    )
                print("Duplicates removed successfully")
            else:
                print("No duplicates found")
                
        except Exception as e:
            logging.error(f"Error deduplicating playlist: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    creator = SpotifyPlaylistCreator()
    
    # Create new playlist with deduplication
    playlist_url = creator.create_playlist_from_results(
        json_path='music_entity_results_with_comments.json',
        playlist_name='Reddit Music Recommendations (Deduplicated)',
        playlist_description='Automatically generated playlist from Reddit music recommendations',
        sample_top_tracks=True,
        artist_limit=0,
        album_limit=0,
        remove_duplicates=True  # Enable deduplication
    )
    
    # Or deduplicate an existing playlist
    # creator.deduplicate_existing_playlist('spotify:playlist:your_playlist_id')
    
    if playlist_url:
        print(f"\nPlaylist created successfully!")
        print(f"URL: {playlist_url}")
    else:
        print("\nFailed to create playlist - no tracks found")

if __name__ == "__main__":
    main()