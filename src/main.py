import asyncio
import json
import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime

from playlist_generator import SpotifyPlaylistCreator
from parse import process_submission_and_comments, save_comments_to_csv
from llm_linker import process_comments_file

class MusicRecommendationPipeline:
    def __init__(self,log_level:int=logging.INFO):
        load_dotenv()
        self.setup_logging()
        
        # Configure paths
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)
        
        # Reddit data paths (update these to your paths)
        self.submissions_path = "/Users/coltonflowers/Repos/ifyoulike-dataset/reddit/subreddits/ifyoulikeblank_submissions.zst"
        self.comments_path = "/Users/coltonflowers/Repos/ifyoulike-dataset/reddit/subreddits/ifyoulikeblank_comments.zst"
        
    def setup_logging(self):
        """Configure logging"""
        self.log_dir = Path("logs")
        self.log_dir.mkdir(exist_ok=True)
        self.log_file = self.log_dir / f'pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(self.log_file)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    async def process_submission(self, submission_id: str,artist_limit=2,album_limit=2) -> Optional[str]:
        """
        Process a submission from start to finish
        
        Args:
            submission_id: Reddit submission ID
            
        Returns:
            Optional[str]: URL of created Spotify playlist
        """
        try:
            # Step 1: Extract submission and comments
            self.logger.info(f"Processing submission: {submission_id}")
            result = process_submission_and_comments(
                submission_id,
                self.submissions_path,
                self.comments_path
            )
            
            if not result:
                self.logger.error(f"No data found for submission {submission_id}")
                return None
                
            # Save to CSV
            csv_path = self.output_dir / f"{submission_id}_comments.csv"
            save_comments_to_csv(result, csv_path)
            self.logger.info(f"Saved comments to {csv_path}")
            
            # Step 2: Extract music entities
            self.logger.info("Extracting music entities...")
            json_path = self.output_dir / f"{submission_id}_comments.json"
            results = await process_comments_file(str(csv_path))
            
            #save results to json
            with open(json_path, 'w') as f:
                json.dump(results, f,indent=2)
                
            if not results:
                self.logger.error("No music entities found")
                return None
                
            # Step 3: Create Spotify playlist
            self.logger.info("Creating Spotify playlist...")
            creator = SpotifyPlaylistCreator()
            
            # Get submission title for playlist name
            submission_title = result['submission']['title']
            playlist_name = f"Reddit: {submission_title[:50]}..."  # Truncate if too long
            
            playlist_url = creator.create_playlist_from_results(
                json_path=str(json_path),
                playlist_name=playlist_name,
                playlist_description=f"Generated from Reddit submission: https://www.reddit.com/r/ifyoulikeblank/comments/{submission_id}",
                sample_top_tracks=True,
                artist_limit=artist_limit,
                album_limit=album_limit,
                remove_duplicates=True
            )
            
            if playlist_url:
                self.logger.info(f"Successfully created playlist: {playlist_url}")
                return playlist_url
            else:
                self.logger.error("Failed to create playlist")
                return None
                
        except Exception as e:
            self.logger.error(f"Error processing submission: {e}", exc_info=True)
            return None

async def main():
    # Example submission IDs
    submission_ids = [
        "agf8cd",  # Replace with your submission IDs
    ]
    
    pipeline = MusicRecommendationPipeline()
    
    for submission_id in submission_ids:
        playlist_url = await pipeline.process_submission(submission_id)
        
        if playlist_url:
            print(f"\nSuccess! Playlist created for submission {submission_id}")
            print(f"Playlist URL: {playlist_url}")
        else:
            print(f"\nFailed to process submission {submission_id}")
            
if __name__ == "__main__":
    asyncio.run(main()) 