
import json
import os
import time
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Optional
import logging
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from llm_linker import MusicEntityExtractor
from playlist_generator import SpotifyPlaylistCreator
from parse import process_submission_and_comments, save_comments_to_csv
from search_executor import execute_searches
from spotify_resolver import extract_and_replace_spotify_links

class MusicRecommendationPipeline:
    def __init__(self,log_level:int=logging.INFO):
        load_dotenv()
        self.setup_logging()
        
        # Configure paths
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)
        
        # Reddit data paths (update these to your paths)
        self.submissions_path = "/Users/coltonflowers/Repos/ifyoulike/data/reddit/subreddits/ifyoulikeblank_submissions.zst"
        self.comments_path = "/Users/coltonflowers/Repos/ifyoulike/data/reddit/subreddits/ifyoulikeblank_comments.zst"
        
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
    
    def process_submission(self, submission_id: str,artist_limit=2,album_limit=2) -> Optional[str]:
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
            results = process_comments_file(str(csv_path))
            
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
            print(json_path)
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

def process_comments_file(csv_path: str, batch_size: int = 100) -> List[Dict]:
    """Process a CSV file containing a submission and its comments"""
    df = pd.read_csv(csv_path)
    load_dotenv()
    results = []
    extractor = MusicEntityExtractor(os.getenv("OPENAI_API_KEY"))

    # Process in batches
    texts = []
    metadata = []

    for _, row in df.iterrows():
        if row['type'] == 'submission':
            text = f"{row['title']} {row['body']}"
        else:
            text = row['body']

        if not pd.isna(text) and text.lower() not in ['[deleted]', '[removed]', '']:
            modified_text, spotify_tracks = extract_and_replace_spotify_links(text)
            texts.append(modified_text)
            metadata.append({
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
        logging.info("Extracting music entities...")
        search_extraction_results = extractor.extract_searches_batch(texts,batch_size=batch_size)
        logging.info("Executing searches...")
        logging.info(f"Extracted {len(search_extraction_results)} searches from {len(texts)} comments.")

        logging.info(f"Executing {len(search_extraction_results)} searches")
        execution_results = [execute_searches(searches) for searches in tqdm(search_extraction_results) ]
        
        # Match results back to metadata (maintaining original order)
        for metadata, result in zip(metadata, execution_results):
            results.append({**metadata, "results": result})
            tqdm.write(f"✓ Processed {metadata['type']}: {metadata['id']}")

    return results


def main():
    # Example submission IDs
    submission_ids = [
        "agf8cd",  # Replace with your submission IDs
    ]
    # benchmark this
    start_time = time.time()        
    pipeline = MusicRecommendationPipeline()
    
    for submission_id in submission_ids:
        playlist_url = pipeline.process_submission(submission_id)
        
        if playlist_url:
            print(f"\nSuccess! Playlist created for submission {submission_id}")
            print(f"Playlist URL: {playlist_url}")
        else:
            print(f"\nFailed to process submission {submission_id}")
    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()