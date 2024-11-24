from pathlib import Path
import click
import asyncio
from main import MusicRecommendationPipeline

@click.command()
@click.argument('submission_id')
@click.argument('submissions-path')
@click.argument('comments-path')
@click.option('--output-dir',
              default='data/output',
              help='Directory for output files',)
@click.option('--artist-limit',
              default=0,
              help='Number of tracks to include from a particular artist')
@click.option('--album-limit',
              default=0,
              help='Number of tracks to include from a particular album')


def process_submission(submission_id: str,
                      submissions_path: str,
                      comments_path: str,
                      output_dir: str,
                      artist_limit: int,
                      album_limit: int):
    """Process a Reddit submission and create a Spotify playlist."""
    
    async def run():
        pipeline = MusicRecommendationPipeline()
        pipeline.submissions_path = submissions_path
        pipeline.comments_path = comments_path
        pipeline.output_dir = Path(output_dir)
        pipeline.output_dir.mkdir(parents=True, exist_ok=True)
        
        playlist_url = await pipeline.process_submission(submission_id,artist_limit,album_limit)
        
        if playlist_url:
            click.echo(f"\nSuccess! Playlist created:")
            click.echo(f"Playlist URL: {playlist_url}")
        else:
            click.echo(f"\nFailed to process submission {submission_id}")
    
    asyncio.run(run())

if __name__ == '__main__':
    process_submission() 