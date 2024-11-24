# IfYouLike Playlist Generator

A tool that processes Reddit's r/ifyoulikeblank submissions to generate personalized Spotify playlists based on music recommendations.

## Features

- Processes Reddit submissions and comments from r/ifyoulikeblank
- Extracts music entities (artists, albums, songs) using LLM
- Automatically creates Spotify playlists from the recommendations
- Configurable limits for artists and albums
- CLI support

## Prerequisites

- Python 3.13+
- Poetry for dependency management
- Reddit dataset files
- OpenAI API key
- Spotify API credentials

## Installation

1. Clone the repository
2. Install dependencies using Poetry: `poetry install`
3. Set up environment variables for OpenAI and Spotify API keys in `.env` (see `.env.template`)

4. Run the CLI tool to process submissions and generate playlists: `poetry run ifyoulike [options]`

## Dependencies

- openai (^1.55.0)
- requests (^2.32.3)
- zstandard (^0.23.0)
- python-dotenv (^1.0.1)
- pandas (^2.2.3)
- spotipy (^2.24.0)
- bs4 (^0.0.2)
- click (^8.1.7)
