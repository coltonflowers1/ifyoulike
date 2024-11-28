from musicBrainz.client import MusicBrainzClient

def main():
    # Initialize the client with your app information
    client = MusicBrainzClient(
        app_name="YourAppName",
        app_version="0.1",
        contact="your@email.com"
    )

    try:
        # Search for an artist
        results = client.search_artist("The Beatles")
        print("Search results:", results)

        # Get specific artist info (using The Beatles' MBID)
        beatles_mbid = "b10bbbfc-cf9e-42e0-be17-e2c3e1d2600d"
        artist_info = client.get_artist(
            beatles_mbid, 
            include=['aliases', 'releases']
        )
        print("Artist info:", artist_info)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main() 