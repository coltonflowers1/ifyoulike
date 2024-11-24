import requests
import time
from typing import Optional, Dict, Any
from urllib.parse import urljoin

class MusicBrainzClient:
    def __init__(self, app_name: str, app_version: str, contact: str):
        self.base_url = "https://musicbrainz.org/ws/2/"
        # Required headers for API etiquette
        self.headers = {
            'User-Agent': f'{app_name}/{app_version} ( {contact} )',
            'Accept': 'application/json'
        }
        # Rate limiting (1 request per second for anonymous users)
        self.last_request_time = 0
        self.min_request_interval = 1.0  # seconds

    def _rate_limit(self) -> None:
        """Implement rate limiting."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last_request)
        self.last_request_time = time.time()

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict:
        """Make a rate-limited request to the MusicBrainz API."""
        self._rate_limit()
        
        url = urljoin(self.base_url, endpoint)
        response = requests.get(url, headers=self.headers, params=params)
        
        response.raise_for_status()  # Raise exception for bad status codes
        return response.json()

    def get_artist(self, mbid: str, include: Optional[list] = None) -> Dict:
        """
        Get artist information by MBID.
        
        Args:
            mbid: MusicBrainz ID for the artist
            include: Optional list of includes (e.g., ['aliases', 'releases'])
        """
        params = {'fmt': 'json'}
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'artist/{mbid}', params)

    def search_artist(self, query: str, limit: int = 10) -> Dict:
        """
        Search for artists by name or query.
        
        Args:
            query: Search query
            limit: Maximum number of results (default: 10)
        """
        params = {
            'query': query,
            'fmt': 'json',
            'limit': limit
        }
        return self._make_request('artist/', params)

    def get_release(self, mbid: str, include: Optional[list] = None) -> Dict:
        """
        Get release information by MBID.
        
        Args:
            mbid: MusicBrainz ID for the release
            include: Optional list of includes (e.g., ['recordings', 'artists'])
        """
        params = {'fmt': 'json'}
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'release/{mbid}', params)

    def search_release(self, query: str, limit: int = 10) -> Dict:
        """
        Search for releases by name or query.
        
        Args:
            query: Search query
            limit: Maximum number of results (default: 10)
        """
        params = {
            'query': query,
            'fmt': 'json',
            'limit': limit
        }
        return self._make_request('release/', params)

    def get_area(self, mbid: str, include: Optional[list] = None) -> Dict:
        """
        Get area information by MBID.
        
        Args:
            mbid: MusicBrainz ID for the area
            include: Optional list of includes (e.g., ['aliases', 'relationships'])
        """
        params = {'fmt': 'json'}
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'area/{mbid}', params)

    def search_area(self, query: str, limit: int = 10) -> Dict:
        """
        Search for areas.
        
        Args:
            query: Search query
            limit: Maximum number of results (default: 10)
        """
        params = {
            'query': query,
            'fmt': 'json',
            'limit': limit
        }
        return self._make_request('area/', params)

    def get_event(self, mbid: str, include: Optional[list] = None) -> Dict:
        """
        Get event information by MBID.
        
        Args:
            mbid: MusicBrainz ID for the event
            include: Optional list of includes (e.g., ['artist-rels', 'place-rels'])
        """
        params = {'fmt': 'json'}
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'event/{mbid}', params)

    def search_event(self, query: str, limit: int = 10) -> Dict:
        """
        Search for events.
        
        Args:
            query: Search query
            limit: Maximum number of results (default: 10)
        """
        params = {
            'query': query,
            'fmt': 'json',
            'limit': limit
        }
        return self._make_request('event/', params)

    def get_instrument(self, mbid: str, include: Optional[list] = None) -> Dict:
        """
        Get instrument information by MBID.
        
        Args:
            mbid: MusicBrainz ID for the instrument
            include: Optional list of includes (e.g., ['aliases', 'tags'])
        """
        params = {'fmt': 'json'}
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'instrument/{mbid}', params)

    def search_instrument(self, query: str, limit: int = 10) -> Dict:
        """
        Search for instruments.
        
        Args:
            query: Search query
            limit: Maximum number of results (default: 10)
        """
        params = {
            'query': query,
            'fmt': 'json',
            'limit': limit
        }
        return self._make_request('instrument/', params)

    def browse_artists(self, entity: str, mbid: str, limit: int = 25, offset: int = 0, include: Optional[list] = None) -> Dict:
        """
        Browse artists by their relationships to other entities.
        
        Args:
            entity: The type of entity to browse by (e.g., 'release', 'recording', 'label')
            mbid: The MBID of the entity to browse by
            limit: Maximum number of results (default: 25)
            offset: Search offset for pagination
            include: Optional list of includes
        """
        params = {
            'fmt': 'json',
            'limit': limit,
            'offset': offset
        }
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'artist?{entity}={mbid}', params)

    def get_recording(self, mbid: str, include: Optional[list] = None) -> Dict:
        """
        Get recording information by MBID.
        
        Args:
            mbid: MusicBrainz ID for the recording
            include: Optional list of includes (e.g., ['artists', 'releases', 'isrcs'])
        """
        params = {'fmt': 'json'}
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'recording/{mbid}', params)

    def search_recording(self, query: str, limit: int = 10) -> Dict:
        """
        Search for recordings.
        
        Args:
            query: Search query
            limit: Maximum number of results (default: 10)
        """
        params = {
            'query': query,
            'fmt': 'json',
            'limit': limit
        }
        return self._make_request('recording/', params)

    def get_label(self, mbid: str, include: Optional[list] = None) -> Dict:
        """
        Get label information by MBID.
        
        Args:
            mbid: MusicBrainz ID for the label
            include: Optional list of includes (e.g., ['aliases', 'releases', 'ratings'])
        """
        params = {'fmt': 'json'}
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'label/{mbid}', params)

    def search_label(self, query: str, limit: int = 10, offset: int = 0) -> Dict:
        """
        Search for labels.
        
        Args:
            query: Search query
            limit: Maximum number of results (default: 10)
            offset: Search offset for pagination
        """
        params = {
            'query': query,
            'fmt': 'json',
            'limit': limit,
            'offset': offset
        }
        return self._make_request('label/', params)

    def get_place(self, mbid: str, include: Optional[list] = None) -> Dict:
        """
        Get place information by MBID.
        
        Args:
            mbid: MusicBrainz ID for the place
            include: Optional list of includes (e.g., ['aliases', 'area-rels'])
        """
        params = {'fmt': 'json'}
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'place/{mbid}', params)

    def search_place(self, query: str, limit: int = 10, offset: int = 0) -> Dict:
        """
        Search for places.
        
        Args:
            query: Search query
            limit: Maximum number of results (default: 10)
            offset: Search offset for pagination
        """
        params = {
            'query': query,
            'fmt': 'json',
            'limit': limit,
            'offset': offset
        }
        return self._make_request('place/', params)

    def get_release_group(self, mbid: str, include: Optional[list] = None) -> Dict:
        """
        Get release group information by MBID.
        
        Args:
            mbid: MusicBrainz ID for the release group
            include: Optional list of includes (e.g., ['artists', 'releases', 'tags'])
        """
        params = {'fmt': 'json'}
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'release-group/{mbid}', params)

    def search_release_group(self, query: str, limit: int = 10, offset: int = 0) -> Dict:
        """
        Search for release groups.
        
        Args:
            query: Search query
            limit: Maximum number of results (default: 10)
            offset: Search offset for pagination
        """
        params = {
            'query': query,
            'fmt': 'json',
            'limit': limit,
            'offset': offset
        }
        return self._make_request('release-group/', params)

    def browse_release_groups(self, entity: str, mbid: str, limit: int = 25, offset: int = 0, include: Optional[list] = None) -> Dict:
        """
        Browse release groups by their relationships to other entities.
        
        Args:
            entity: The type of entity to browse by (e.g., 'artist', 'label', 'collection')
            mbid: The MBID of the entity to browse by
            limit: Maximum number of results (default: 25)
            offset: Search offset for pagination
            include: Optional list of includes
        """
        params = {
            'fmt': 'json',
            'limit': limit,
            'offset': offset
        }
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'release-group?{entity}={mbid}', params)

    def browse_recordings(self, entity: str, mbid: str, limit: int = 25, offset: int = 0, include: Optional[list] = None) -> Dict:
        """
        Browse recordings by their relationships to other entities.
        
        Args:
            entity: The type of entity to browse by (e.g., 'artist', 'release', 'work')
            mbid: The MBID of the entity to browse by
            limit: Maximum number of results (default: 25)
            offset: Search offset for pagination
            include: Optional list of includes
        """
        params = {
            'fmt': 'json',
            'limit': limit,
            'offset': offset
        }
        if include:
            params['inc'] = '+'.join(include)
        
        return self._make_request(f'recording?{entity}={mbid}', params)