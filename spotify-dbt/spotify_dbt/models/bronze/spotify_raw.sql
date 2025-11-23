select DISTINCT
            rank,
            track_id,
            track_name,
            artist_id,
            artist_name,
            album_name,
            popularity,
            duration_ms,
            ingested_at,
            file_name
from {{ source('raw', 'SPOTIFY_TOP_TRACKS_RAW') }}
