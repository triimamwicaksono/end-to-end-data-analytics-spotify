select distinct
    track_id,
    track_name,
    album_name,
    duration_ms,
    duration_ms / 1000 as duration_seconds
from {{ ref('spotify_raw') }}
where track_id is not null
