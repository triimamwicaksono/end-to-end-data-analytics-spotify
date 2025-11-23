select distinct
    track_id,
    track_name,
    album_name,
    duration_ms
from {{ ref('spotify_raw') }}
where track_id is not null
