
select
    r.rank,
    r.popularity,
    r.ingested_at,
    r.file_name,
    r.track_id,
    r.artist_id
from {{ ref('spotify_raw') }} r
