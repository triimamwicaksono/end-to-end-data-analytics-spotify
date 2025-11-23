
select distinct
    artist_id,
    artist_name
from {{ ref('spotify_raw') }}
where artist_id is not null
