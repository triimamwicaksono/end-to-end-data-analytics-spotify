select
    f.track_id,
    t.track_name,
    a.artist_id,
    a.artist_name,
    f.popularity,
    f.rank,
    f.file_name,
    f.ingested_at
from {{ ref('fact_top_tracks') }} f
left join {{ ref('dim_track') }} t
    on f.track_id = t.track_id
left join {{ ref('dim_artist') }} a
    on f.artist_id = a.artist_id
order by popularity desc
