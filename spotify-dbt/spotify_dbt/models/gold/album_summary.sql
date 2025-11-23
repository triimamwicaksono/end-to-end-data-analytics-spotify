select
    t.album_name,
    count(distinct f.track_id) as total_tracks,
    avg(f.popularity) as avg_popularity
from {{ ref('fact_top_tracks') }} f
join {{ ref('dim_track') }} t
    on f.track_id = t.track_id
group by t.album_name
order by avg_popularity desc
