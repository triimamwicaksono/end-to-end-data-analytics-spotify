select
    a.artist_id,
    a.artist_name,
    count(distinct f.track_id) as total_tracks,
    avg(f.popularity) as avg_popularity,
    max(f.popularity) as max_popularity
from {{ ref('fact_top_tracks') }} f
join {{ ref('dim_artist') }} a
    on f.artist_id = a.artist_id
group by a.artist_id, a.artist_name
order by avg_popularity desc
