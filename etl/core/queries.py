"""SQL-запросы для извлечения данных из PostgreSQL."""

FETCH_MODIFIED_MOVIES = """
WITH recently_modified AS (
    SELECT DISTINCT fw.id
    FROM content.film_work fw
    LEFT JOIN content.genre_film_work  gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre            g   ON g.id = gfw.genre_id
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person           p   ON p.id = pfw.person_id
    WHERE fw.modified > '{last_modified}'
       OR g.modified   > '{last_modified}'
       OR p.modified   > '{last_modified}'
)
SELECT fw.id,
       fw.rating            AS imdb_rating,
       fw.title,
       fw.description,
       fw.modified,
       fw.creation_date,
       COALESCE(
           json_agg(
               DISTINCT jsonb_build_object('id', g.id, 'name', g.name)
           ) FILTER (WHERE g.id IS NOT NULL),
           '[]'::json
       ) AS genres,
       COALESCE(
           json_agg(
               DISTINCT jsonb_build_object(
                   'id', p.id,
                   'name', p.full_name,
                   'role', pfw.role
               )
           ) FILTER (WHERE p.id IS NOT NULL),
           '[]'::json
       ) AS persons
FROM content.film_work fw
LEFT JOIN content.genre_film_work  gfw  ON gfw.film_work_id = fw.id
LEFT JOIN content.genre            g    ON g.id = gfw.genre_id
LEFT JOIN content.person_film_work pfw  ON pfw.film_work_id = fw.id
LEFT JOIN content.person           p    ON p.id = pfw.person_id
WHERE fw.id IN (SELECT id FROM recently_modified)
GROUP BY fw.id, fw.rating, fw.title, fw.description, fw.modified, fw.creation_date
ORDER BY fw.modified
LIMIT {limit};
"""