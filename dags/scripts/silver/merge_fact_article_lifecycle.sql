-- upsert silver.fact_article_lifecycle, satu baris per artikel
MERGE INTO silver.fact_article_lifecycle AS t
USING (
    SELECT
        src.id,
        src.author_id,
        src.created_at AT TIME ZONE 'Asia/Jakarta'    AS created_at,
        src.published_at AT TIME ZONE 'Asia/Jakarta'  AS published_at,
        src.updated_at AT TIME ZONE 'Asia/Jakarta'    AS last_updated_at,
        src.deleted_at AT TIME ZONE 'Asia/Jakarta'    AS deleted_at,
        to_char(src.created_at, 'YYYYMMDD')::int      AS created_date_id, --sudah WIB
        to_char(src.published_at, 'YYYYMMDD')::int    AS published_date_id, --sudah WIB
        (extract(epoch FROM (src.published_at - src.created_at)) / 60)::int AS minutes_to_publish, --gap menit dari dicreate sampe publish
        regexp_count(
            regexp_replace(src.content, '<[^>]+>', ' ', 'g'), --regex buang tag html, abis itu di count by karakter bukan spasi
            '\S+'
        )                                           AS word_count,
        (src.deleted_at IS NOT NULL)                  AS is_deleted
    FROM (
        -- yang terbaru dari bronze
        SELECT
            a.*,
            ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.updated_at DESC) AS rn
        FROM bronze.articles a
    ) src
    WHERE src.rn = 1
) AS s
ON t.article_id = s.id
WHEN MATCHED THEN
    UPDATE SET
        author_id = s.author_id,
        created_at = s.created_at,
        published_at = s.published_at,
        last_updated_at = s.last_updated_at,
        deleted_at = COALESCE(t.deleted_at, s.deleted_at),
        created_date_id = s.created_date_id,
        published_date_id = s.published_date_id,
        minutes_to_publish = s.minutes_to_publish,
        word_count = s.word_count,
        is_deleted = t.is_deleted OR s.is_deleted -- yang sudah true gaboleh balik jadi false
WHEN NOT MATCHED THEN
    INSERT (article_id, author_id, created_at, published_at, last_updated_at, deleted_at,
            created_date_id, published_date_id, minutes_to_publish, word_count, is_deleted)
    VALUES (s.id, s.author_id, s.created_at, s.published_at, s.last_updated_at, s.deleted_at,
            s.created_date_id, s.published_date_id, s.minutes_to_publish, s.word_count, s.is_deleted);
