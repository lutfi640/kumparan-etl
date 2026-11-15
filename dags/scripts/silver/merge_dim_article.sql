-- upsert silver.dim_article dari versi terbaru tiap artikel di bronze
MERGE INTO silver.dim_article AS t
USING (
    SELECT DISTINCT ON (src.id)
        src.id,
        trim(src.title)                                  AS title,
        regexp_replace(src.content, '<[^>]+>', '', 'g')  AS content, --regex buang tag html
        src.author_id,
        ac.category_id,
        (src.published_at IS NULL)                       AS is_draft,
        (src.deleted_at IS NOT NULL)                     AS is_deleted,
        src.deleted_at AT TIME ZONE 'Asia/Jakarta'       AS deleted_detected_at,
        src.updated_at AT TIME ZONE 'Asia/Jakarta'       AS updated_at
    FROM bronze.articles src
    LEFT JOIN bronze.article_categories ac ON ac.article_id = src.id
    ORDER BY src.id, src.updated_at DESC, ac.category_id
) AS s
ON (t.article_id = s.id)
WHEN MATCHED THEN
    UPDATE SET
        title = s.title,
        content = s.content,
        author_id = s.author_id,
        category_id = COALESCE(s.category_id, t.category_id),
        is_draft = s.is_draft,
        is_deleted = t.is_deleted OR s.is_deleted, -- yang sudah true gaboleh balik jadi false
        deleted_detected_at = COALESCE(t.deleted_detected_at, s.deleted_detected_at),
        updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (article_id, title, content, author_id, category_id, is_draft, is_deleted, deleted_detected_at, updated_at)
    VALUES (s.id, s.title, s.content, s.author_id, s.category_id, s.is_draft, s.is_deleted, s.deleted_detected_at, s.updated_at);
