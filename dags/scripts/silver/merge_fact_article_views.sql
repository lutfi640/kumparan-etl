-- tambah article view baru ke silver.fact_article_views
-- view yang sudah ada dijaga ON CONFLICT, jadi aman dijalankan ulang
INSERT INTO silver.fact_article_views
    (view_id, article_id, session_id, device, viewed_at, viewed_date_id)
SELECT
    v.id,
    v.article_id,
    v.session_id,
    v.device,
    v.viewed_at AT TIME ZONE 'Asia/Jakarta',
    to_char(v.viewed_at, 'YYYYMMDD')::int
FROM bronze.article_views v
ON CONFLICT (view_id) DO NOTHING;