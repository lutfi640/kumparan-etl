-- Buat 2 MV di GOLD untuk keperluan anlytic
-- 1) gold.mv_monthly_publishing --> untuk insight produksi konten naik/turun per bulan
-- 2) gold.mv_author_productivity --> untuk insight siapa penulis paling produktif dan paling dibaca

DROP MATERIALIZED VIEW IF EXISTS gold.mv_monthly_publishing;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_author_productivity;

CREATE MATERIALIZED VIEW gold.mv_monthly_publishing AS
SELECT
    d.year,
    d.month,
    d.month_name,
    c.name                               AS category_name,
    count(*)                             AS published_articles,
    round(avg(f.minutes_to_publish))     AS avg_minutes_to_publish,
    round(avg(f.word_count))             AS avg_word_count
FROM silver.fact_article_lifecycle f
JOIN silver.dim_article a ON a.article_id = f.article_id
JOIN silver.dim_category c ON c.category_id = a.category_id
JOIN silver.dim_date d ON d.date_id = f.published_date_id
WHERE f.published_date_id IS NOT NULL
  AND f.is_deleted = false
GROUP BY d.year, d.month, d.month_name, c.name
ORDER BY d.year, d.month, c.name;

CREATE MATERIALIZED VIEW gold.mv_author_productivity AS
SELECT
    au.name                                                  AS author_name,
    au.desk                                                  AS desk,
    count(DISTINCT f.article_id)                             AS published_articles,
    COALESCE(sum(v.total_views), 0)::bigint                  AS total_views,
    round(avg(f.word_count))                                 AS avg_word_count,
    max(to_date(f.published_date_id::text, 'YYYYMMDD'))      AS last_published_date
FROM silver.dim_author au
JOIN silver.dim_article a ON a.author_id = au.author_id
    AND a.is_deleted = false
JOIN silver.fact_article_lifecycle f ON f.article_id = a.article_id
    AND f.published_date_id IS NOT NULL
LEFT JOIN (
    SELECT article_id, count(*) AS total_views
    FROM silver.fact_article_views
    GROUP BY article_id
) v ON v.article_id = a.article_id
GROUP BY au.author_id, au.name, au.desk
ORDER BY total_views DESC;
