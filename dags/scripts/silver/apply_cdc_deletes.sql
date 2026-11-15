--Update flagging is_deleted di dim_article dan fact_article_lifecycle berdasarkan data di bronze.cdc_articles
UPDATE silver.dim_article
SET is_deleted = true,
    deleted_detected_at = now()
WHERE article_id IN (SELECT article_id FROM bronze.cdc_articles WHERE op = 'DELETE')
  AND is_deleted = false;

UPDATE silver.fact_article_lifecycle
SET is_deleted = true,
    deleted_at = now()
WHERE article_id IN (SELECT article_id FROM bronze.cdc_articles WHERE op = 'DELETE')
  AND is_deleted = false;
