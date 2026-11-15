-- upsert silver.dim_category dari versi terbaru tiap kategori di bronze
MERGE INTO silver.dim_category AS t
USING (
    SELECT DISTINCT ON (b.id)
        b.id,
        b.name,
        b.updated_at AT TIME ZONE 'Asia/Jakarta' AS updated_at
    FROM bronze.categories b
    ORDER BY b.id, b.updated_at DESC
) AS s
ON (t.category_id = s.id)
WHEN MATCHED THEN
    UPDATE SET
        name = s.name,
        updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (category_id, name, updated_at)
    VALUES (s.id, s.name, s.updated_at);
