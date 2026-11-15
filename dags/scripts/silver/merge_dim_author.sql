-- upsert silver.dim_author dari versi terbaru tiap author di bronze
MERGE INTO silver.dim_author AS t
USING (
    SELECT DISTINCT ON (b.id)
        b.id,
        b.name,
        b.desk,
        b.joined_at AT TIME ZONE 'Asia/Jakarta'  AS joined_at,
        b.updated_at AT TIME ZONE 'Asia/Jakarta' AS updated_at
    FROM bronze.authors b
    ORDER BY b.id, b.updated_at DESC
) AS s
ON (t.author_id = s.id)
WHEN MATCHED THEN
    UPDATE SET
        name = s.name,
        desk = s.desk,
        joined_at = s.joined_at,
        updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (author_id, name, desk, joined_at, updated_at)
    VALUES (s.id, s.name, s.desk, s.joined_at, s.updated_at);
