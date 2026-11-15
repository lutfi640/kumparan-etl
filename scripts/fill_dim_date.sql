-- isi silver.dim_date untuk tanggal 2016-01-01 sampai 2030-12-31
-- dijalankan manual sekali, aman diulang karena ON CONFLICT DO NOTHING
INSERT INTO silver.dim_date
    (date_id, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
SELECT
    to_char(d, 'YYYYMMDD')::int       AS date_id,
    d::date                           AS full_date,
    extract(year FROM d)::int         AS year,
    extract(quarter FROM d)::int      AS quarter,
    extract(month FROM d)::int        AS month,
    trim(to_char(d, 'Month'))         AS month_name,
    extract(day FROM d)::int          AS day,
    -- isodow: 1 = Senin sampai 7 = Minggu
    extract(isodow FROM d)::int       AS day_of_week,
    trim(to_char(d, 'Day'))           AS day_name,
    -- sabtu (6) dan minggu (7) dihitung weekend
    extract(isodow FROM d) IN (6, 7)  AS is_weekend
FROM generate_series('2016-01-01'::timestamp, '2030-12-31'::timestamp, interval '1 day') AS d
ON CONFLICT (date_id) DO NOTHING;
