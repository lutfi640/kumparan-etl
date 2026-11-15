from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

cols = "id, article_id, session_id, device, viewed_at"


def run(days=2):
    # jika param None maka full load, jika integer maka incremental load
    src = PostgresHook(postgres_conn_id="postgres_source")
    dwh = PostgresHook(postgres_conn_id="postgres_dwh")
    conn = dwh.get_conn()
    cur = conn.cursor()

    query = f"""
        SELECT {cols} FROM public.article_views
    """
    query1 = f"""
        TRUNCATE bronze.article_views
    """
    query2 = f"""
        SELECT {cols} 
        FROM public.article_views 
        WHERE viewed_at >= now() - make_interval(days => %s)
    """
    query3 = f"""
        DELETE FROM bronze.article_views WHERE viewed_at >= now() - make_interval(days => %s)
    """
    query4 = f"""
        INSERT INTO bronze.article_views ({cols}) VALUES %s
    """

    if days is None:
        rows = src.get_records(query)
        cur.execute(query1)
    else:
        rows = src.get_records(query2, parameters=(days,))
        cur.execute(query3, (days,))

    if rows:
        execute_values(cur, query4, rows, page_size=5000)

    conn.commit()
    cur.close()
    conn.close()
    print(f"bronze.article_views: {len(rows)} rows")
