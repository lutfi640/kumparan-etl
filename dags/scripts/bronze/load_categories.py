from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

cols = "id, name, created_at, updated_at"


def run(days=2):
    # jika param None maka full load, jika integer maka incremental load
    src = PostgresHook(postgres_conn_id="postgres_source")
    dwh = PostgresHook(postgres_conn_id="postgres_dwh")
    conn = dwh.get_conn()
    cur = conn.cursor()

    query = f"""
        SELECT {cols} FROM public.categories
    """
    query1 = f"""
        TRUNCATE bronze.categories
    """
    query2 = f"""
        SELECT {cols}
        FROM public.categories
        WHERE updated_at >= now() - make_interval(days => %s)
    """
    query3 = f"""
        DELETE FROM bronze.categories WHERE updated_at >= now() - make_interval(days => %s)
    """
    query4 = f"""
        INSERT INTO bronze.categories ({cols}) VALUES %s
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
    print(f"bronze.categories: {len(rows)} rows")
