from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

cols = "article_id, category_id"


def run(days=2):
    # full reload karena tidak ada timestamp
    src = PostgresHook(postgres_conn_id="postgres_source")
    dwh = PostgresHook(postgres_conn_id="postgres_dwh")
    conn = dwh.get_conn()
    cur = conn.cursor()

    query = f"""
        SELECT {cols} FROM public.article_categories
    """
    query1 = f"""
        TRUNCATE bronze.article_categories
    """
    query2 = f"""
        INSERT INTO bronze.article_categories ({cols}) VALUES %s
    """
    rows = src.get_records(query)
    cur.execute(query1)

    if rows:
        execute_values(cur, query2, rows, page_size=5000)

    conn.commit()
    cur.close()
    conn.close()
    print(f"bronze.article_categories: {len(rows)} rows")
