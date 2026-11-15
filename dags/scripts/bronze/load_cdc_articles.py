import re

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

cols = "lsn, xid, op, article_id, raw_data"


def run():
    src = PostgresHook(postgres_conn_id="postgres_source") #db_source
    dwh = PostgresHook(postgres_conn_id="postgres_dwh") #db_dwh

    query = f"""
        SELECT lsn::text, xid::text, data
        FROM pg_logical_slot_peek_changes('kumparan_cdc', NULL, NULL)
    """
    query1 = f"""
        INSERT INTO bronze.cdc_articles ({cols}) VALUES %s
        ON CONFLICT (lsn) DO NOTHING
    """
    query2 = f"""
        SELECT pg_replication_slot_advance('kumparan_cdc', %s::pg_lsn)
    """

    # cek slot kumparan_cdc
    changes = src.get_records(query)

    if not changes:
        print("no changes")
        return

    # ambil hanya baris DELETE pada tabel articles
    rows = []
    for change in changes:
        lsn = change[0]
        xid = change[1]
        data = change[2]
        match = re.match(r"table public\.articles: DELETE: id\[bigint\]:(\d+)", data)
        if match:
            article_id = int(match.group(1))
            rows.append((lsn, xid, "DELETE", article_id, data))

    # lsn terakhir dari semua baris, dipakai untuk memajukan slot
    last_lsn = changes[-1][0]

    conn = dwh.get_conn()
    cur = conn.cursor()

    # ON CONFLICT supaya aman kalau task di-rerun
    if rows:
        execute_values(cur, query1, rows, page_size=5000)

    conn.commit()
    cur.close()
    conn.close()

    #slot source dimajuin
    src.get_records(query2, parameters=(last_lsn,))
    print(f"bronze.cdc_articles: {len(rows)} rows, slot advanced to {last_lsn}")
