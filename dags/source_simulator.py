import random
import uuid
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# DAG khusus untuk mensimulasikan perubahan data di source, agar bisa dites pipeline ETL


def connection():
    conn = PostgresHook(postgres_conn_id="postgres_source").get_conn()
    return conn


def insert_articles(cur):
    # ambil daftar author dan kategori untuk dipilih acak
    cur.execute("SELECT id FROM authors")
    author_ids = []
    for row in cur.fetchall():
        author_ids.append(row[0])

    cur.execute("SELECT id FROM categories")
    category_ids = []
    for row in cur.fetchall():
        category_ids.append(row[0])

    new_ids = []
    for i in range(20):
        author_id = random.choice(author_ids)
        title = "Artikel simulasi " + str(random.randint(1000, 9999))
        content = "Ini isi artikel simulasi nomor " + str(i + 1) + " untuk menguji pipeline."

        # sekitar 70% langsung terbit, sisanya draft (published_at NULL)
        if random.random() < 0.7:
            sql = """INSERT INTO articles (title, content, published_at, author_id, created_at, updated_at)
                     VALUES (%s, %s, now(), %s, now(), now()) RETURNING id"""
        else:
            sql = """INSERT INTO articles (title, content, published_at, author_id, created_at, updated_at)
                     VALUES (%s, %s, NULL, %s, now(), now()) RETURNING id"""
        cur.execute(sql, (title, content, author_id))
        article_id = cur.fetchone()[0]

        # satu kategori acak per artikel
        category_id = random.choice(category_ids)
        cur.execute(
            "INSERT INTO article_categories (article_id, category_id) VALUES (%s, %s)",
            (article_id, category_id),
        )
        new_ids.append(article_id)

    print("insert articles:", new_ids)


def update_articles(cur):
    # pilih 10 artikel acak yang belum dihapus
    cur.execute("SELECT id FROM articles WHERE deleted_at IS NULL ORDER BY random() LIMIT 10")
    ids = []
    for row in cur.fetchall():
        ids.append(row[0])

    # ubah title dan updated_at
    for article_id in ids:
        cur.execute(
            "UPDATE articles SET title = title || ' (updated)', updated_at = now() WHERE id = %s",
            (article_id,),
        )

    print("update articles:", ids)


def soft_delete_articles(cur):
    # pilih 3 artikel acak yang belum dihapus
    cur.execute("SELECT id FROM articles WHERE deleted_at IS NULL ORDER BY random() LIMIT 3")
    ids = []
    for row in cur.fetchall():
        ids.append(row[0])

    # soft delete: baris tetap ada, hanya deleted_at diisi
    for article_id in ids:
        cur.execute(
            "UPDATE articles SET deleted_at = now(), updated_at = now() WHERE id = %s",
            (article_id,),
        )

    print("soft delete articles:", ids)


def hard_delete_articles(cur):
    # pilih 3 artikel acak
    cur.execute("SELECT id FROM articles ORDER BY random() LIMIT 3")
    ids = []
    for row in cur.fetchall():
        ids.append(row[0])

    # hard delete: hapus relasi kategorinya dulu, baru artikelnya
    for article_id in ids:
        cur.execute("DELETE FROM article_categories WHERE article_id = %s", (article_id,))
        cur.execute("DELETE FROM articles WHERE id = %s", (article_id,))

    print("hard delete articles:", ids)


def insert_views(cur):
    # hanya artikel yang sudah terbit dan belum dihapus
    cur.execute("SELECT id FROM articles WHERE published_at IS NOT NULL AND deleted_at IS NULL")
    article_ids = []
    for row in cur.fetchall():
        article_ids.append(row[0])

    devices = ["android", "ios", "web", "desktop"]

    new_ids = []
    for i in range(50):
        article_id = random.choice(article_ids)
        device = random.choice(devices)
        session_id = str(uuid.uuid4())
        cur.execute(
            """INSERT INTO article_views (article_id, session_id, device, viewed_at)
               VALUES (%s, %s, %s, now()) RETURNING id""",
            (article_id, session_id, device),
        )
        new_ids.append(cur.fetchone()[0])

    print("insert article_views:", new_ids)


@dag(
    dag_id="source_simulator",
    schedule="30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kumparan", "simulator"],
)
def source_simulator():

    @task
    def simulate():
        conn = connection()
        cur = conn.cursor()

        # tiap langkah di-commit sendiri
        insert_articles(cur)
        conn.commit()

        update_articles(cur)
        conn.commit()

        soft_delete_articles(cur)
        conn.commit()

        hard_delete_articles(cur)
        conn.commit()

        insert_views(cur)
        conn.commit()

        cur.close()
        conn.close()
        print("Simulation completed.")

    simulate()


source_simulator()
