from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from scripts.bronze import load_articles
from scripts.bronze import load_authors
from scripts.bronze import load_categories
from scripts.bronze import load_article_views
from scripts.bronze import load_article_categories
from scripts.bronze import load_cdc_articles

# REMINDER:
# Wajib menjalankan query berikut
#   1. dags/sql/fill_dim_date.sql   (isi silver.dim_date)
#   2. dags/sql/create_gold_mv.sql  (buat materialized view gold)


@dag(
    dag_id="dwh_hourly",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    template_searchpath="/opt/airflow/dags/scripts/silver",
    tags=["kumparan", "dwh"],
)
def dwh_hourly():

    #========= BRONZE ==================
    @task
    def authors():
        load_authors.run()

    @task
    def categories():
        load_categories.run()

    @task
    def article_categories():
        load_article_categories.run()

    @task
    def articles():
        load_articles.run()

    # CDC
    @task
    def cdc_articles():
        load_cdc_articles.run()

    @task
    def article_views():
        load_article_views.run()

    with TaskGroup(group_id="bronze_layer", prefix_group_id=False):
        authors_task = authors()
        categories_task = categories()
        article_categories_task = article_categories()
        articles_task = articles()
        cdc_articles_task = cdc_articles()
        article_views_task = article_views()

    #alur task
    [authors_task, categories_task, article_categories_task] >> articles_task >> cdc_articles_task >> article_views_task

    #=============== SILVER ================
    with TaskGroup(group_id="silver_layer", prefix_group_id=False):
        merge_dim_author = SQLExecuteQueryOperator(
            task_id="merge_dim_author",
            conn_id="postgres_dwh",
            sql="merge_dim_author.sql",
        )

        merge_dim_category = SQLExecuteQueryOperator(
            task_id="merge_dim_category",
            conn_id="postgres_dwh",
            sql="merge_dim_category.sql",
        )

        merge_dim_article = SQLExecuteQueryOperator(
            task_id="merge_dim_article",
            conn_id="postgres_dwh",
            sql="merge_dim_article.sql",
        )

        merge_fact_article_lifecycle = SQLExecuteQueryOperator(
            task_id="merge_fact_article_lifecycle",
            conn_id="postgres_dwh",
            sql="merge_fact_article_lifecycle.sql",
        )

        merge_fact_article_views = SQLExecuteQueryOperator(
            task_id="merge_fact_article_views",
            conn_id="postgres_dwh",
            sql="merge_fact_article_views.sql",
        )

        # CDC
        apply_cdc_deletes = SQLExecuteQueryOperator(
            task_id="apply_cdc_deletes",
            conn_id="postgres_dwh",
            sql="apply_cdc_deletes.sql",
        )

    #alur task
    article_views_task >> merge_dim_author >> merge_dim_category >> merge_dim_article
    merge_dim_article >> merge_fact_article_lifecycle >> merge_fact_article_views >> apply_cdc_deletes

    #============== GOLD =============
    # MV only, kebutuhan analytic
    with TaskGroup(group_id="gold_layer", prefix_group_id=False):
        refresh_mv_monthly_publishing = SQLExecuteQueryOperator(
            task_id="refresh_mv_monthly_publishing",
            conn_id="postgres_dwh",
            sql="REFRESH MATERIALIZED VIEW gold.mv_monthly_publishing",
        )

        refresh_mv_author_productivity = SQLExecuteQueryOperator(
            task_id="refresh_mv_author_productivity",
            conn_id="postgres_dwh",
            sql="REFRESH MATERIALIZED VIEW gold.mv_author_productivity",
        )

    #alur task
    apply_cdc_deletes >> [refresh_mv_monthly_publishing, refresh_mv_author_productivity]


dwh_hourly()
