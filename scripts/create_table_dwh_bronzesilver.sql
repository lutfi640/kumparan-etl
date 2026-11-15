
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

--1) Bronze layer
CREATE TABLE bronze.authors (
    id          BIGINT,
    name        TEXT,
    desk        TEXT,
    joined_at   TIMESTAMP,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE bronze.categories (
    id          BIGINT,
    name        TEXT,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE bronze.articles (
    id           BIGINT,
    title        TEXT,
    content      TEXT,
    published_at TIMESTAMP,
    author_id    BIGINT,
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP,
    deleted_at   TIMESTAMP,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE bronze.article_categories (
    article_id  BIGINT,
    category_id BIGINT,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE bronze.article_views (
    id          BIGINT,
    article_id  BIGINT,
    session_id  TEXT,
    device      TEXT,
    viewed_at   TIMESTAMP,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

--cdc di article
CREATE TABLE bronze.cdc_articles (
    lsn         pg_lsn PRIMARY KEY,
    xid         TEXT,
    op          TEXT,
    article_id  BIGINT,
    raw_data    TEXT,
    captured_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_bronze_articles_id   ON bronze.articles (id, updated_at DESC);
CREATE INDEX idx_bronze_authors_id    ON bronze.authors (id, updated_at DESC);
CREATE INDEX idx_bronze_categories_id ON bronze.categories (id, updated_at DESC);
CREATE INDEX idx_bronze_views_id      ON bronze.article_views (id);

--2) Silver layer
CREATE TABLE silver.dim_author (
    author_id   BIGINT PRIMARY KEY,
    name        TEXT NOT NULL,
    desk        TEXT,
    joined_at   TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE silver.dim_category (
    category_id BIGINT PRIMARY KEY,
    name        TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE silver.dim_article (
    article_id          BIGINT PRIMARY KEY,
    title               TEXT NOT NULL,
    content             TEXT,
    author_id           BIGINT,
    category_id         BIGINT,
    is_draft            BOOLEAN NOT NULL,
    is_deleted          BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_detected_at TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE silver.dim_date (
    date_id     INT PRIMARY KEY,          -- format 20261113
    full_date   DATE NOT NULL,
    year        INT NOT NULL,
    quarter     INT NOT NULL,
    month       INT NOT NULL,
    month_name  TEXT NOT NULL,
    day         INT NOT NULL,
    day_of_week INT NOT NULL,             -- 1 = Senin
    day_name    TEXT NOT NULL,
    is_weekend  BOOLEAN NOT NULL
);

CREATE TABLE silver.fact_article_lifecycle (
    article_id         BIGINT PRIMARY KEY,
    author_id          BIGINT,
    created_at         TIMESTAMPTZ NOT NULL,
    published_at       TIMESTAMPTZ,
    last_updated_at    TIMESTAMPTZ NOT NULL,
    deleted_at         TIMESTAMPTZ,
    created_date_id    INT,
    published_date_id  INT,
    minutes_to_publish INT,
    word_count         INT,
    is_deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE silver.fact_article_views (
    view_id        BIGINT PRIMARY KEY,
    article_id     BIGINT NOT NULL,
    session_id     TEXT,
    device         TEXT,
    viewed_at      TIMESTAMPTZ NOT NULL,
    viewed_date_id INT NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_fav_article        ON silver.fact_article_views (article_id);
CREATE INDEX idx_fav_date           ON silver.fact_article_views (viewed_date_id);
CREATE INDEX idx_dim_article_author ON silver.dim_article (author_id);
