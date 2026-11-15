CREATE TABLE authors (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    desk        TEXT,
    joined_at   TIMESTAMP,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE categories (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE articles (
    id           BIGSERIAL PRIMARY KEY,
    title        TEXT NOT NULL,
    content      TEXT,
    published_at TIMESTAMP,
    author_id    BIGINT REFERENCES authors(id),
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now(),
    deleted_at   TIMESTAMP
);

CREATE TABLE article_categories (
    article_id  BIGINT REFERENCES articles(id) ON DELETE CASCADE,
    category_id BIGINT REFERENCES categories(id),
    PRIMARY KEY (article_id, category_id)
);

CREATE TABLE article_views (
    id          BIGSERIAL PRIMARY KEY,
    article_id  BIGINT NOT NULL,
    session_id  TEXT,
    device      TEXT,
    viewed_at   TIMESTAMP NOT NULL
);

CREATE INDEX idx_articles_updated_at ON articles (updated_at);
CREATE INDEX idx_views_viewed_at     ON article_views (viewed_at);