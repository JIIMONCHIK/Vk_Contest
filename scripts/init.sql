CREATE TABLE IF NOT EXISTS raw_users_by_posts (
    post_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    extracted_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS top_users_by_posts (
    user_id INTEGER PRIMARY KEY,
    posts_cnt INTEGER NOT NULL,
    calculated_at TIMESTAMP NOT NULL
);