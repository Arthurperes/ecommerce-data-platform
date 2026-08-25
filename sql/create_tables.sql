CREATE TABLE IF NOT EXISTS ecommerce_events (
    id BIGSERIAL PRIMARY KEY,

    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(30) NOT NULL,

    product_id BIGINT NOT NULL,
    category_id BIGINT,

    category_code VARCHAR(255),
    brand VARCHAR(255),

    price NUMERIC(12,2) NOT NULL,

    user_id BIGINT NOT NULL,
    user_session VARCHAR(255),

    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'ecommerce'
);