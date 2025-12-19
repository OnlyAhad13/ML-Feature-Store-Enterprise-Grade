-- ============================================================
-- PostgreSQL Initialization Script
-- Creates databases and schemas for Feature Store
-- ============================================================

-- Create additional databases for different purposes
CREATE DATABASE feast_offline;
CREATE DATABASE airflow;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE feast_registry TO feast;
GRANT ALL PRIVILEGES ON DATABASE feast_offline TO feast;
GRANT ALL PRIVILEGES ON DATABASE airflow TO feast;

-- Connect to feast_registry and create schemas
\c feast_registry;

CREATE SCHEMA IF NOT EXISTS feast_metadata;
CREATE SCHEMA IF NOT EXISTS feature_views;

-- Create table for tracking feature freshness
CREATE TABLE IF NOT EXISTS feast_metadata.feature_freshness (
    feature_view_name VARCHAR(255) PRIMARY KEY,
    last_updated_at TIMESTAMP WITH TIME ZONE,
    row_count BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Connect to feast_offline and create schemas
\c feast_offline;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS features;

-- Create sample tables for offline feature storage
CREATE TABLE IF NOT EXISTS features.user_features (
    entity_id VARCHAR(255) NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    feature_1 DOUBLE PRECISION,
    feature_2 DOUBLE PRECISION,
    feature_3 DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_id, event_timestamp)
);

CREATE TABLE IF NOT EXISTS features.product_features (
    entity_id VARCHAR(255) NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    category VARCHAR(100),
    price DOUBLE PRECISION,
    inventory_count INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_id, event_timestamp)
);

-- Create indexes for better query performance
CREATE INDEX idx_user_features_timestamp ON features.user_features(event_timestamp);
CREATE INDEX idx_product_features_timestamp ON features.product_features(event_timestamp);

-- Connect to airflow database and set up
\c airflow;

-- Airflow will create its own tables, but we can set up the schema
CREATE SCHEMA IF NOT EXISTS public;
