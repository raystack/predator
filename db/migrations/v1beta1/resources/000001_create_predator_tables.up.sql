-- create profile
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS profile(
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v1(),
    urn TEXT NOT NULL,
    group_name VARCHAR,
    filter TEXT,
    mode VARCHAR,
    total_records BIGINT NOT NULL DEFAULT 0,
    audit_time TIMESTAMP,
    event_timestamp TIMESTAMP
    );

-- create table bigquery job
CREATE TABLE IF NOT EXISTS bigquery_job(
                                           id SERIAL PRIMARY KEY,
                                           profile_id UUID NOT NULL references profile(id),
    bq_id TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
    );

CREATE INDEX bj_profile_id_idx ON bigquery_job (profile_id);
CREATE INDEX bj_bq_id_idx ON bigquery_job (bq_id);

-- create metric table
CREATE TABLE IF NOT EXISTS metric(
                                     id SERIAL PRIMARY KEY,
                                     profile_id uuid NOT NULL references profile(id),
    group_value TEXT,
    field_id TEXT,
    owner_type VARCHAR (30) NOT NULL,
    metric_name VARCHAR (50) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    condition TEXT,
    category VARCHAR,
    metadata JSONB,
    created_at TIMESTAMP NOT NULL
    );

CREATE INDEX m_profile_id_idx ON metric (profile_id);
CREATE INDEX m_field_id_idx ON metric (field_id);
CREATE INDEX m_event_timestamp_idx ON metric (created_at);

-- create audit table
CREATE TABLE IF NOT EXISTS audit(
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v1(),
    profile_id UUID NOT NULL references profile(id),
    total_records BIGINT NOT NULL DEFAULT 0,
    event_timestamp TIMESTAMP NOT NULL
    );

CREATE INDEX au_profile_id_idx ON audit (profile_id);

-- create audit result table

CREATE TABLE IF NOT EXISTS audit_result(
                                           id SERIAL PRIMARY KEY,
                                           audit_id UUID NOT NULL references audit(id),
    group_value TEXT,
    field_id TEXT,
    metric_name VARCHAR (50) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    tolerance_rules JSON NOT NULL,
    condition TEXT,
    pass_flag BOOLEAN NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP NOT NULL
    );

CREATE INDEX ar_audit_id_idx ON audit_result (audit_id);
CREATE INDEX ar_field_id_idx ON audit_result (field_id);

-- create status table

CREATE TABLE IF NOT EXISTS status(
                                     id SERIAL PRIMARY KEY,
                                     job_id VARCHAR (36) NOT NULL,
    job_type VARCHAR (30) NOT NULL,
    status VARCHAR (50) NOT NULL,
    message TEXT,
    created_at TIMESTAMP NOT NULL
    );

CREATE INDEX s_job_id_idx ON status (job_id);
CREATE INDEX s_job_type_idx ON status (job_type);
CREATE INDEX s_event_timestamp_idx ON status (created_at);

-- create entity table

CREATE TABLE IF NOT EXISTS entity(
   id varchar PRIMARY KEY,
   name varchar not null,
   environment varchar not null,
   git_url varchar not null,
   gcp_project_ids varchar not null,
   created_at timestamp not null,
   updated_at timestamp
   );
