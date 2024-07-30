BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS hec_user (
    id           SERIAL PRIMARY KEY,
    tenant_id    TEXT  NOT NULL,
    user_id      UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    practitioner_id  BIGINT DEFAUlT NULL,
    active       BOOL NOT NULL,
    first_name   TEXT NOT NULL,
    last_name    TEXT NOT NULL,
    email        TEXT NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by   VARCHAR(255) NOT NULL,
    CONSTRAINT email_unique UNIQUE (email)
);
CREATE INDEX user_tenant_id_idx ON hec_user(tenant_id);
CREATE INDEX user_practitioner_id_idx ON hec_user(practitioner_id);

CREATE TABLE IF NOT EXISTS hec_user_history (
    id           SERIAL PRIMARY KEY,
    tenant_id    TEXT  NOT NULL,
    user_id      UUID  NOT NULL,
    practitioner_id  BIGINT DEFAUlT NULL,
    active       BOOL NOT NULL,
    first_name   TEXT NOT NULL,
    last_name    TEXT NOT NULL,
    email        TEXT NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by   VARCHAR(255) NOT NULL
);

COMMIT;

END;
