BEGIN;


CREATE TABLE IF NOT EXISTS ROLE (
    id           SERIAL PRIMARY KEY,
    active       BOOL NOT NULL,
    role_id      UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    key          TEXT NOT NULL,
    description  TEXT NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by   VARCHAR(255) NOT NULL,
    CONSTRAINT role_key UNIQUE (key)
);

CREATE TABLE IF NOT EXISTS USER_ROLE (
    id              SERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL,
    role_id         BIGINT NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(255) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES hec_user (id),
    FOREIGN KEY (role_id) REFERENCES ROLE (id)
);

CREATE TABLE IF NOT EXISTS PERMISSION (
    id              SERIAL PRIMARY KEY,
    active          BOOL NOT NULL,
    permission_id   UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    key             TEXT NOT NULL,
    description     TEXT NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(255) NOT NULL,
    CONSTRAINT permission_key UNIQUE (key)
);

CREATE TABLE IF NOT EXISTS USER_PERMISSION (
    id              SERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL,
    permission_id   BIGINT NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(255) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES hec_user (id),
    FOREIGN KEY (permission_id) REFERENCES PERMISSION (id)
);


CREATE TABLE IF NOT EXISTS ROLE_PERMISSION (
    id              SERIAL PRIMARY KEY,
    role_id         BIGINT NOT NULL,
    permission_id   BIGINT NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(255) NOT NULL,
    FOREIGN KEY (role_id) REFERENCES ROLE (id),
    FOREIGN KEY (permission_id) REFERENCES PERMISSION (id)
);

COMMIT;

END;
