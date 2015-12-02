CREATE TABLE IF NOT EXISTS lddb (
    id text not null unique primary key,
    data jsonb not null,
    manifest jsonb not null,
    quoted jsonb,
    created timestamp with time zone not null default now(),
    modified timestamp with time zone not null default now(),
    deleted boolean default false
    );

CREATE TABLE IF NOT EXISTS lddb__versions (
    pk serial,
    id text not null,
    checksum char(32) not null,
    data jsonb not null,
    manifest jsonb not null,
    modified timestamp with time zone not null default now(),
    unique (id, checksum, modified)
    );

CREATE TABLE IF NOT EXISTS lddb__settings (
    key text not null unique primary key,
    settings jsonb not null
);
