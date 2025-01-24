CREATE TABLE IF NOT EXISTS spawn_locks
(
    lock_id   TEXT NOT NULL,
    identity  TEXT NOT NULL,
    kind      TEXT NOT NULL,
    member_id TEXT NOT NULL,
    locked_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT spawn_locks_pk PRIMARY KEY (lock_id),
    UNIQUE (identity, kind)
);
