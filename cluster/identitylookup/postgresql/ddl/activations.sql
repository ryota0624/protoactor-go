CREATE TABLE IF NOT EXISTS activations
(
    identity     TEXT  NOT NULL,
    kind         TEXT  NOT NULL,
    identity_key TEXT  NOT NULL,
    pid          json NOT NULL,
    member_id    TEXT  NOT NULL,
    lock_id      TEXT  NOT NULL,
    activated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (identity, kind),
    -- lockの無効化を考慮したい
    FOREIGN KEY (lock_id) references spawn_locks (lock_id)

);

CREATE INDEX IF NOT EXISTS activations_member_idx ON activations (member_id);
