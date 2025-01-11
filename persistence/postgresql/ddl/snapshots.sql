create table if not exists snapshots
(
    actor_name     varchar(255) not null,
    snapshot_index bigint       not null,
    snapshot       json         not null,
    message_type   varchar(255) not null,
    primary key (actor_name)
);
