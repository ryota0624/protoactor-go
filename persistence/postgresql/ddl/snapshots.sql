create table if not exists snapshots
(
    id                varchar(255) not null,
    snapshot_index    bigint       not null,
    snapshot          json         not null,
    message_type varchar(255) not null,
    primary key (id)
);
