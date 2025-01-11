create table if not exists event_journals
(
    actor_name   varchar(255) not null,
    event_index  bigint       not null,
    event        json         not null,
    message_type varchar(255) not null,
    primary key (actor_name, event_index)
);

