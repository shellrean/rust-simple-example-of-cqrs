create table public.events
(
    aggregate_type text   not null,
    aggregate_id   text   not null,
    sequence       bigint not null
        constraint events_sequence_check
            check (sequence >= 0),
    event_type     text   not null,
    event_version  text   not null,
    payload        json   not null,
    metadata       json   not null,
    primary key (aggregate_type, aggregate_id, sequence)
);