CREATE KEYSPACE IF NOT exists system_distributed_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT exists system_distributed_test.repair_process (
    cluster_name    text,
    repair_id       int,
    created_node_id text,
    start_time      timestamp,
    end_time        timestamp,
    pause_time      timestamp,
    status          text,
    last_event      map<text, text>,
    PRIMARY KEY ((cluster_name), repair_id)
) WITH CLUSTERING ORDER BY (repair_id DESC)
AND compaction = {'class': 'LeveledCompactionStrategy'}
AND default_time_to_live = 15778463;

CREATE TABLE IF NOT exists system_distributed_test.repair_sequence (
    cluster_name    text,
    repair_id       int,
    seq             int,
    node_id         text,
    created_nodeid  text,
    create_time     timestamp,
    start_time      timestamp,
    end_time        timestamp,
    pause_time      timestamp,
    status          text,
    last_heartbeat  timestamp,
    schedule_name   text,
    last_event      map<text, text>,
    PRIMARY KEY ((cluster_name), repair_id, seq)
) WITH CLUSTERING ORDER BY (repair_id DESC, seq ASC)
AND compaction = {'class': 'LeveledCompactionStrategy'}
AND default_time_to_live = 15778463;

CREATE TABLE IF NOT exists system_distributed_test.repair_status (
    cluster_name    text,
    repair_id       int,
    node_id         text,
    keyspace_name   text,
    table_name      text,
    repair_cmd      int,
    start_token     text,
    end_token       text,
    create_time     timestamp,
    start_time      timestamp,
    end_time        timestamp,
    pause_time      timestamp,
    status          text,
    last_event      map<text,text>,
    config          map<text, text>,
    PRIMARY KEY ((cluster_name, repair_id), node_id, keyspace_name, table_name, repair_cmd, start_token, end_token)
) WITH compaction = {'class': 'LeveledCompactionStrategy'}
AND default_time_to_live = 15778463;

CREATE TABLE IF NOT exists system_distributed_test.repair_hook_status (
    cluster_name    text,
    repair_id       int,
    node_id         text,
    created_node_id text,
    create_time     timestamp,
    start_time      timestamp,
    end_time        timestamp,
    pause_time      timestamp,
    status          text,
    last_heartbeat  timestamp,
    last_event      map<text,text>,
    PRIMARY KEY ((cluster_name), repair_id, node_id)
) WITH CLUSTERING ORDER BY (repair_id DESC, node_id DESC)
AND compaction = {'class': 'LeveledCompactionStrategy'}
AND default_time_to_live = 15778463;

CREATE TABLE IF NOT exists system_distributed_test.repair_config (
    cluster_name text,
    schedule_name text,
    keyspace_name text,
    table_name text,
    type text,
    workers int,
    parallelism text,
    hooks list<text>,
    split_strategy text,
    interrepair_delay_minutes int,
    PRIMARY KEY ((cluster_name), schedule_name, keyspace_name, table_name)
) WITH CLUSTERING ORDER BY (schedule_name ASC, keyspace_name ASC, table_name ASC)
AND compaction = {'class': 'LeveledCompactionStrategy'}
AND default_time_to_live = 15778463;



