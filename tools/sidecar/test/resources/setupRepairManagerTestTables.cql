CREATE KEYSPACE IF NOT exists test_repair WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'};

ALTER KEYSPACE test_repair WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'};
ALTER KEYSPACE system_distributed_test WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'};

CREATE TABLE IF NOT exists test_repair.subrange_test (
    key text PRIMARY KEY,
    value text,
);

CREATE TABLE IF NOT exists test_repair.no_repair (
    key text PRIMARY KEY,
    value text,
);

CREATE TABLE IF NOT exists test_repair.incremental_test (
    key text PRIMARY KEY,
    value text,
);

CREATE TABLE IF NOT exists test_repair.default_test (
    key text PRIMARY KEY,
    value text,
);

CREATE KEYSPACE IF NOT exists single_rf WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
CREATE KEYSPACE IF NOT exists single_nt_rf WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '1'};