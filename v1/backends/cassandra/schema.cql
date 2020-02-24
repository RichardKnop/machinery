# CQL for initializing Keyspace and database for Machinery. Keyspace name can be customizable.
CREATE KEYSPACE IF NOT EXISTS machinery WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true;
CREATE TABLE job (id text, name text, data text, state text, PRIMARY KEY (id));
CREATE INDEX ON job (state);
