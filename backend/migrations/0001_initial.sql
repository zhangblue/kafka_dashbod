CREATE TABLE clusters (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  bootstrap_servers TEXT NOT NULL,
  security_protocol TEXT NOT NULL,
  sasl_mechanism TEXT,
  username TEXT,
  encrypted_secrets BLOB,
  connection_timeout_ms INTEGER NOT NULL DEFAULT 10000 CHECK (connection_timeout_ms > 0),
  default_normal_lag_max INTEGER NOT NULL DEFAULT 999 CHECK (default_normal_lag_max >= 0),
  default_high_lag_min INTEGER NOT NULL DEFAULT 5000 CHECK (default_high_lag_min > default_normal_lag_max),
  default_over_limit_seconds INTEGER NOT NULL DEFAULT 30 CHECK (default_over_limit_seconds > 0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE lag_rules (
  id TEXT PRIMARY KEY,
  cluster_id TEXT NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
  group_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  normal_lag_max INTEGER NOT NULL CHECK (normal_lag_max >= 0),
  high_lag_min INTEGER NOT NULL CHECK (high_lag_min > normal_lag_max),
  over_limit_seconds INTEGER NOT NULL CHECK (over_limit_seconds > 0)
);

CREATE UNIQUE INDEX lag_rules_cluster_group_topic ON lag_rules(cluster_id, group_id, topic);
