CREATE TABLE feishu_alert_settings (
  cluster_id TEXT PRIMARY KEY REFERENCES clusters(id) ON DELETE CASCADE,
  access_token TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE feishu_alert_rules (
  id TEXT PRIMARY KEY,
  cluster_id TEXT NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
  group_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  lag_threshold INTEGER NOT NULL CHECK (lag_threshold >= 0),
  growth_seconds INTEGER NOT NULL CHECK (growth_seconds >= 10),
  enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(cluster_id, group_id, topic)
);
