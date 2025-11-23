package config

type Config struct {
	Env               string `mapstructure:"env"`
	CoordinatorAddr   string `mapstructure:"coordinator_addr"`
	CoordinatorPort   string `mapstructure:"coordinator_port"`
	StorageNodePort   string `mapstructure:"storage_node_port"`
	DataDir           string `mapstructure:"data_dir"`
	ReplicationFactor int    `mapstructure:"replication_factor"`

	// Heartbeat configuration
	HeartbeatIntervalSec int `mapstructure:"heartbeat_interval_sec"`
	HeartbeatTimeoutSec  int `mapstructure:"heartbeat_timeout_sec"`

	// MongoDB
	MongoURI string `mapstructure:"mongo_uri"`
	MongoDB  string `mapstructure:"mongo_db"`

	// Redis
	RedisAddr string `mapstructure:"redis_addr"`
}
