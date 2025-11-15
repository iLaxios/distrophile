package config

type Config struct {
	Env               string `mapstructure:"env"`
	CoordinatorPort   string `mapstructure:"coordinator_port"`
	StorageNodePort   string `mapstructure:"storage_node_port"`
	DataDir           string `mapstructure:"data_dir"`
	ReplicationFactor int    `mapstructure:"replication_factor"`

	// mongo configs
	MongoURI string `mapstructure:"mongo_uri"`
	MongoDB  string `mapstructure:"mongo_db"`
}
