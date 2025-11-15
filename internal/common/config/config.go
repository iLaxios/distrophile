package config

import (
	"log"

	"github.com/spf13/viper"
)

func Load() (*Config, error) {
	v := viper.New()

	// config file defaults
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./config")
	v.AddConfigPath("/etc/distrophile/")

	// Automatically read ENV variables
	v.AutomaticEnv()

	// ENV key -> lowercase with underscore
	v.SetEnvPrefix("DP") // DP_ENV, DP_COORDINATOR_PORT etc.
	v.BindEnv("env")
	v.BindEnv("coordinator_port")
	v.BindEnv("storage_node_port")
	v.BindEnv("data_dir")
	v.BindEnv("replication_factor")

	// Read config file (not mandatory)
	if err := v.ReadInConfig(); err != nil {
		log.Println("No config file found, relying on ENV only")
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
