// Command mokv runs a mokv node.
package main

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/dynamic-calm/mokv/logger"
	"github.com/dynamic-calm/mokv/mokv"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	defaultBindAddr    = "127.0.0.1:8401"
	defaultRPCPort     = 8400
	defaultMetricsPort = 4000
	serviceName        = "mokv"
)

type cli struct {
	config *mokv.Config
}

func main() {
	app := &cli{}
	cmd := &cobra.Command{
		Use:          "mokv",
		Short:        "A distributed key-value store",
		PreRunE:      app.setupConfig,
		RunE:         app.run,
		SilenceUsage: true,
	}

	if err := setupFlags(cmd); err != nil {
		log.Fatal().Err(err).Msg("failed to setup flags")
	}

	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("failed to execute command")
	}
}

func (c *cli) setupConfig(cmd *cobra.Command, args []string) error {
	c.config = &mokv.Config{}

	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}

	if configFile != "" {
		viper.SetConfigFile(configFile)
		if err := viper.ReadInConfig(); err != nil && !errors.As(err, &viper.ConfigFileNotFoundError{}) {
			return err
		}
	}

	c.config.DataDir = viper.GetString("data-dir")
	c.config.NodeName = viper.GetString("node-name")
	c.config.BindAddr = viper.GetString("bind-addr")
	c.config.RPCPort = viper.GetInt("rpc-port")
	c.config.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	c.config.Bootstrap = viper.GetBool("bootstrap")
	c.config.MetricsPort = viper.GetInt("metrics-port")

	logger.Setup(
		logger.Config{
			Level:       viper.GetString("log-level"),
			Development: getEnvOrDefault("ENV", "production") == "development",
			ServiceName: serviceName,
		},
		os.Stderr,
	)
	return nil
}

func (c *cli) run(cmd *cobra.Command, args []string) error {
	m, err := mokv.New(c.config)
	if err != nil {
		return err
	}
	return m.Listen(cmd.Context())
}

func setupFlags(cmd *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	dataDir := filepath.Join(os.TempDir(), "mokv")

	cmd.Flags().String("config-file", "", "Path to config file.")
	cmd.Flags().String("data-dir", dataDir, "Directory to store KV and Raft data.")
	cmd.Flags().String("node-name", hostname, "Unique server ID.")
	cmd.Flags().String("bind-addr", defaultBindAddr, "Address to bind Serf on.")
	cmd.Flags().Int("rpc-port", defaultRPCPort, "Port for RPC clients (and Raft) connections.")
	cmd.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")
	cmd.Flags().Int("metrics-port", defaultMetricsPort, "Port for metrics server.")
	cmd.Flags().String("log-level", getEnvOrDefault("LOG_LEVEL", "INFO"), "Log level.")

	return viper.BindPFlags(cmd.Flags())
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
