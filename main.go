package main

import (
	"os"
	"path/filepath"

	"github.com/dynamic-calm/mokv/mokv"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	defaultBindAddr    = "127.0.0.1:8401"
	defaultRPCPort     = 8400
	defaultMetricsPort = 4000
)

// CLI represents the command-line interface application context and configuration.
type CLI struct {
	config *mokv.Config
}

func main() {
	app := &CLI{}
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

// setupConfig initializes the configuration by reading from the config file (if provided)
// and merging flag values into the application configuration struct.
func (c *CLI) setupConfig(cmd *cobra.Command, args []string) error {
	c.config = &mokv.Config{}

	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}

	if configFile != "" {
		viper.SetConfigFile(configFile)
		if err := viper.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return err
			}
		}
	}

	c.config.DataDir = viper.GetString("data-dir")
	c.config.NodeName = viper.GetString("node-name")
	c.config.BindAddr = viper.GetString("bind-addr")
	c.config.RPCPort = viper.GetInt("rpc-port")
	c.config.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	c.config.Bootstrap = viper.GetBool("bootstrap")
	c.config.MetricsPort = viper.GetInt("metrics-port")

	return nil
}

// Run executes the main application logic, starting mokv service.
func (c *CLI) run(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	service, err := mokv.New(c.config, os.Getenv)
	if err != nil {
		return err
	}
	return service.Listen(ctx)
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

	return viper.BindPFlags(cmd.Flags())
}
