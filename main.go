package main

import (
	"context"
	"net/http"
	"os"
	"path"

	_ "net/http/pprof"

	"github.com/dynamic-calm/mokv/mokv"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type cli struct {
	Cfg
}

type Cfg struct {
	*mokv.Config
}

func main() {
	cli := &cli{}
	cmd := &cobra.Command{
		Use:     "mokv",
		PreRunE: cli.setupConfig,
		RunE:    cli.run,
	}

	if err := setupFlags(cmd); err != nil {
		log.Fatal().Err(err).Msg("error setting up flags")
	}

	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("error executing command")
	}
}

func setupFlags(cmd *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	dataDir := path.Join(os.TempDir(), "mokv")
	cmd.Flags().String("config-file", "", "Path to config file.")
	cmd.Flags().String("data-dir", dataDir, "Directory to store KV and Raft data.")
	cmd.Flags().String("node-name", hostname, "Unique server ID.")
	cmd.Flags().String("bind-addr", "127.0.0.1:8401", "Address to bind Serf on.")
	cmd.Flags().Int("rpc-port", 8400, "Port for RPC clients (and Raft) connections.")
	cmd.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")
	cmd.Flags().Int("metrics-port", 4000, "Port for metrics server.")
	return viper.BindPFlags(cmd.Flags())
}

func (cli *cli) setupConfig(cmd *cobra.Command, args []string) error {
	var err error
	if cli.Config == nil {
		cli.Config = &mokv.Config{}
	}

	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}
	viper.SetConfigFile(configFile)
	if err = viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	cli.DataDir = viper.GetString("data-dir")
	cli.NodeName = viper.GetString("node-name")
	cli.BindAddr = viper.GetString("bind-addr")
	cli.RPCPort = viper.GetInt("rpc-port")
	cli.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	cli.Bootstrap = viper.GetBool("bootstrap")
	cli.MetricsPort = viper.GetInt("metrics-port")

	return nil
}

func (cli *cli) run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	mokv, err := mokv.New(cli.Config, os.Getenv)
	if err != nil {
		return err
	}

	go func() {
		log.Info().Str("addr", "loclahost:6060").Msg("starting pprof server")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Error().Err(err).Msg("pprof server failed")
		}
	}()

	if err := mokv.Listen(ctx); err != nil {
		return err
	}
	return nil
}
