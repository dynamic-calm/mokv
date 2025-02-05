package main

import (
	"context"
	"log"
	"os"
	"path"

	"github.com/mateopresacastro/mokv"
	"github.com/mateopresacastro/mokv/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type cli struct {
	cfg Cfg
}

type Cfg struct {
	*mokv.RunnerConfig
	ServerTLSConfig config.TLSConfig
	PeerTLSConfig   config.TLSConfig
}

func main() {
	cli := &cli{}
	cmd := &cobra.Command{
		Use:     "mokv",
		PreRunE: cli.setupConfig,
		RunE:    cli.run,
	}

	if err := setupFlags(cmd); err != nil {
		log.Fatal(err)
	}

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func setupFlags(cmd *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	dataDir := path.Join(os.TempDir(), "mokv")
	cmd.Flags().String("config-file", "", "Path to config file.")
	cmd.Flags().String("data-dir", dataDir, "Directory to store KV and Raft data.")
	cmd.Flags().String("node-name", hostname, "Unique server ID.")
	cmd.Flags().String("bind-addr", "127.0.0.1:8401", "Address to bind Serf on.")
	cmd.Flags().Int("rpc-port", 8400, "Port for RPC clients (and Raft) connections.")
	cmd.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")
	cmd.Flags().String("acl-model-file", "", "Path to ACL model.")
	cmd.Flags().String("acl-policy-file", "", "Path to ACL policy.")
	cmd.Flags().String("server-tls-cert-file", "", "Path to server tls cert.")
	cmd.Flags().String("server-tls-key-file", "", "Path to server tls key.")
	cmd.Flags().String("server-tls-ca-file", "", "Path to server certificate authority.")
	cmd.Flags().String("peer-tls-cert-file", "", "Path to peer tls cert.")
	cmd.Flags().String("peer-tls-key-file", "", "Path to peer tls key.")
	cmd.Flags().String("peer-tls-ca-file", "", "Path to peer certificate authority.")
	cmd.Flags().Int("metrics-port", 4000, "Port for metrics server.")
	return viper.BindPFlags(cmd.Flags())
}

func (cli *cli) setupConfig(cmd *cobra.Command, args []string) error {
	var err error
	if cli.cfg.RunnerConfig == nil {
		cli.cfg.RunnerConfig = &mokv.RunnerConfig{}
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

	cli.cfg.DataDir = viper.GetString("data-dir")
	cli.cfg.NodeName = viper.GetString("node-name")
	cli.cfg.BindAddr = viper.GetString("bind-addr")
	cli.cfg.RPCPort = viper.GetInt("rpc-port")
	cli.cfg.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	cli.cfg.Bootstrap = viper.GetBool("bootstrap")
	cli.cfg.ACLModelFile = viper.GetString("acl-mode-file")
	cli.cfg.ACLPolicyFile = viper.GetString("acl-policy-file")
	cli.cfg.ServerTLSConfig.CertFile = viper.GetString("server-tls-cert-file")
	cli.cfg.ServerTLSConfig.KeyFile = viper.GetString("server-tls-key-file")
	cli.cfg.ServerTLSConfig.CAFile = viper.GetString("server-tls-ca-file")
	cli.cfg.ServerTLSConfig.ServerAddress = viper.GetString("node-name")
	cli.cfg.PeerTLSConfig.CertFile = viper.GetString("peer-tls-cert-file")
	cli.cfg.PeerTLSConfig.KeyFile = viper.GetString("peer-tls-key-file")
	cli.cfg.PeerTLSConfig.CAFile = viper.GetString("peer-tls-ca-file")
	cli.cfg.PeerTLSConfig.ServerAddress = viper.GetString("node-name")
	cli.cfg.MetricsPort = viper.GetInt("metrics-port")

	if cli.cfg.ServerTLSConfig.CertFile != "" && cli.cfg.ServerTLSConfig.KeyFile != "" {
		cli.cfg.ServerTLSConfig.Server = true
		cli.cfg.RunnerConfig.ServerTLSConfig, err = config.SetupTLSConfig(cli.cfg.ServerTLSConfig)
		if err != nil {
			return err
		}
	}

	if cli.cfg.PeerTLSConfig.CertFile != "" && cli.cfg.PeerTLSConfig.KeyFile != "" {
		cli.cfg.RunnerConfig.PeerTLSConfig, err = config.SetupTLSConfig(
			cli.cfg.PeerTLSConfig,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cli *cli) run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	r := mokv.NewRunner(cli.cfg.RunnerConfig, os.Getenv)
	if err := r.Run(ctx); err != nil {
		return err
	}
	return nil
}
