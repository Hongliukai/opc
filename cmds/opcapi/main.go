package main

// To start opcapi in powershell
// & {$ENV:OPC_SERVER="Graybox.Simulator"; $ENV:OPC_NODES="localhost";  go run main.go -addr ":8765"}

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/konimarti/opc"
	"github.com/konimarti/opc/api"
)

var (
	addr      = flag.String("addr", ":8765", "enter address to start api")
	pprofAddr = flag.String("pprof", ":6060", "enter address to start pprof")
	cfgFile   = flag.String("conf", "opcapi.conf", "config file name")
)

type tmlConfig struct {
	Config api.Config `toml:"config"`
	Opc    opcConfig  `toml:"opc"`
}

type opcConfig struct {
	Server string `toml:"server"`
	Nodes  []string
	Tags   []string
}

func main() {
	flag.Parse()

	go func() {
		log.Println("Starting pprof on", *pprofAddr)
		log.Println(http.ListenAndServe(*pprofAddr, nil))
	}()

	opc.Debug()

	// parse config
	data, err := os.ReadFile(*cfgFile)
	if err != nil {
		panic(err)
	}

	// parse config
	var cfg tmlConfig
	if _, err := toml.Decode(string(data), &cfg); err != nil {
		log.Fatal(err)
	}

	server := cfg.Opc.Server
	if server == "" {
		server = strings.Trim(os.Getenv("OPC_SERVER"), " ")
		if server == "" {
			panic("OPC_SERVER not set")
		}
	}
	nodes := cfg.Opc.Nodes
	if len(nodes) == 0 {
		nodes = strings.Split(os.Getenv("OPC_NODES"), ",")
		if len(nodes) == 0 {
			panic("OPC_NODES not set; separate nodes with ','")
		}
	}
	for i := range nodes {
		nodes[i] = strings.Trim(nodes[i], " ")
	}

	log.Println("API starting with OPC", server, nodes, *addr)
	log.Printf("Load Global Config %+v:\n", cfg.Config)

	opc.OPCConfig.Mode = cfg.Config.Mode
	if cfg.Config.ReadSource != 0 {
		opc.OPCConfig.ReadSource = cfg.Config.ReadSource
	}
	opc.OPCConfig.ReadTagsAsServer = cfg.Config.ReadTagsAsServer
	if cfg.Config.ReadTagsAsServer && cfg.Config.ServerReadPeriod > 0 {
		opc.OPCConfig.ServerReadPeriod = cfg.Config.ServerReadPeriod
	}
	log.Printf("Load OPC Config:%+v\n", *opc.OPCConfig)

	if cfg.Config.AllTags {
		log.Println("Enable AllTags, so collecting all tags from server...")
		tree, err := opc.CreateBrowser(server, nodes)
		if err != nil {
			log.Fatal(err)
		}
		cfg.Opc.Tags = opc.CollectTags(tree)
		log.Printf("Collected %d tags from server\n", len(cfg.Opc.Tags))
	}

	log.Println("starting OPC Connection...")
	client, err := opc.NewConnection(
		server,
		nodes,
		cfg.Opc.Tags,
	)

	if err != nil {
		panic(err)
	}
	defer client.Close()

	app := api.App{Config: cfg.Config}
	app.Initialize(client)

	app.Run(*addr)
}
