package main

import (
	"flag"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/siddontang/go-mysql/server"
)

var (
	flagListenAddr = flag.String("listen", ":4000", "the caching proxy listen address")
	flagDataDir    = flag.String("data", "./data", "the databases directory")
	flagOptions    = flag.String("options", "cache=shared&_journal=memory", "the default database options (uri style)")
	flagRoot       = flag.String("root", "root:root", "the default root info (user:secret)")
	flagVerbose    = flag.Bool("verbose", false, "verbose to the output")
)

var (
	store *Store
)

func init() {
	flag.Parse()

	*flagDataDir, _ = filepath.Abs(*flagDataDir)

	os.MkdirAll(*flagDataDir, 0755)

	s, e := NewStore(*flagDataDir)
	if e != nil {
		log.Fatal(e.Error())
	}

	store = s
}

func main() {
	l, e := net.Listen("tcp", *flagListenAddr)
	if e != nil {
		log.Fatal(e.Error())
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println(err.Error())
			continue
		}

		go (func(remoteConn net.Conn) {
			s, e := NewSessionHandler(store)
			if e != nil {
				return
			}

			parts := strings.SplitN(*flagRoot, ":", 2)
			if len(parts) < 2 {
				parts = append(parts, "")
			}

			conn, err := server.NewConn(remoteConn, parts[0], parts[1], s)
			if err != nil {
				return
			}

			for {
				if err := conn.HandleCommand(); err != nil {
					break
				}
			}
		})(conn)
	}
}
