package main

import (
	"flag"
	"github.com/ma6174/mgosniff/mongo"
	"github.com/mylxsw/asteria/log"
	"io"
	"net"
	"os"
	"sync"
)

var (
	listenAddr = flag.String("l", ":7017", "listen port")
	dstAddr    = flag.String("d", "127.0.0.1:27017", "proxy to dest addr")
	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4096)
		},
	}
)

func handleConn(conn net.Conn) {
	dst, err := net.Dial("tcp", *dstAddr)
	if err != nil {
		log.Errorf("[%s] unexpected err:%v, close connection:%s\n", conn.RemoteAddr(), err, conn.RemoteAddr())
		conn.Close()
		return
	}
	defer dst.Close()
	log.Debugf("[%s] new client connected: %v -> %v -> %v -> %v\n", conn.RemoteAddr(),
		conn.RemoteAddr(), conn.LocalAddr(), dst.LocalAddr(), dst.RemoteAddr())
	parser := mongo.NewParser(conn.RemoteAddr().String(), func(opCode int32, message string, data map[string]interface{}) {
		if opCode == 0 {
			log.WithFields(data).Error(message)
			return
		}

		log.WithFields(data).Info(message)
	})
	clean := func() {
		conn.Close()
		dst.Close()
		parser.Close()
	}
	cp := func(dst io.Writer, src io.Reader, srcAddr string) {
		p := bufferPool.Get().([]byte)
		for {
			n, err := src.Read(p)
			if err != nil {
				if err != io.EOF && !mongo.IsClosedErr(err) {
					log.Errorf("[%s] unexpected error:%v\n", conn.RemoteAddr(), err)
				}
				log.Errorf("[%s] close connection:%s\n", conn.RemoteAddr(), srcAddr)
				clean()
				break
			}

			go parser.Write(p[:n])

			_, err = dst.Write(p[:n])
			if err != nil {
				if err != io.EOF && !mongo.IsClosedErr(err) {
					log.Errorf("[%s] unexpected error:%v\n", conn.RemoteAddr(), err)
				}
				clean()
				break
			}
		}
		bufferPool.Put(p)
	}
	go cp(conn, dst, dst.RemoteAddr().String())
	cp(dst, conn, conn.RemoteAddr().String())
}

func main() {
	flag.Parse()

	log.Debugf("%s listen at %s, proxy to mongodb server %s\n", os.Args[0], *listenAddr, *dstAddr)
	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Errorf("listen failed:", err)
		return
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Errorf("accept connection failed:", err)
			continue
		}
		go handleConn(conn)
	}
}
