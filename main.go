package main

import (
	"flag"
	"fmt"
	"github.com/ma6174/mgosniff/mongo"
	"io"
	"log"
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
		log.Printf("[%s] unexpected err:%v, close connection:%s\n", conn.RemoteAddr(), err, conn.RemoteAddr())
		conn.Close()
		return
	}
	defer dst.Close()
	log.Printf("[%s] new client connected: %v -> %v -> %v -> %v\n", conn.RemoteAddr(),
		conn.RemoteAddr(), conn.LocalAddr(), dst.LocalAddr(), dst.RemoteAddr())
	parser := mongo.NewParser(conn.RemoteAddr().String(), func(message string) {
		fmt.Println(message)
	})
	parser2 := mongo.NewParser(conn.RemoteAddr().String(), func(message string) {

	})
	teeReader := io.TeeReader(conn, parser)
	teeReader2 := io.TeeReader(dst, parser2)
	clean := func() {
		conn.Close()
		dst.Close()
		parser.Close()
		parser2.Close()
	}
	cp := func(dst io.Writer, src io.Reader, srcAddr string) {
		p := bufferPool.Get().([]byte)
		for {
			n, err := src.Read(p)
			if err != nil {
				if err != io.EOF && !mongo.IsClosedErr(err) {
					log.Printf("[%s] unexpected error:%v\n", conn.RemoteAddr(), err)
				}
				log.Printf("[%s] close connection:%s\n", conn.RemoteAddr(), srcAddr)
				clean()
				break
			}
			_, err = dst.Write(p[:n])
			if err != nil {
				if err != io.EOF && !mongo.IsClosedErr(err) {
					log.Printf("[%s] unexpected error:%v\n", conn.RemoteAddr(), err)
				}
				clean()
				break
			}
		}
		bufferPool.Put(p)
	}
	go cp(conn, teeReader2, dst.RemoteAddr().String())
	cp(dst, teeReader, conn.RemoteAddr().String())
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	log.Printf("%s listen at %s, proxy to mongodb server %s\n", os.Args[0], *listenAddr, *dstAddr)
	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatal("listen failed:", err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept connection failed:", err)
			continue
		}
		go handleConn(conn)
	}
}
