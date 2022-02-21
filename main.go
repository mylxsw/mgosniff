package main

import (
	"flag"
	"fmt"
	"github.com/ma6174/mgosniff/mongo"
	"github.com/mongodb/mongonet"
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

	clean := func() {
		conn.Close()
		dst.Close()
	}

	pr, pw := io.Pipe()
	defer func() {
		pr.Close()
		pw.Close()
	}()

	go func() {
		for {
			message, err := mongonet.ReadMessage(pr)
			if err != nil {
				panic(err)
			}

			fmt.Println(message.Header().OpCode)
			switch message.Header().OpCode {
			case mongonet.OP_MSG, mongonet.OP_MSG_LEGACY:
				msg := message.(*mongonet.MessageMessage)
				bodyDoc, err := msg.BodyDoc()
				if err != nil {
					panic(err)
				}

				log.WithFields(log.Fields{
					"bodyDoc": bodyDoc.String(),
				}).Info("OP_MSG")
			case mongonet.OP_INSERT:
				msg := message.(*mongonet.InsertMessage)
				log.WithFields(log.Fields{
					"namespace": msg.Namespace,
					"docs":      msg.Docs,
				}).Info("OP_INSERT")
			case mongonet.OP_QUERY:
				msg := message.(*mongonet.QueryMessage)
				log.F(log.M{
					"namespace": msg.Namespace,
					"project":   msg.Project,
					"query":     msg.Query,
				}).Info("OP_QUERY")

			}
		}
	}()

	cp := func(dst io.Writer, src io.Reader, srcAddr string, cb func(data []byte)) {
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

			if cb != nil {
				cb(p[:n])
			}

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
	go cp(conn, dst, dst.RemoteAddr().String(), func(data []byte) {
		//pw.Write(data)
	})
	cp(dst, conn, conn.RemoteAddr().String(), func(data []byte) {
		pw.Write(data)
	})
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
