package mongo

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/globalsign/mgo/bson"
)

func IsClosedErr(err error) bool {
	if e, ok := err.(*net.OpError); ok {
		if e.Err.Error() == "use of closed network connection" {
			return true
		}
	}
	return false
}

func mustReadUInt32(r io.Reader) (n uint32) {
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		panic(err)
	}
	return
}

func mustReadInt32(r io.Reader) (n int32) {
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		panic(err)
	}
	return
}
func readInt32(r io.Reader) (n int32, err error) {
	err = binary.Read(r, binary.LittleEndian, &n)
	return
}

func readUint32(r io.Reader) (n uint32, err error) {
	err = binary.Read(r, binary.LittleEndian, &n)
	return
}

func readInt64(r io.Reader) *int64 {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}
	return &n
}

func readBytes(r io.Reader, n int) []byte {
	b := make([]byte, n)
	_, err := r.Read(b)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}
	return b
}

func readCString(r io.Reader) string {
	var b []byte
	var one = make([]byte, 1)
	for {
		_, err := r.Read(one)
		if err != nil {
			panic(err)
		}
		if one[0] == '\x00' {
			break
		}
		b = append(b, one[0])
	}
	return string(b)
}

func readOne(r io.Reader) []byte {
	docLen, err := readInt32(r)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}
	buf := make([]byte, int(docLen))
	binary.LittleEndian.PutUint32(buf, uint32(docLen))
	if _, err := io.ReadFull(r, buf[4:]); err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return nil
		}
		panic(err)
	}
	return buf
}

func readDocument(r io.Reader) (m bson.M) {
	if one := readOne(r); one != nil {
		err := bson.Unmarshal(one, &m)
		if err != nil {
			panic(err)
		}
	}
	return m
}

func readDocuments(r io.Reader) (ms []bson.M) {
	for {
		m := readDocument(r)
		if m == nil {
			break
		}
		ms = append(ms, m)
	}
	return
}

func toJson(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("{\"error\":%s}", err.Error())
	}
	return string(b)
}

func currentTime() string {
	layout := "2006/01/02-15:04:05.000000"
	return time.Now().Format(layout)
}
