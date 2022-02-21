package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/mylxsw/asteria/log"
	"io"
	"net"
	"time"
)

type MsgHeader struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32
	OpCode        int32
}

const (
	opReply                  = 1
	opMsg                    = 1000
	opUpdate                 = 2001
	opInsert                 = 2002
	opReserved               = 2003
	opQuery                  = 2004
	opGetMore                = 2005
	opDelete                 = 2006
	opKillCursors            = 2007
	opCommandDeprecated      = 2008
	opCommandReplyDeprecated = 2009
	opCommand                = 2010
	opCommandReply           = 2011
	opMsgNew                 = 2013
)

func main() {
	fmt.Println(len(data))

	reader := bytes.NewBuffer(data)

	for {
		header := MsgHeader{}
		if err := binary.Read(reader, binary.LittleEndian, &header); err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}

		limitReader := io.LimitReader(reader, int64(header.MessageLength-4*4))
		fmt.Printf("----------- %d -------------\n", header.OpCode)
		switch header.OpCode {
		case opQuery:
			parseOpQuery(header, limitReader)
		case opReply:
			parseOpReply(header, limitReader)
		case opMsgNew:
			parseOpMsgNew(header, limitReader)
		default:
			return
		}
	}

}

func parseOpMsgNew(header MsgHeader, reader io.Reader) {
	// flag:
	// 0 - checksumPresent: The message ends with 4 bytes containing a CRC-32C [2] checksum
	// 1 - moreToCome: Another message will follow this one without further action from the receiver.
	//		The receiver MUST NOT send another message until receiving one with moreToCome set to 0 as
	//		sends may block, causing deadlock. Requests with the moreToCome bit set will not receive a reply.
	//		Replies will only have this set in response to requests with the exhaustAllowed bit set
	// 16 - exhaustAllowed: The client is prepared for multiple replies to this request using the moreToCome bit.
	//		The server will never produce replies with the moreToCome bit set unless the request has this bit set.
	flag := mustReadInt32(reader)
	for {
		t := readBytes(reader, 1)
		if t == nil {
			break
		}
		switch t[0] {
		case 0: // body
			body := toJson(readDocument(reader))
			checksum, _ := readUint32(reader)

			log.F(log.M{
				"flag":       flag,
				"type":       0,
				"body":       body,
				"request_id": header.RequestID,
				"checksum":   checksum,
			}).Infof("OP_MSG_NEW")
		case 1:
			sectionSize := mustReadInt32(reader)
			r1 := io.LimitReader(reader, int64(sectionSize))
			documentSequenceIdentifier := readCString(r1)
			objects := toJson(readDocuments(r1))

			log.F(log.M{
				"flag":                       flag,
				"type":                       1,
				"request_id":                 header.RequestID,
				"documentSequenceIdentifier": documentSequenceIdentifier,
				"objects":                    objects,
			}).Infof("OP_MSG_NEW")
		default:
			panic(fmt.Sprintf("unknown body kind: %v", t[0]))
		}
	}
}

func parseOpReply(header MsgHeader, reader io.Reader) {
	flag := mustReadInt32(reader)
	cursorID := readInt64(reader)
	startingFrom := mustReadInt32(reader)
	numberReturned := mustReadInt32(reader)
	docs := readDocuments(reader)
	var docsStr string
	if len(docs) == 1 {
		docsStr = toJson(docs[0])
	} else {
		docsStr = toJson(docs)
	}

	log.F(log.M{
		"header.ResponseTo": header.ResponseTo,
		"flag":              flag,
		"cursorID":          cursorID,
		"startingFrom":      startingFrom,
		"numberReturned":    numberReturned,
		"docsStr":           docsStr,
	}).Infof("OP_REPLY")
}

func parseOpQuery(header MsgHeader, reader io.Reader) {
	flag := mustReadInt32(reader)
	fullCollectionName := readCString(reader)
	numberToSkip := mustReadInt32(reader)
	numberToReturn := mustReadInt32(reader)
	query := toJson(readDocument(reader))
	selector := toJson(readDocument(reader))

	log.F(log.M{
		"request_id":         header.RequestID,
		"fullCollectionName": fullCollectionName,
		"numberToSkip":       numberToSkip,
		"numberToReturn":     numberToReturn,
		"flag":               flag,
		"query":              query,
		"selector":           selector,
	}).Infof("OP_QUERY")
}

func IsClosedErr(err error) bool {
	if e, ok := err.(*net.OpError); ok {
		if e.Err.Error() == "use of closed network connection" {
			return true
		}
	}
	return false
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
