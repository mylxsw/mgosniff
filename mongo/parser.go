package mongo

import (
	"encoding/binary"
	"fmt"
	"github.com/mylxsw/asteria/log"
	"io"
	"io/ioutil"
	"runtime/debug"
)

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

type msgHeader struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32
	OpCode        int32
}

type Parser struct {
	pipeWriter       *io.PipeWriter
	remoteAddr       string
	pipeWriterClosed bool
	recorder         func(opCode int32, message string, data map[string]interface{})
}

func NewParser(remoteAddr string, recorder func(opCode int32, message string, data map[string]interface{})) *Parser {
	pr, pw := io.Pipe()
	parser := &Parser{
		pipeWriter: pw,
		remoteAddr: remoteAddr,
		recorder:   recorder,
	}
	go parser.Parse(pr)
	return parser
}

func (parser *Parser) Write(p []byte) (n int, err error) {
	if parser.pipeWriterClosed {
		return len(p), nil
	}
	return parser.pipeWriter.Write(p)
}

func (parser *Parser) Close() {
	parser.pipeWriterClosed = true
	_ = parser.pipeWriter.Close()
}

func (parser *Parser) writeParsedMessage(opCode int32, message string, data map[string]interface{}) {
	switch opCode {
	case opCommand, opDelete, opInsert, opQuery, opUpdate, opMsg, opMsgNew:
		if data == nil {
			data = log.Fields{}
		}

		data["opCode"] = opCode
		parser.recorder(opCode, message, data)
	default:
	}
}

func (parser *Parser) writeErrorMessage(message string) {
	parser.recorder(0, message, log.Fields{
		"opCode": 0,
	})
}

func (parser *Parser) Parse(r *io.PipeReader) {
	defer func() {
		if e := recover(); e != nil {
			parser.writeErrorMessage(fmt.Sprintf("parser failed, painc: %v\n", e))
			debug.PrintStack()
			parser.pipeWriterClosed = true
			_ = parser.pipeWriter.Close()
		}
	}()
	for {
		header := msgHeader{}
		err := binary.Read(r, binary.LittleEndian, &header)
		if err != nil {
			if err != io.EOF {
				parser.writeErrorMessage(fmt.Sprintf("unexpected error:%v\n", err))
			}
			break
		}
		rd := io.LimitReader(r, int64(header.MessageLength-4*4))
		switch header.OpCode {
		case opQuery:
			parser.parseQuery(header, rd)
		case opInsert:
			parser.parseInsert(header, rd)
		case opDelete:
			parser.parseDelete(header, rd)
		case opUpdate:
			parser.parseUpdate(header, rd)
		case opMsg:
			parser.parseMsg(header, rd)
		case opReply:
			parser.parseReply(header, rd)
		case opGetMore:
			parser.parseGetMore(header, rd)
		case opKillCursors:
			parser.parseKillCursors(header, rd)
		case opReserved:
			parser.parseReserved(header)
		case opCommandDeprecated:
			parser.parseCommandDeprecated(header, rd)
		case opCommandReplyDeprecated:
			parser.parseCommandReplyDeprecated(header, rd)
		case opCommand:
			parser.parseCommand(header, rd)
		case opCommandReply:
			parser.parseCommandReply(header, rd)
		case opMsgNew:
			parser.parseMsgNew(header, rd)
		default:
			parser.writeErrorMessage(fmt.Sprintf("unknown OpCode: %d", header.OpCode))
			_, err = io.Copy(ioutil.Discard, rd)
			if err != nil {
				parser.writeErrorMessage(fmt.Sprintf("read failed: %v", err))
				break
			}
		}
	}
}

func (parser *Parser) parseQuery(header msgHeader, r io.Reader) {
	flag := mustReadInt32(r)
	fullCollectionName := readCString(r)
	numberToSkip := mustReadInt32(r)
	numberToReturn := mustReadInt32(r)
	query := toJson(readDocument(r))
	selector := toJson(readDocument(r))
	parser.writeParsedMessage(
		header.OpCode,
		fmt.Sprintf("QUERY id:%d coll:%s toskip:%d toret:%d flag:%b query:%v sel:%v\n",
			header.RequestID,
			fullCollectionName,
			numberToSkip,
			numberToReturn,
			flag,
			query,
			selector),
		nil,
	)
}

func (parser *Parser) parseInsert(header msgHeader, r io.Reader) {
	flag := mustReadInt32(r)
	fullCollectionName := readCString(r)
	docs := readDocuments(r)
	var docsStr string
	if len(docs) == 1 {
		docsStr = toJson(docs[0])
	} else {
		docsStr = toJson(docs)
	}
	parser.writeParsedMessage(header.OpCode, fmt.Sprintf("INSERT id:%d coll:%s flag:%b docs:%v\n",
		header.RequestID, fullCollectionName, flag, docsStr), nil)
}

func (parser *Parser) parseUpdate(header msgHeader, r io.Reader) {
	_ = mustReadInt32(r)
	fullCollectionName := readCString(r)
	flag := mustReadInt32(r)
	selector := toJson(readDocument(r))
	update := toJson(readDocument(r))
	parser.writeParsedMessage(header.OpCode, fmt.Sprintf("UPDATE id:%d coll:%s flag:%b sel:%v update:%v\n",
		header.RequestID, fullCollectionName, flag, selector, update), nil)
}

func (parser *Parser) parseGetMore(header msgHeader, r io.Reader) {
	_ = mustReadInt32(r)
	fullCollectionName := readCString(r)
	numberToReturn := mustReadInt32(r)
	cursorID := readInt64(r)
	parser.writeParsedMessage(header.OpCode, fmt.Sprintf("GETMORE id:%d coll:%s toret:%d curID:%d\n",
		header.RequestID, fullCollectionName, numberToReturn, cursorID), nil)
}

func (parser *Parser) parseDelete(header msgHeader, r io.Reader) {
	_ = mustReadInt32(r)
	fullCollectionName := readCString(r)
	flag := mustReadInt32(r)
	selector := toJson(readDocument(r))
	parser.writeParsedMessage(header.OpCode, fmt.Sprintf("DELETE id:%d coll:%s flag:%b sel:%v \n",
		header.RequestID, fullCollectionName, flag, selector), nil)
}

func (parser *Parser) parseKillCursors(header msgHeader, r io.Reader) {
	_ = mustReadInt32(r)
	numberOfCursorIDs := mustReadInt32(r)
	var cursorIDs []int64
	for {
		n := readInt64(r)
		if n != nil {
			cursorIDs = append(cursorIDs, *n)
			continue
		}
		break
	}
	parser.writeParsedMessage(header.OpCode, fmt.Sprintf("KILLCURSORS id:%d numCurID:%d curIDs:%d\n",
		header.RequestID, numberOfCursorIDs, cursorIDs), nil)
}

func (parser *Parser) parseReply(header msgHeader, r io.Reader) {
	flag := mustReadInt32(r)
	cursorID := readInt64(r)
	startingFrom := mustReadInt32(r)
	numberReturned := mustReadInt32(r)
	docs := readDocuments(r)
	var docsStr string
	if len(docs) == 1 {
		docsStr = toJson(docs[0])
	} else {
		docsStr = toJson(docs)
	}
	parser.writeParsedMessage(header.OpCode, fmt.Sprintf("REPLY to:%d flag:%b curID:%d from:%d reted:%d docs:%v\n",
		header.ResponseTo,
		flag,
		cursorID,
		startingFrom,
		numberReturned,
		docsStr), nil)
}

func (parser *Parser) parseMsg(header msgHeader, r io.Reader) {
	msg := readCString(r)
	parser.writeParsedMessage(header.OpCode, fmt.Sprintf("MSG %d %s\n", header.RequestID, msg), log.Fields{
		"msg": msg,
	})
}
func (parser *Parser) parseReserved(header msgHeader) {
	parser.writeParsedMessage(header.OpCode, fmt.Sprintf("RESERVED header:%v data:%v\n", header.RequestID, toJson(header)), nil)
}

func (parser *Parser) parseCommandDeprecated(header msgHeader, r io.Reader) {
	parser.writeParsedMessage(header.OpCode, fmt.Sprintf("msgHeader %v\n", toJson(header)), nil)
	// TODO: no document, current not understand
	_, err := io.Copy(ioutil.Discard, r)
	if err != nil {
		parser.writeErrorMessage(fmt.Sprintf("read failed: %v", err))
		return
	}
}
func (parser *Parser) parseCommandReplyDeprecated(header msgHeader, r io.Reader) {
	parser.writeParsedMessage(header.OpCode, fmt.Sprintf("msgHeader %v\n", toJson(header)), nil)
	// TODO: no document, current not understand
	_, err := io.Copy(ioutil.Discard, r)
	if err != nil {
		parser.writeErrorMessage(fmt.Sprintf("read failed: %v", err))
		return
	}
}
func (parser *Parser) parseCommand(header msgHeader, r io.Reader) {
	database := readCString(r)
	commandName := readCString(r)
	metadata := toJson(readDocument(r))
	commandArgs := toJson(readDocument(r))
	inputDocs := toJson(readDocuments(r))
	parser.writeParsedMessage(
		header.OpCode,
		fmt.Sprintf("COMMAND id:%v db:%v meta:%v cmd:%v args:%v docs %v\n",
			header.RequestID,
			database,
			metadata,
			commandName,
			commandArgs,
			inputDocs),
		nil)
}

func (parser *Parser) parseMsgNew(header msgHeader, r io.Reader) {
	mustReadInt32(r)
	for {
		t := readBytes(r, 1)
		if t == nil {
			break
		}
		switch t[0] {
		case 0: // body
			body := toJson(readDocument(r))
			checksum, _ := readUint32(r)
			parser.writeParsedMessage(
				header.OpCode,
				fmt.Sprintf("MSG id: %v checksum: %v", header.RequestID, checksum),
				log.Fields{
					"type": 0,
					"body": body,
				},
			)
		case 1:
			sectionSize := mustReadInt32(r)
			r1 := io.LimitReader(r, int64(sectionSize))
			documentSequenceIdentifier := readCString(r1)
			objects := toJson(readDocuments(r1))
			parser.writeParsedMessage(
				header.OpCode,
				fmt.Sprintf("MSG id: %v objects: %v",
					header.RequestID,
					objects,
				),
				log.Fields{
					"type":                       1,
					"documentSequenceIdentifier": documentSequenceIdentifier,
					"objects":                    objects,
				},
			)
		default:
			parser.writeErrorMessage(fmt.Sprintf("unknown body kind: %v", t[0]))
		}
	}
}

func (parser *Parser) parseCommandReply(header msgHeader, r io.Reader) {
	metadata := toJson(readDocument(r))
	commandReply := toJson(readDocument(r))
	outputDocs := toJson(readDocument(r))
	parser.writeParsedMessage(
		header.OpCode,
		fmt.Sprintf("COMMANDREPLY to:%d id:%v meta:%v cmdReply:%v outputDocs:%v\n",
			header.ResponseTo, header.RequestID, metadata, commandReply, outputDocs),
		log.Fields{
			"meta":          metadata,
			"command_reply": commandReply,
			"output_docs":   outputDocs,
		},
	)
}
