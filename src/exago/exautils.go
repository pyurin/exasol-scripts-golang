package exago

import (
	zProto "zmqcontainer"
	zmq "github.com/pebbe/zmq2"
	"github.com/golang/protobuf/proto"
	"log"
	"syscall"
)

type ExaContext struct {
	ConnectionId uint64
	ExaMeta map[string]string
	ZSocket *zmq.Socket
	ZMetaMsg *zProto.ExascriptResponse
	ZInfoMsg *zProto.ExascriptResponse
}


func Comm(exaContext ExaContext, reqType zProto.MessageType, expectedTypes []zProto.MessageType, req* zProto.ExascriptRequest) (*zProto.ExascriptResponse) {
	if req == nil {
		req = new(zProto.ExascriptRequest)
	}
	req.Type = &reqType;
	req.ConnectionId = &exaContext.ConnectionId;
	reqBytes, err1 := proto.Marshal(req)
	if err1 != nil {
		log.Panic("Failed to compact request")
	}
	exaContext.ZSocket.SendBytes(reqBytes, 0)
	log.Println("Send message ", req.Type)
	//log.Println("Send message ", req)

	if (len(expectedTypes) == 0) {
		log.Println("No response expected, got ", req.Type)
		return nil;
	}
	var inZMsgBytes []byte;
	var err2 error;
	for {
		inZMsgBytes, err2 = exaContext.ZSocket.RecvBytes(0);
		if err2 != nil {
			switch zmq.AsErrno(err2) {
			case zmq.Errno(syscall.EINTR):
				log.Println("Cought EINTR error")
				continue;
			default:
				log.Panic("Failed reading zmq: ", err2)
			}
		}
		break;
	}
	var inZMsg zProto.ExascriptResponse;
	err3 := proto.Unmarshal(inZMsgBytes, &inZMsg)
	if err3 != nil {
		log.Panic("Failed to parse request")
	}
	log.Println("Received message ", inZMsg.Type)
	//log.Println("Received message (", b, "): ", inZMsgBuf)

	if (*inZMsg.Type == zProto.MessageType_MT_CLOSE) {
		if inZMsg.Close != nil && inZMsg.Close.ExceptionMessage != nil {
			log.Panic(inZMsg.Close.ExceptionMessage);
		}
	}

	if (exaContext.ConnectionId != 0 && exaContext.ConnectionId != *inZMsg.ConnectionId) {
		log.Panic("Incorrect connection id: ", exaContext.ConnectionId, " / ", *inZMsg.ConnectionId);
	}


	//check expected type
	expectedTypeMatched := false
	for _, expectedType := range expectedTypes {
		if *inZMsg.Type == expectedType {
			expectedTypeMatched = true
		}
	}
	if !expectedTypeMatched {
		log.Panic("Incorrect response type - expecting ", expectedTypes, ", got ", inZMsg.Type)
	}

	return &inZMsg;

}