package exago

import (
	zProto "zmqcontainer"
	zmq "github.com/pebbe/zmq4"
	"github.com/golang/protobuf/proto"
	"log"
	"syscall"
)

type ExaContext struct {
	ConnectionId uint64
	ZSocket *zmq.Socket
	ZMetaMsg *zProto.ExascriptResponse
	ZInfoMsg *zProto.ExascriptResponse
}


func Comm(exaContext ExaContext, reqType zProto.MessageType, expectedTypes []zProto.MessageType, req* zProto.ExascriptRequest) (*zProto.ExascriptResponse) {

	// send request
	if req == nil {
		req = new(zProto.ExascriptRequest)
	}
	req.Type = &reqType;
	req.ConnectionId = &exaContext.ConnectionId;
	reqBytes, err1 := proto.Marshal(req)
	if err1 != nil {
		log.Panic("Failed to compact request")
	}
	_, sendErr := exaContext.ZSocket.SendBytes(reqBytes, 0)
	if sendErr != nil {
		log.Panic("Failed sending zmq message: ", sendErr)
	} else {
		log.Println("Sent message ", req.Type)
	}
	//log.Println("Send message ", req)

	// fetch response
	var respBytes []byte;
	var respErr error;
	for {
		// this cycle is required to avoid 'syscal interrupted' error in tests (when read and write done within same process)
		respBytes, respErr = exaContext.ZSocket.RecvBytes(0);
		if respErr != nil {
			switch zmq.AsErrno(respErr) {
			case zmq.Errno(syscall.EINTR):
				log.Println("Cought EINTR error")
				continue;
			default:
				log.Panic("Failed reading zmq: ", respErr)
			}
		}
		break;
	}
	var respMsg zProto.ExascriptResponse;
	respMarshErr := proto.Unmarshal(respBytes, &respMsg)
	if respMarshErr != nil {
		log.Panic("Failed to parse request ", respMarshErr)
	}
	log.Println("Received message ", respMsg.Type)

	if (*respMsg.Type == zProto.MessageType_MT_CLOSE) {
		if respMsg.Close != nil && respMsg.Close.ExceptionMessage != nil {
			log.Panic("Received close message with error: ", respMsg.Close.ExceptionMessage);
		}
	}

	if (exaContext.ConnectionId != 0 && exaContext.ConnectionId != *respMsg.ConnectionId) {
		log.Panic("Incorrect connection id: ", exaContext.ConnectionId, " / ", *respMsg.ConnectionId);
	}

	//check expected type
	expectedTypeMatched := false
	for _, expectedType := range expectedTypes {
		if *respMsg.Type == expectedType {
			expectedTypeMatched = true
		}
	}
	if !expectedTypeMatched {
		log.Panic("Incorrect response type - expecting ", expectedTypes, ", got ", respMsg.Type)
	}

	return &respMsg;

}