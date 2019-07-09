package exago

import (
	zProto "zmqcontainer" 
	zmq "github.com/pebbe/zmq2"
	"github.com/golang/protobuf/proto"
	"log"
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
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		log.Panic("Failed to compact request")
	}
    exaContext.ZSocket.SendBytes(reqBytes, 0)
    log.Println("Send message ", req.Type)
    //log.Println("Send message ", req)

    if (len(expectedTypes) == 0) {
		log.Println("No response expected")
		return nil;
    }

    respBytes, _ := exaContext.ZSocket.RecvBytes(0);
    var resp zProto.ExascriptResponse;
	err2 := proto.Unmarshal(respBytes, &resp)
	if err2 != nil {
		log.Panic("Failed to parse request")
	}
    log.Println("Received message ", resp.Type)
    //log.Println("Received message (", b, "): ", resp)

    if (*resp.Type == zProto.MessageType_MT_CLOSE) {
    	log.Panic(resp.Close.ExceptionMessage);
    }

    if (exaContext.ConnectionId != 0 && exaContext.ConnectionId != *resp.ConnectionId) {
    	log.Panic("Incorrect connection id: ", exaContext.ConnectionId, " / ", resp.ConnectionId);
    }


   	//check expected type
   	expectedTypeMatched := false
	for _, expectedType := range expectedTypes {
		if *resp.Type == expectedType {
			expectedTypeMatched = true
		}
	}
	if !expectedTypeMatched {
    	log.Panic("Incorrect response type - expecting ", expectedTypes, ", got ", resp.Type)
	}

    return &resp;

}