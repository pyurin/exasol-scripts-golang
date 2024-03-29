package main

/**
 Self-sufficient test (w/o exasol server) - emulates exasol server by sending synthetic zmq messages.
 Could be useful for benchmarks, not used in travis.
 */

import (
        "testing"
        //        "syscall"
        //       "os/exec"
        "log"
        zProto "exago/zmqcontainer"
        zmq "github.com/pebbe/zmq4"
        "github.com/golang/protobuf/proto"
        "reflect"
        "syscall"
        //"unsafe"
        "errors"
        "strings"
        "exago"
        "io/ioutil"
        "fmt"
        apd "github.com/cockroachdb/apd"
)

const ZSOCKADDR = "ipc:///tmp/zmqvmcontainer_conn_2240680559578752105"


func initZSocket() *zmq.Socket {
        log.Println("EMU Starting zmq server")
        zSock, _ := zmq.NewSocket(zmq.REP)
        zSock.Bind(ZSOCKADDR)
        return zSock
}

func readMsgOrFatal(fatalFunc func(args ...interface{}), zSock *zmq.Socket, flags zmq.Flag, expectedMessageTypes []zProto.MessageType) (*zProto.ExascriptRequest) {
        msg, err := readMsg(zSock, flags, expectedMessageTypes);
        if err != nil {
                fatalFunc(err)
        }
        return msg;
}

func readMsg(zSock *zmq.Socket, flags zmq.Flag, expectedMessageTypes []zProto.MessageType) (*zProto.ExascriptRequest, error) {
        var inMsgBytes []byte;
        var err error;
        // @todo is recv bytes optimal way to fetch zmq?
        for {
                inMsgBytes, err = zSock.RecvBytes(flags);
                if err != nil {
                        switch zmq.AsErrno(err) {
                        case zmq.Errno(syscall.EINTR):
                                log.Println("EMU Cought EINTR error")
                                continue;
                        default:
                                log.Panic("EMU Failed reading zmq: ", err, err.(syscall.Errno))
                        }
                }
                break;
        }
        var inMsg zProto.ExascriptRequest;
        err2 := proto.Unmarshal(inMsgBytes, &inMsg)
        if err2 != nil {
                log.Panic("EMU Failed to parse request ", inMsgBytes)
        }
        log.Println("EMU Fetched: ", inMsg.Type)
        if (*inMsg.Type == zProto.MessageType_MT_CLOSE) {
                writeSimpleMsg(zSock, zProto.MessageType_MT_FINISHED)
                log.Println("EMU Fetched exception message: ", *inMsg.Close.ExceptionMessage)
                return nil, errors.New(*inMsg.Close.ExceptionMessage)
        }
        for _, mType := range expectedMessageTypes {
                if *inMsg.Type == mType {
                        return &inMsg, nil
                }
        }
        log.Panic("EMU Unexpected message type ", *inMsg.Type, " when expected: ", expectedMessageTypes);
        return nil, nil;
}

func writeMsg(zSock *zmq.Socket, outMsg *zProto.ExascriptResponse) {
        connectionId := uint64(1);
        outMsg.ConnectionId = &connectionId;
        outBytes, err := proto.Marshal(outMsg)
        if err != nil {
                log.Panic("Failed to compact request", err)
        }
        zSock.SendBytes(outBytes, 0)
        log.Println("EMU Send message ", outMsg.Type)
}

func writeInfoMsg(zSock *zmq.Socket, scriptName string, sourceCode string) {
        dummyS := "dummy_string"
        dummyUint32 := uint32(1)
        dummyUint64 := uint64(1)
        msg := new (zProto.ExascriptResponse);
        m1T := zProto.MessageType_MT_INFO;
        msg.Type = &m1T;
        msg.Info = new (zProto.ExascriptInfo);
        msg.Info.ScriptName = &scriptName;
        msg.Info.SourceCode = &sourceCode;

        msg.Info.DatabaseName = &dummyS;
        msg.Info.DatabaseVersion = &dummyS;
        msg.Info.SessionId = &dummyUint64;
        msg.Info.StatementId = &dummyUint32;
        msg.Info.NodeCount = &dummyUint32;
        msg.Info.NodeId = &dummyUint32;
        msg.Info.VmId = &dummyUint64;
        msg.Info.MaximalMemoryLimit = &dummyUint64;
        msg.Info.ScriptSchema = &dummyS;

        writeMsg(zSock, msg)
}

func writeMetaMsg(zSock *zmq.Socket, inIterMulti bool, scriptInRowPrototype []interface{}, outIterMulti bool, scriptOutRowPrototype []interface{}) {
        metaMsg := new (zProto.ExascriptResponse)
        metaMsgT := zProto.MessageType_MT_META
        metaMsg.Type = &metaMsgT
        metaMsg.Meta = new (zProto.ExascriptMetadata)

        var metaMsgInIterType, metaMsgOutIterType zProto.IterType
        if inIterMulti {
                metaMsgInIterType = zProto.IterType_PB_MULTIPLE
        } else {
                metaMsgInIterType = zProto.IterType_PB_EXACTLY_ONCE
        }
        if outIterMulti {
                metaMsgOutIterType = zProto.IterType_PB_MULTIPLE
        } else {
                metaMsgOutIterType = zProto.IterType_PB_EXACTLY_ONCE
        }
        metaMsg.Meta.InputIterType = &metaMsgInIterType
        metaMsg.Meta.OutputIterType = &metaMsgOutIterType
        metaMsgSingleCallMode := false
        metaMsg.Meta.SingleCallMode = &metaMsgSingleCallMode

        //        metaMsg.Meta.InputColumns = new (zProto.ExascriptMetadataColumnDefinition)
        var typeInt64name = zProto.ColumnType_name[int32(zProto.ColumnType_PB_INT64)]
        var typeInt64 = zProto.ColumnType_PB_INT64
        var typeStringName = zProto.ColumnType_name[int32(zProto.ColumnType_PB_STRING)]
        var typeString = zProto.ColumnType_PB_STRING
        parseRowPrototype := func(colsDefinition *[]*zProto.ExascriptMetadataColumnDefinition, rowPrototype []interface{}) {
                for fieldI, field := range rowPrototype {
                        *colsDefinition = append(*colsDefinition, new (zProto.ExascriptMetadataColumnDefinition))
                        switch field.(type) {
                        case int64:
                                (*colsDefinition)[fieldI].Type = &typeInt64
                                (*colsDefinition)[fieldI].TypeName = &typeInt64name
                        case string:
                                (*colsDefinition)[fieldI].Type = &typeString
                                (*colsDefinition)[fieldI].TypeName = &typeStringName
                        default:
                                log.Panic("Not ready to use type ", reflect.TypeOf(field), " in writemetamsg")
                        }
                        var colName = "col" + string(fieldI)
                        (*colsDefinition)[fieldI].Name = &colName
                }
        }
        parseRowPrototype(&metaMsg.Meta.InputColumns, scriptInRowPrototype)
        parseRowPrototype(&metaMsg.Meta.OutputColumns, scriptOutRowPrototype)

        writeMsg(zSock, metaMsg)
}

func writeSimpleMsg(zSock *zmq.Socket, msgType zProto.MessageType) {
        msg := new (zProto.ExascriptResponse)
        msgTypeLocal := msgType
        msg.Type = &msgTypeLocal
        writeMsg(zSock, msg)
}

func writeDataMessage(zSock *zmq.Socket, rows *[][]interface{}) {
        if (len(*rows) == 0) {
                log.Panic("Need row to be non-empt")
        }
        msg := new (zProto.ExascriptResponse)
        msgTypeLocal := zProto.MessageType_MT_NEXT
        msg.Type = &msgTypeLocal

        msg.Next = new (zProto.ExascriptNextDataRep)
        msg.Next.Table = new (zProto.ExascriptTableData)

        numOfRows := uint64(len(*rows))
        msg.Next.Table.Rows = &numOfRows
        msg.Next.Table.RowsInGroup = &numOfRows
        for rowI, row := range *rows {
                for _, field := range row {
                        switch field.(type) {
                        case nil:
                                msg.Next.Table.DataNulls = append(msg.Next.Table.DataNulls, true)
                        case int64:
                                msg.Next.Table.DataNulls = append(msg.Next.Table.DataNulls, false)
                                msg.Next.Table.DataInt64 = append(msg.Next.Table.DataInt64, (field.(int64)))
                        case string:
                                msg.Next.Table.DataNulls = append(msg.Next.Table.DataNulls, false)
                                msg.Next.Table.DataString = append(msg.Next.Table.DataString, (field.(string)))
                        default:
                                log.Panic("Not ready to use type ", reflect.TypeOf(field), " in writedatamsg")
                        }
                }
                msg.Next.Table.RowNumber = append(msg.Next.Table.RowNumber, uint64(rowI))
        }

        writeMsg(zSock, msg)
}

func writeCommunicationInitialization(fatalFunc func(args ...interface{}), zSock *zmq.Socket, scriptName string, sourceCode string, inIterMulti bool, scriptInRowPrototype []interface{}, outIterMulti bool, scriptOutRowPrototype []interface{}) {
        readMsgOrFatal(fatalFunc, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_CLIENT})
        writeInfoMsg(zSock, scriptName, sourceCode)
        readMsgOrFatal(fatalFunc, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_META})
        writeMetaMsg(zSock, inIterMulti, scriptInRowPrototype, outIterMulti, scriptOutRowPrototype)
        readMsgOrFatal(fatalFunc, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_RUN})
        writeSimpleMsg(zSock, zProto.MessageType_MT_RUN)
}

func writeCommunicationFinalization(fatalFunc func(args ...interface{}), zSock *zmq.Socket) error {
        var msg *zProto.ExascriptRequest
        for a := 0; a < 10; a++ {
                msg = readMsgOrFatal(fatalFunc, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_DONE, zProto.MessageType_MT_RUN, zProto.MessageType_MT_FINISHED})
                if *msg.Type == zProto.MessageType_MT_DONE || *msg.Type == zProto.MessageType_MT_RUN {
                        writeSimpleMsg(zSock, zProto.MessageType_MT_CLEANUP)
                        continue;
                }
                if *msg.Type == zProto.MessageType_MT_FINISHED {
                        writeSimpleMsg(zSock, zProto.MessageType_MT_FINISHED)
                        zSock.Close()
                        return nil;
                }
        }
        log.Panic("Incorrect flow closing")
        return nil;
}

const CONCAT_STR_FUNC = `

        package main

        import "exago"

        func Run(iter *exago.ExaIter) *string {
                var resultS string;
                for true {
                        resultS += *iter.ReadString(0)
                        if !iter.Next() {
                                break;
                        }
                }
                return &resultS;
        }

`;


const SUM_INT_FUNC = `

        package main

        import "exago"
        import "log"

        func Run(iter *exago.ExaIter) *int64 {
                var resultInt int64;
                for true {
                        switch iter.Row[0].(type) {
                                case int64:
                                        resultInt += iter.Row[0].(int64)
                                case nil:
                                        break;
                                default:
                                      log.Panic("Unknown type for input value, expecting int64")
                        }
                        if !iter.Next() {
                                break;
                        }
                }
                return &resultInt;
        }
`;


const SUM_INT_RETURNINT_FUNC = `

        package main

        import "exago"

        func Run(iter *exago.ExaIter) *int64 {
                var resultInt *int64;
                for true {
                        intval := iter.ReadInt64(0)
                        if intval != nil {
                                if resultInt == nil {
                                        resultInt = new(int64)
                                }
                                *resultInt += *intval

                        }
                        if !iter.Next() {
                                break;
                        }
                }
                return resultInt;
        }

`;


const GENERATE_SERIES_EMITROW_FUNC = `
        package main

        import "exago"

        func Run(iter *exago.ExaIter) {
                offsetFrom := *iter.ReadInt64(0)
                offsetTo := *iter.ReadInt64(1)
                for i := offsetFrom; i < offsetTo; i++ {
                        iter.Emit(i)
                }
        }
`;

const GENERATE_SERIES_EMITVAL_FUNC = `
        package main

        import "exago"

        func Run(iter *exago.ExaIter) {
                offsetFrom := *iter.ReadInt64(0)
                offsetTo := *iter.ReadInt64(1)
                for i := offsetFrom; i < offsetTo; i++ {
                        iter.EmitInt64(i)
                }
        }
`;



func Test1(t *testing.T) {
        zSock := initZSocket();
        go func() {
                writeCommunicationInitialization(t.Fatal, zSock, "test_script", CONCAT_STR_FUNC, true, []interface{}{"string1"}, false, []interface{}{"string1"});

                readMsgOrFatal(t.Fatal, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                var rows [][]interface{}
                rows = append(rows, []interface{}{"string1"})
                rows = append(rows, []interface{}{"string2"})
                writeDataMessage(zSock, &rows)
                readMsgOrFatal(t.Fatal, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                readMsgOrFatal(t.Fatal, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_EMIT})
                writeSimpleMsg(zSock, zProto.MessageType_MT_EMIT)

                writeCommunicationFinalization(t.Fatal, zSock)
        }()
        runProcess(ZSOCKADDR)
        log.Println("Finished test")
}


func TestScriptReturningInt64_1(t *testing.T) {
        zSock := initZSocket();
        go func() {
                writeCommunicationInitialization(t.Fatal, zSock, "test_script", SUM_INT_RETURNINT_FUNC, true, []interface{}{int64(0)}, false, []interface{}{int64(0)});

                readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                var rows [][]interface{}
                rows = append(rows, []interface{}{int64(1)})
                rows = append(rows, []interface{}{int64(3)})
                writeDataMessage(zSock, &rows)
                readMsgOrFatal(t.Fatal, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                msg := readMsgOrFatal(t.Fatal, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_EMIT})
                if *msg.Emit.Table.Rows != 1 || msg.Emit.Table.DataInt64[0] != 4 {
                        log.Fatal("Incorrect return for sum function: ", msg)
                }
                writeSimpleMsg(zSock, zProto.MessageType_MT_EMIT)
                writeCommunicationFinalization(t.Fatal, zSock)
        }()
        runProcess(ZSOCKADDR)
        log.Println("Finished test")
}


func TestDecimal(t *testing.T) {
        var d apd.Decimal
        d.SetString("101.101")
        c := apd.BaseContext.WithPrecision(36)
        for {
                res, err := c.Quo(&d, &d, apd.New(2, 0))
                fmt.Printf("d: %s, inexact: %5v, err: %v\n", d.Text('f'), res.Inexact(), err)
                if err != nil {
                        return
                }
                if res.Inexact() {
                        return
                }
        }
}

func TestScriptReturningInt64_2(t *testing.T) {
        zSock := initZSocket();
        go func() {
                writeCommunicationInitialization(t.Fatal, zSock, "test_script", SUM_INT_RETURNINT_FUNC, false, []interface{}{int64(0)}, false, []interface{}{int64(0)});

                readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                var rows [][]interface{}
                rows = append(rows, []interface{}{int64(1)})
                rows = append(rows, []interface{}{int64(3)})
                writeDataMessage(zSock, &rows)
                readMsgOrFatal(t.Fatal, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                msg := readMsgOrFatal(t.Fatal, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_EMIT})
                if *msg.Emit.Table.Rows != 1 || msg.Emit.Table.DataInt64[0] != 4 {
                        log.Fatal("Incorrect return for sum function: ", msg)
                }
                writeSimpleMsg(zSock, zProto.MessageType_MT_EMIT)

                writeCommunicationFinalization(t.Fatal, zSock)
        }()
        runProcess(ZSOCKADDR)
        log.Println("Finished test")
}

func TestScriptReturningInt64_3(t *testing.T) {
        zSock := initZSocket();
        go func() {
                writeCommunicationInitialization(t.Fatal, zSock, "test_script", SUM_INT_RETURNINT_FUNC, true, []interface{}{int64(0)}, false, []interface{}{int64(0)});

                readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                var rows [][]interface{}
                rows = append(rows, []interface{}{nil})
                writeDataMessage(zSock, &rows)
                readMsgOrFatal(t.Fatal, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                msg := readMsgOrFatal(t.Fatal, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_EMIT})
                if *msg.Emit.Table.Rows != 1 || len(msg.Emit.Table.DataInt64) != 0 || msg.Emit.Table.DataNulls[0] != true {
                        log.Fatal("Incorrect return for sum function: ", msg)
                }
                writeSimpleMsg(zSock, zProto.MessageType_MT_EMIT)

                writeCommunicationFinalization(t.Fatal, zSock)
        }()
        runProcess(ZSOCKADDR)
        log.Println("Finished test")
}


func BenchmarkDummy1(b *testing.B) {
        var inFields []zProto.ExascriptMetadataColumnDefinition;
        inFields = append(inFields, zProto.ExascriptMetadataColumnDefinition{})

        for a := 0; a < b.N; a++ {
                for colI, colInfo := range inFields {
                        _ = colInfo.Type
                        _ = colI
                }
        }
}

func BenchmarkDummy2(b *testing.B) {
        var s []int;
        var s_proto []int;
        b.StopTimer();
        for a := 0; a < 1000 * 1000; a++ {
                s_proto = append(s_proto, a)
        }
        b.StartTimer();
        for i := 0; i < b.N; i++ {
                s = make([]int, 1000 * 1000)
                _ = s
        }
}

func Benchmark1(b *testing.B) {
        if !testing.Verbose() {
                log.SetOutput(ioutil.Discard)
        }
        done := make(chan bool)
        zSock := initZSocket();
        go func() {
                writeCommunicationInitialization(b.Fatal, zSock, "test_script", SUM_INT_FUNC, true, []interface{}{int64(0)}, false, []interface{}{int64(0)});

                for i := b.N; i > 0; i-- {
                        readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                        var rows [][]interface{}
                        for x := 0; x < 1000 * 1000 ; x++ {
                                rows = append(rows, []interface{}{int64(x)})
                        }
                        writeDataMessage(zSock, &rows)
                        readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                        writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                        readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_EMIT})
                        writeSimpleMsg(zSock, zProto.MessageType_MT_EMIT)


                        if (i > 1) {
                                readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_DONE})
                                writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                                readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_RUN})
                                writeSimpleMsg(zSock, zProto.MessageType_MT_RUN)
                        }
                }

                writeCommunicationFinalization(b.Fatal, zSock)
                done <- true
        }()
        runProcess(ZSOCKADDR)
        <-done
        log.Println("Finished benchmark")
}

func continueDatasetIteration(fatalFunc func(args ...interface{}), zSock *zmq.Socket) error {
        msg := readMsgOrFatal(fatalFunc, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_DONE, zProto.MessageType_MT_RUN})
        if *msg.Type == zProto.MessageType_MT_DONE {
                writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                readMsgOrFatal(fatalFunc, zSock, 0, []zProto.MessageType{zProto.MessageType_MT_RUN})
        }
        writeSimpleMsg(zSock, zProto.MessageType_MT_RUN)
        return nil;
}

func benchmarkGenerateSeriesBase(b *testing.B, generateSeriesFunc string) {
        if !testing.Verbose() {
                log.SetOutput(ioutil.Discard)
        }
        done := make(chan bool)
        zSock := initZSocket();
        go func() {
                writeCommunicationInitialization(b.Fatal, zSock, "test_script", generateSeriesFunc, true, []interface{}{int64(0),int64(0)}, true, []interface{}{int64(0)});

                b.ResetTimer();
                b.StopTimer();
                for i := b.N; i > 0; i-- {
                        readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                        var rows [][]interface{}
                        rows = append(rows, []interface{}{int64(1), int64(10 * 1000 * 1000)})
                        writeDataMessage(zSock, &rows)
                        b.StartTimer();

                        for true {
                                dMsg, err := readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_EMIT, zProto.MessageType_MT_NEXT, zProto.MessageType_MT_DONE})
                                if err != nil {
                                        b.Error(err)
                                }
                                if *dMsg.Type == zProto.MessageType_MT_NEXT {
                                        writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                                        break;
                                } else if *dMsg.Type == zProto.MessageType_MT_DONE {
                                        b.StopTimer()
                                        writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                                        break;
                                } else {
                                        writeSimpleMsg(zSock, zProto.MessageType_MT_EMIT)
                                        continue;
                                }
                        }

                        if (i > 1) {
                                continueDatasetIteration(b.Fatal, zSock);
                        }
                }
                b.StopTimer();
                writeCommunicationFinalization(b.Fatal, zSock)
                done <- true
        }()
        runProcess(ZSOCKADDR)
        <-done
        log.Println("Finished benchmark")
}

func BenchmarkGenerateSeriesWithEmitRow(b *testing.B) {
        benchmarkGenerateSeriesBase(b, GENERATE_SERIES_EMITROW_FUNC);
}

func BenchmarkGenerateSeriesWithEmit(b *testing.B) {
        benchmarkGenerateSeriesBase(b, GENERATE_SERIES_EMITVAL_FUNC);
}

func BenchmarkLoadDataset(b *testing.B) {
        if !testing.Verbose() {
                log.SetOutput(ioutil.Discard)
        }
        done := make(chan bool)
        zSock := initZSocket();
        go func() {
                writeCommunicationInitialization(b.Fatal, zSock, "test_script", SUM_INT_FUNC, true, []interface{}{int64(0)}, false, []interface{}{int64(0)});

                for i := b.N; i > 0; i-- {
                        readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                        var rows [][]interface{}
                        for x := 0; x < 1000 * 1000 ; x++ {
                                rows = append(rows, []interface{}{int64(x)})
                        }
                        writeDataMessage(zSock, &rows)
                        readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_NEXT})
                        writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                        readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_EMIT})
                        writeSimpleMsg(zSock, zProto.MessageType_MT_EMIT)


                        if (i > 1) {
                                readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_DONE})
                                writeSimpleMsg(zSock, zProto.MessageType_MT_DONE)
                                readMsg(zSock, 0, []zProto.MessageType{zProto.MessageType_MT_RUN})
                                writeSimpleMsg(zSock, zProto.MessageType_MT_RUN)
                        }
                }

                writeCommunicationFinalization(b.Fatal, zSock)
                done <- true
        }()
        runProcess(ZSOCKADDR)
        <-done
        log.Println("Finished benchmark")
}


func TestFuncIncorrectReturnType(b *testing.T) {
        done := make(chan bool);
        zSock := initZSocket();
        go func() {
                defer func() {
                        done<-true
                }()
                var errorGot string;
                writeCommunicationInitialization(func(err ...interface{}){
                        errorGot = fmt.Sprint(err...)
                }, zSock, "test_script", CONCAT_STR_FUNC, true, []interface{}{"string1"}, false, []interface{}{int64(0)});
                if strings.HasPrefix(errorGot, exago.ERROR_INCOMPATIBLE_FUNCTION_FORMAT) {
                        zSock.Close()
                        return;
                } else {
                        b.Fatal("Did not get incompatible return type error, got: ", errorGot);
                }
        }()
        runProcess(ZSOCKADDR)
        <-done
        log.Println("Finished test")
}
