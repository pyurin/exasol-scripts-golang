package exago

import (
	zProto "zmqcontainer" 
	"log"
    "time"
    "reflect"
    "math"
)

const MAX_DATASIZE = 6000000;

type ExaIterInputOffsets struct {
    ExternalRowNumber int
    Nulls int
    Strings int
    Int32s int
    Int64s int
    Bools int
    Doubles int
}

type ExaIter struct {
	IsFinished bool
	Rownum uint64
	ZMessage *zProto.ExascriptResponse
	ExternalRowNumber uint64
	Row []interface{}
    RowColumn map[string]*interface{}
	OutRow []interface{}
    exaContext ExaContext
    WriteBufferBytes uint64
    ResultZMsg *zProto.ExascriptRequest
    ResultRowsInGroup uint64
    ResultRows uint64
    InputOffsets ExaIterInputOffsets
}

/**
  * Usage: 

        func Run(iter *exago.ExaIter) interface{} {
                fieldByName := iter.RowColumn["fieldKey"])
                fieldIndex := iter.Row[0]
        }
  * 

    @todo optimize array usage in terms of append for input and output
    @todo inputOffsets - don't use map

  **/

func NewExaIter(exaContext ExaContext) *ExaIter {
	iter := &ExaIter{
        exaContext: exaContext,
        ResultZMsg: new(zProto.ExascriptRequest),
    }
    iter.ClearResultData();
    iter.Row = make([]interface{}, len(iter.exaContext.ZMetaMsg.Meta.InputColumns), len(iter.exaContext.ZMetaMsg.Meta.InputColumns))
    iter.RowColumn = make(map[string]*interface{})
    for colI, colInfo := range iter.exaContext.ZMetaMsg.Meta.InputColumns {
        iter.RowColumn[*colInfo.Name] = &iter.Row[colI];
    }
    return iter;
}

func (iter *ExaIter) ClearResultData() {
    iter.ResultZMsg = new(zProto.ExascriptRequest);
    iter.ResultZMsg.Emit = new (zProto.ExascriptEmitDataReq);
    iter.ResultZMsg.Emit.Table = new (zProto.ExascriptTableData);
    iter.ResultRowsInGroup = 0;
    iter.ResultRows = 0;
    iter.ResultZMsg.Emit.Table.RowsInGroup = &iter.ResultRowsInGroup;
    iter.ResultZMsg.Emit.Table.Rows = &iter.ResultRows;
    iter.WriteBufferBytes = 0;
    // preallocate slices for optimization
    iter.ResultZMsg.Emit.Table.RowNumber = make([]uint64, 0, 10 * 1000)
    iter.ResultZMsg.Emit.Table.DataNulls = make([]bool, 0, 10 * 1000)
    for _, colI := range iter.exaContext.ZMetaMsg.Meta.OutputColumns {
        switch *colI.Type {
            case zProto.ColumnType_PB_DOUBLE:
                if len(iter.ResultZMsg.Emit.Table.DataDouble) == 0 {
                    iter.ResultZMsg.Emit.Table.DataDouble = make([]float64, 0, 10 * 1000)
                }
            case zProto.ColumnType_PB_BOOLEAN:
                if len(iter.ResultZMsg.Emit.Table.DataBool) == 0 {
                    iter.ResultZMsg.Emit.Table.DataBool = make([]bool, 0, 10 * 1000)
                }
            case zProto.ColumnType_PB_INT32:
                if len(iter.ResultZMsg.Emit.Table.DataInt32) == 0 {
                    iter.ResultZMsg.Emit.Table.DataInt32 = make([]int32, 0, 10 * 1000)
                }
            case zProto.ColumnType_PB_INT64:
                if len(iter.ResultZMsg.Emit.Table.DataInt64) == 0 {
                    iter.ResultZMsg.Emit.Table.DataInt64 = make([]int64, 0, 10 * 1000)
                }
            case zProto.ColumnType_PB_NUMERIC:
                fallthrough
            case zProto.ColumnType_PB_DATE:
                fallthrough
            case zProto.ColumnType_PB_TIMESTAMP:
                fallthrough
            case zProto.ColumnType_PB_STRING:
                if len(iter.ResultZMsg.Emit.Table.DataString) == 0 {
                    iter.ResultZMsg.Emit.Table.DataString = make([]string, 0, 10 * 1000)
                }
        }
    }
}

func (iter *ExaIter) CleanupInput() {
    iter.IsFinished = false;
    iter.ZMessage = nil;
    iter.Rownum = 0;
}

func (iter *ExaIter) Next() bool {
    if (iter.IsFinished) {
    	return false;
    }
    if iter.ZMessage == nil || iter.Rownum >= *iter.ZMessage.Next.Table.Rows {
    	iter.ZMessage = Comm(iter.exaContext, zProto.MessageType_MT_NEXT, []zProto.MessageType{zProto.MessageType_MT_NEXT, zProto.MessageType_MT_DONE}, nil)
    	iter.Rownum = 0

        p := reflect.ValueOf(&iter.InputOffsets).Elem() 
        p.Set(reflect.Zero(p.Type()))

        if *iter.ZMessage.Type == zProto.MessageType_MT_DONE {
            //log.Println("ITER.", "iterNext", " - finished")
            iter.IsFinished = true;
            return false
        } else {
            //log.Println("ITER.", "iterNext", " - finished", iter.ZMessage.Next.Table)
        }

    }
    iter.readRow();
    iter.Rownum++;
    return true;
}

/**
  * Writes row into ResultZMsg message buf
  */
func (iter *ExaIter) writeRow(out []interface{}) {
    iter.ResultZMsg.Emit.Table.RowNumber = append(iter.ResultZMsg.Emit.Table.RowNumber, iter.ExternalRowNumber)
    iter.WriteBufferBytes += 8;

    var fieldI int;
    var field interface{};

    defer func() {
        if r := recover(); r != nil {
            if  *iter.exaContext.ZMetaMsg.Meta.OutputIterType == zProto.IterType_PB_EXACTLY_ONCE {
                //return
                log.Panic(
                    "Failed writing result, trying to return incompatible type `", reflect.TypeOf(out[fieldI]), "`",
                    "\n", r);
            } else {
                //emits
                log.Panic(
                    "Failed writing output column ", (fieldI), " (name=", *iter.exaContext.ZMetaMsg.Meta.OutputColumns[fieldI].Name, "), ",
                    "trying to emit incompatible type `", reflect.TypeOf(out[fieldI]), "`",
                    "\n", r);
            }
        }
    }()
    for fieldI, field = range(out) {
        if field == nil {
            iter.WriteBufferBytes++
            iter.ResultZMsg.Emit.Table.DataNulls = append(iter.ResultZMsg.Emit.Table.DataNulls, true)
        } else {
            iter.ResultZMsg.Emit.Table.DataNulls = append(iter.ResultZMsg.Emit.Table.DataNulls, false)
            switch *iter.exaContext.ZMetaMsg.Meta.OutputColumns[fieldI].Type {
                case zProto.ColumnType_PB_NUMERIC:
                    fallthrough
                case zProto.ColumnType_PB_STRING:
                    iter.ResultZMsg.Emit.Table.DataString = append(iter.ResultZMsg.Emit.Table.DataString, out[fieldI].(string))
                    iter.WriteBufferBytes += uint64(len(field.(string)));
                case zProto.ColumnType_PB_DOUBLE:
                    iter.ResultZMsg.Emit.Table.DataDouble = append(iter.ResultZMsg.Emit.Table.DataDouble, out[fieldI].(float64))
                    iter.WriteBufferBytes += 12 //why 12 but not 8?....
                case zProto.ColumnType_PB_BOOLEAN:
                    iter.ResultZMsg.Emit.Table.DataBool = append(iter.ResultZMsg.Emit.Table.DataBool, out[fieldI].(bool))
                    iter.WriteBufferBytes += 1
                case zProto.ColumnType_PB_INT32:
                    iter.ResultZMsg.Emit.Table.DataInt32 = append(iter.ResultZMsg.Emit.Table.DataInt32, out[fieldI].(int32))
                    iter.WriteBufferBytes += 4
                case zProto.ColumnType_PB_INT64:
                    iter.ResultZMsg.Emit.Table.DataInt64 = append(iter.ResultZMsg.Emit.Table.DataInt64, out[fieldI].(int64))
                    iter.WriteBufferBytes += 8
                case zProto.ColumnType_PB_TIMESTAMP:
                    switch out[fieldI].(type) {
                        case float64:
                            sec, dec := math.Modf(out[fieldI].(float64));
                            timeFormatted := (time.Unix(int64(sec), int64(dec*(1e9)))).Format("2006-01-02 15:04:05.999")
                            iter.ResultZMsg.Emit.Table.DataString = append(iter.ResultZMsg.Emit.Table.DataString, timeFormatted)
                            iter.WriteBufferBytes += uint64(len(timeFormatted));
                        case int64:
                            timeFormatted := (time.Unix(out[fieldI].(int64), 0)).Format("2006-01-02 15:04:05.999")
                            iter.ResultZMsg.Emit.Table.DataString = append(iter.ResultZMsg.Emit.Table.DataString, timeFormatted)
                            iter.WriteBufferBytes += uint64(len(timeFormatted));
                        case time.Time:
                            timeFormatted := (out[fieldI].(time.Time)).Format("2006-01-02 15:04:05.999")
                            iter.ResultZMsg.Emit.Table.DataString = append(iter.ResultZMsg.Emit.Table.DataString, timeFormatted)
                            iter.WriteBufferBytes += uint64(len(timeFormatted));
                        case string:
                            iter.ResultZMsg.Emit.Table.DataString = append(iter.ResultZMsg.Emit.Table.DataString, out[fieldI].(string))
                            iter.WriteBufferBytes += uint64(len(field.(string)));
                        default:
                            log.Panic("Can't convert out column PB_TIMESTAMP from ", reflect.TypeOf(out[fieldI]))
                    }
                case zProto.ColumnType_PB_DATE:
                    switch out[fieldI].(type) {
                        case time.Time:
                            timeFormatted := (out[fieldI].(time.Time)).Format("2006-01-02")
                            iter.ResultZMsg.Emit.Table.DataString = append(iter.ResultZMsg.Emit.Table.DataString, timeFormatted)
                            iter.WriteBufferBytes += uint64(len(timeFormatted));
                        case string:
                            iter.ResultZMsg.Emit.Table.DataString = append(iter.ResultZMsg.Emit.Table.DataString, out[fieldI].(string))
                            iter.WriteBufferBytes += uint64(len(field.(string)));
                        default:
                            log.Panic("Can't convert out column PB_DATE from ", reflect.TypeOf(out[fieldI]))
                    }
                default:
                    log.Panic("Undefined outcolumn type: ", iter.exaContext.ZMetaMsg.Meta.OutputColumns[fieldI])
            }
        }
    }
    iter.ResultRows++;
}

func (iter *ExaIter) EmitFlush() {
    Comm(iter.exaContext, zProto.MessageType_MT_EMIT, []zProto.MessageType{zProto.MessageType_MT_EMIT}, iter.ResultZMsg)
    iter.ClearResultData();
}

func (iter *ExaIter) Emit(out ...interface{}) {
    // emits one row
    // if size of all emmited rows in the buffer is larger then
    // MAX_DATASIZE, then flush the buffer.
    //log.Println("Row to emit: ", out)
    if len(out) == len(iter.exaContext.ZMetaMsg.Meta.OutputColumns) {
        iter.writeRow(out)
        //log.Println("Write finished, buf size: ", iter.WriteBufferBytes)
    } else {
        log.Panic("Incorrect number of rows - expecting ", len(iter.exaContext.ZMetaMsg.Meta.OutputColumns), ", got ", len(out))
    } 
    if (iter.WriteBufferBytes > MAX_DATASIZE) {
        iter.EmitFlush();
    }
}


//read next row from ZMessage
func (iter *ExaIter) readRow() {

	iter.ExternalRowNumber = iter.ZMessage.Next.Table.RowNumber[iter.InputOffsets.ExternalRowNumber];
    iter.InputOffsets.ExternalRowNumber++;

	for i, colInfo := range iter.exaContext.ZMetaMsg.Meta.InputColumns {
        isNullValue := iter.ZMessage.Next.Table.DataNulls[ iter.InputOffsets.Nulls ]
        iter.InputOffsets.Nulls++;
        if isNullValue {
            iter.Row[i] = nil;
        } else {
            switch *colInfo.Type {
                case zProto.ColumnType_PB_DOUBLE:
                    iter.Row[i] =  iter.ZMessage.Next.Table.DataDouble[ iter.InputOffsets.Doubles ];
                    iter.InputOffsets.Doubles++;
                case zProto.ColumnType_PB_INT32:
                    iter.Row[i] =  iter.ZMessage.Next.Table.DataInt32[ iter.InputOffsets.Int32s ];
                    iter.InputOffsets.Int32s++;
                case zProto.ColumnType_PB_INT64:
                    iter.Row[i] =  iter.ZMessage.Next.Table.DataInt64[ iter.InputOffsets.Int64s ];
                    iter.InputOffsets.Int64s++;
                case zProto.ColumnType_PB_BOOLEAN:
                    iter.Row[i] =  iter.ZMessage.Next.Table.DataBool[ iter.InputOffsets.Bools ];
                    iter.InputOffsets.Bools++;
                case zProto.ColumnType_PB_NUMERIC:
                    // @todo I dont know about numeric / decimal types in golang
                    iter.Row[i] =  iter.ZMessage.Next.Table.DataString[ iter.InputOffsets.Strings ];
                    iter.InputOffsets.Strings++;
                case zProto.ColumnType_PB_TIMESTAMP:
                    var err error;
                    iter.Row[i], err = time.Parse("2006-01-02 15:04:05.999999", iter.ZMessage.Next.Table.DataString[ iter.InputOffsets.Strings ])
                    if err != nil {
                        log.Panic("Could not parse time ", iter.ZMessage.Next.Table.DataString[ iter.InputOffsets.Strings ], "; ", err)
                    }
                    iter.InputOffsets.Strings++;
                case zProto.ColumnType_PB_DATE:
                    var err error;
                    iter.Row[i], err = time.Parse("2006-01-02", iter.ZMessage.Next.Table.DataString[ iter.InputOffsets.Strings ])
                    if err != nil {
                        log.Panic("Could not parse date ", iter.ZMessage.Next.Table.DataString[ iter.InputOffsets.Strings ], "; ", err)
                    }
                    iter.InputOffsets.Strings++;
                case zProto.ColumnType_PB_STRING:
                    iter.Row[i] =  iter.ZMessage.Next.Table.DataString[ iter.InputOffsets.Strings ];
                    iter.InputOffsets.Strings++;
                default:
                    log.Panic("Unknown column type: ", colInfo.Type);
            }
        }
	}
	//log.Println("Read row: ", iter.Row);
}