package exago

import (
	zProto "zmqcontainer"
	"log"
	"time"
	"math"
	"math/big"
)


func (iter *ExaIter) ClearResultData() {
	if iter.ResultZMsg.Emit == nil {
		iter.ResultZMsg.Emit = new (zProto.ExascriptEmitDataReq);
		iter.ResultZMsg.Emit.Table = new (zProto.ExascriptTableData);
	}
	iter.ResultRowsInGroup = 0;
	iter.ResultRows = 0;
	iter.WriteBufferBytes = 0;
	// preallocate slices for optimization
	if (cap(iter.ResultZMsg.Emit.Table.RowNumber) == 0) {
		//initial clean up
		iter.ResultZMsg.Emit.Table.RowNumber = make([]uint64, 0, 100 * 1000)
		iter.ResultZMsg.Emit.Table.DataNulls = make([]bool, 0, 100 * 1000)
		iter.ResultZMsg.Emit.Table.DataBool = make([]bool, 0, 100 * 1000)
		iter.ResultZMsg.Emit.Table.DataInt32 = make([]int32, 0, 100 * 1000)
		iter.ResultZMsg.Emit.Table.DataInt64 = make([]int64, 0, 100 * 1000)
		iter.ResultZMsg.Emit.Table.DataString = make([]string, 0, 100 * 1000)
		iter.ResultZMsg.Emit.Table.DataDouble = make([]float64, 0, 100 * 1000)
	}
	iter.ResultZMsg.Emit.Table.RowNumber = iter.ResultZMsg.Emit.Table.RowNumber[:0]
	iter.ResultZMsg.Emit.Table.DataNulls = iter.ResultZMsg.Emit.Table.DataNulls[:0]
	iter.ResultZMsg.Emit.Table.DataDouble = iter.ResultZMsg.Emit.Table.DataDouble[:0]
	iter.ResultZMsg.Emit.Table.DataBool = iter.ResultZMsg.Emit.Table.DataBool[:0]
	iter.ResultZMsg.Emit.Table.DataInt32 = iter.ResultZMsg.Emit.Table.DataInt32[:0]
	iter.ResultZMsg.Emit.Table.DataInt64 = iter.ResultZMsg.Emit.Table.DataInt64[:0]
	iter.ResultZMsg.Emit.Table.DataDouble = iter.ResultZMsg.Emit.Table.DataDouble[:0]
	iter.ResultZMsg.Emit.Table.DataString = iter.ResultZMsg.Emit.Table.DataString[:0]
	iter.ResultZMsg.Emit.Table.RowsInGroup = &iter.ResultRowsInGroup;
	iter.ResultZMsg.Emit.Table.Rows = &iter.ResultRows;
	iter.ResultTable = iter.ResultZMsg.Emit.Table;
}

func (iter *ExaIter) beforeWriteValue() {
	if iter.OutRowColumnIndex == 0 {
		// first column in row
		iter.ResultTable.RowNumber = append(iter.ResultTable.RowNumber, iter.ExternalRowNumber)
		iter.WriteBufferBytes += 8;
	}
}

func (iter *ExaIter) afterWriteValue() {
	iter.OutRowColumnIndex++
	if iter.OutRowColumnIndex == iter.MetaOutRowSize {
		// last field in row
		iter.ResultRows++;
		iter.OutRowColumnIndex = 0
	}
}


/**
  * Writes row into ResultZMsg message buf
  */
func (iter *ExaIter) writeRow(out *[]interface{}) {
	iter.ResultTable.RowNumber = append(iter.ResultTable.RowNumber, iter.ExternalRowNumber)
	iter.WriteBufferBytes += 8;

	for fieldI, field := range(*out) {
		if field == nil {
			iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, true)
			iter.WriteBufferBytes++
		} else {
			iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, false)
			switch iter.MetaOutColumnTypes[fieldI] {
			case zProto.ColumnType_PB_NUMERIC:
				fallthrough
			case zProto.ColumnType_PB_STRING:
				switch field.(type){
				case string:
					iter.ResultTable.DataString = append(iter.ResultTable.DataString, field.(string))
					iter.WriteBufferBytes += uint64(len(field.(string)));
				default:
					iter.PanicTypeAssert(fieldI, iter.ExternalRowNumber, field)
				}
			case zProto.ColumnType_PB_DOUBLE:
				switch field.(type){
				case float64:
					iter.ResultTable.DataDouble = append(iter.ResultTable.DataDouble, field.(float64))
					iter.WriteBufferBytes += 8
				default:
					iter.PanicTypeAssert(fieldI, iter.ExternalRowNumber, field)
				}
			case zProto.ColumnType_PB_BOOLEAN:
				switch field.(type){
				case bool:
					iter.ResultTable.DataBool = append(iter.ResultTable.DataBool, field.(bool))
					iter.WriteBufferBytes += 1
				default:
					iter.PanicTypeAssert(fieldI, iter.ExternalRowNumber, field)
				}
			case zProto.ColumnType_PB_INT32:
				switch field.(type){
				case int32:
					iter.ResultTable.DataInt32 = append(iter.ResultTable.DataInt32, field.(int32))
					iter.WriteBufferBytes += 4
				default:
					iter.PanicTypeAssert(fieldI, iter.ExternalRowNumber, field)
				}
			case zProto.ColumnType_PB_INT64:
				switch field.(type){
				case int64:
					iter.ResultTable.DataInt64 = append(iter.ResultTable.DataInt64, field.(int64))
					iter.WriteBufferBytes += 8
				default:
					iter.PanicTypeAssert(fieldI, iter.ExternalRowNumber, field)
				}
			case zProto.ColumnType_PB_TIMESTAMP:
				switch field.(type) {
				case float64:
					sec, dec := math.Modf(field.(float64));
					timeFormatted := (time.Unix(int64(sec), int64(dec*(1e9)))).Format("2006-01-02 15:04:05.999")
					iter.ResultTable.DataString = append(iter.ResultTable.DataString, timeFormatted)
					iter.WriteBufferBytes += uint64(len(timeFormatted));
				case int64:
					timeFormatted := (time.Unix(field.(int64), 0)).Format("2006-01-02 15:04:05.999")
					iter.ResultTable.DataString = append(iter.ResultTable.DataString, timeFormatted)
					iter.WriteBufferBytes += uint64(len(timeFormatted));
				case time.Time:
					timeFormatted := (field.(time.Time)).Format("2006-01-02 15:04:05.999")
					iter.ResultTable.DataString = append(iter.ResultTable.DataString, timeFormatted)
					iter.WriteBufferBytes += uint64(len(timeFormatted));
				case string:
					iter.ResultTable.DataString = append(iter.ResultZMsg.Emit.Table.DataString, field.(string))
					iter.WriteBufferBytes += uint64(len(field.(string)));
				default:
					iter.PanicTypeAssert(fieldI, iter.ExternalRowNumber, field)
				}
			case zProto.ColumnType_PB_DATE:
				switch field.(type) {
				case time.Time:
					timeFormatted := (field.(time.Time)).Format("2006-01-02")
					iter.ResultTable.DataString = append(iter.ResultTable.DataString, timeFormatted)
					iter.WriteBufferBytes += uint64(len(timeFormatted));
				case string:
					iter.ResultTable.DataString = append(iter.ResultTable.DataString, field.(string))
					iter.WriteBufferBytes += uint64(len(field.(string)));
				default:
					iter.PanicTypeAssert(fieldI, iter.ExternalRowNumber, field)
				}
			default:
				log.Panic("Undefined outcolumn type: ", iter.MetaOutColumnTypes[fieldI])
			}
		}
	}
	iter.ResultRows++;
}

func (iter *ExaIter) EmitFlush() {
	Comm(iter.exaContext, zProto.MessageType_MT_EMIT, []zProto.MessageType{zProto.MessageType_MT_EMIT}, iter.ResultZMsg)
	iter.ClearResultData();
}

func (iter *ExaIter) EmitValueInt64(i int64) {
	iter.beforeWriteValue()
	if iter.MetaOutColumnTypes[iter.OutRowColumnIndex] != zProto.ColumnType_PB_INT64 {
		iter.PanicTypeAssert(iter.OutRowColumnIndex, iter.ExternalRowNumber, i)
	}
	iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, false)
	iter.ResultTable.DataInt64 = append(iter.ResultTable.DataInt64, i)
	iter.WriteBufferBytes += 8 + 1
	iter.afterWriteValue()
	if (iter.WriteBufferBytes > MAX_DATASIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueInt32(i int32) {
	iter.beforeWriteValue()
	if iter.MetaOutColumnTypes[iter.OutRowColumnIndex] != zProto.ColumnType_PB_INT32 {
		iter.PanicTypeAssert(iter.OutRowColumnIndex, iter.ExternalRowNumber, i)
	}
	iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, false)
	iter.ResultTable.DataInt32 = append(iter.ResultTable.DataInt32, i)
	iter.WriteBufferBytes += 4 + 1
	iter.afterWriteValue()
	if (iter.WriteBufferBytes > MAX_DATASIZE) {
		iter.EmitFlush();
	}
}


func (iter *ExaIter) EmitValueBool(b bool) {
	iter.beforeWriteValue()
	if iter.MetaOutColumnTypes[iter.OutRowColumnIndex] != zProto.ColumnType_PB_BOOLEAN {
		iter.PanicTypeAssert(iter.OutRowColumnIndex, iter.ExternalRowNumber, b)
	}
	iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, false)
	iter.ResultTable.DataBool = append(iter.ResultTable.DataBool, b)
	iter.WriteBufferBytes += 1 + 1
	iter.afterWriteValue()
	if (iter.WriteBufferBytes > MAX_DATASIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueTime(t time.Time) {
	iter.beforeWriteValue()
	if iter.MetaOutColumnTypes[iter.OutRowColumnIndex] == zProto.ColumnType_PB_TIMESTAMP {
		iter.ResultTable.DataString = append(iter.ResultTable.DataString, t.Format("2006-01-02 15:04:05.999"))
		iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, false)
		iter.WriteBufferBytes += 24;
	} else if iter.MetaOutColumnTypes[iter.OutRowColumnIndex] == zProto.ColumnType_PB_DATE {
		iter.ResultTable.DataString = append(iter.ResultTable.DataString, t.Format("2006-01-02"))
		iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, false)
		iter.WriteBufferBytes += 11;
	} else {
		iter.PanicTypeAssert(iter.OutRowColumnIndex,  iter.ExternalRowNumber, t)
	}
	iter.afterWriteValue()
	if (iter.WriteBufferBytes > MAX_DATASIZE) {
		iter.EmitFlush();
	}
}


func (iter *ExaIter) EmitValueFloat64(f float64) {
	iter.beforeWriteValue()
	if iter.MetaOutColumnTypes[iter.OutRowColumnIndex] != zProto.ColumnType_PB_DOUBLE {
		iter.PanicTypeAssert(iter.OutRowColumnIndex, iter.ExternalRowNumber, f)
	}
	iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, false)
	iter.ResultTable.DataDouble = append(iter.ResultTable.DataDouble, f)
	iter.WriteBufferBytes += 8
	iter.afterWriteValue()
	if (iter.WriteBufferBytes > MAX_DATASIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueNull() {
	iter.beforeWriteValue()
	iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, true)
	iter.WriteBufferBytes += 1;
	iter.afterWriteValue()
	if (iter.WriteBufferBytes > MAX_DATASIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueString(s string) {
	iter.beforeWriteValue()
	if (iter.MetaOutColumnTypes[iter.OutRowColumnIndex] != zProto.ColumnType_PB_STRING &&
		iter.MetaOutColumnTypes[iter.OutRowColumnIndex] != zProto.ColumnType_PB_TIMESTAMP &&
		iter.MetaOutColumnTypes[iter.OutRowColumnIndex] != zProto.ColumnType_PB_DATE &&
		iter.MetaOutColumnTypes[iter.OutRowColumnIndex] != zProto.ColumnType_PB_NUMERIC) {
		iter.PanicTypeAssert(iter.OutRowColumnIndex, iter.ExternalRowNumber, s)
	}
	iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, false)
	iter.ResultTable.DataString = append(iter.ResultTable.DataString, s)
	iter.WriteBufferBytes += uint64(len(s));
	iter.afterWriteValue()
	if (iter.WriteBufferBytes > MAX_DATASIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueIntBig(i big.Int) {
	iter.beforeWriteValue()
	if iter.MetaOutColumnTypes[iter.OutRowColumnIndex] != zProto.ColumnType_PB_NUMERIC {
		iter.PanicTypeAssert(iter.OutRowColumnIndex, iter.ExternalRowNumber, i)
	}
	iter.ResultTable.DataNulls = append(iter.ResultTable.DataNulls, false)
	iter.ResultTable.DataString = append(iter.ResultTable.DataString, i.String())
	iter.WriteBufferBytes += uint64(len(i.String()));
	iter.afterWriteValue()
	if (iter.WriteBufferBytes > MAX_DATASIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) Emit(out ...interface{}) {
	// emits one row
	// if size of all emmited rows in the buffer is larger then out buf size, then flush the buffer.
	if len(out) == iter.MetaOutRowSize {
		iter.writeRow(&out)
	} else {
		log.Panic("Incorrect number of rows - expecting ", iter.MetaOutRowSize, ", got ", len(out))
	}
	if (iter.WriteBufferBytes > MAX_DATASIZE) {
		iter.EmitFlush();
	}
}
