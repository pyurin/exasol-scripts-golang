package exago

import (
	zProto "zmqcontainer"
	"log"
	"time"
	"math"
	"math/big"
	apd "github.com/cockroachdb/apd"
)


func (iter *ExaIter) ClearResultData() {
	if iter.writerZMsg.Emit == nil {
		iter.writerZMsg.Emit = new (zProto.ExascriptEmitDataReq);
		iter.writerZMsg.Emit.Table = new (zProto.ExascriptTableData);
	}
	iter.writerRowsInGroup = 0;
	iter.writerRows = 0;
	iter.writerBufferLen = 0;
	// preallocate slices for optimization
	if (cap(iter.writerZMsg.Emit.Table.RowNumber) == 0) {
		//initial clean up
		iter.writerZMsg.Emit.Table.RowNumber = make([]uint64, 0, 100 * 1000)
		iter.writerZMsg.Emit.Table.DataNulls = make([]bool, 0, 100 * 1000)
		iter.writerZMsg.Emit.Table.DataBool = make([]bool, 0, 100 * 1000)
		iter.writerZMsg.Emit.Table.DataInt32 = make([]int32, 0, 100 * 1000)
		iter.writerZMsg.Emit.Table.DataInt64 = make([]int64, 0, 100 * 1000)
		iter.writerZMsg.Emit.Table.DataString = make([]string, 0, 100 * 1000)
		iter.writerZMsg.Emit.Table.DataDouble = make([]float64, 0, 100 * 1000)
	}
	iter.writerZMsg.Emit.Table.RowNumber = iter.writerZMsg.Emit.Table.RowNumber[:0]
	iter.writerZMsg.Emit.Table.DataNulls = iter.writerZMsg.Emit.Table.DataNulls[:0]
	iter.writerZMsg.Emit.Table.DataDouble = iter.writerZMsg.Emit.Table.DataDouble[:0]
	iter.writerZMsg.Emit.Table.DataBool = iter.writerZMsg.Emit.Table.DataBool[:0]
	iter.writerZMsg.Emit.Table.DataInt32 = iter.writerZMsg.Emit.Table.DataInt32[:0]
	iter.writerZMsg.Emit.Table.DataInt64 = iter.writerZMsg.Emit.Table.DataInt64[:0]
	iter.writerZMsg.Emit.Table.DataDouble = iter.writerZMsg.Emit.Table.DataDouble[:0]
	iter.writerZMsg.Emit.Table.DataString = iter.writerZMsg.Emit.Table.DataString[:0]
	iter.writerZMsg.Emit.Table.RowsInGroup = &iter.writerRowsInGroup;
	iter.writerZMsg.Emit.Table.Rows = &iter.writerRows;
	iter.writerEmitTable = iter.writerZMsg.Emit.Table;
}

func (iter *ExaIter) beforeWriteValue() {
	if iter.writerRowColumnIndex == 0 {
		// first column in row
		iter.writerEmitTable.RowNumber = append(iter.writerEmitTable.RowNumber, iter.readerExtRowNumber)
		iter.writerBufferLen += 8;
	}
}

func (iter *ExaIter) afterWriteValue() {
	iter.writerRowColumnIndex++
	if iter.writerRowColumnIndex == iter.writerRowSize {
		// last field in row
		iter.writerRows++;
		iter.writerRowColumnIndex = 0
	}
}


/**
  * Writes row into writerZMsg message buf
  */
func (iter *ExaIter) writeRow(out *[]interface{}) {
	iter.writerEmitTable.RowNumber = append(iter.writerEmitTable.RowNumber, iter.readerExtRowNumber)
	iter.writerBufferLen += 8;

	for fieldI, field := range(*out) {
		if field == nil {
			iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, true)
			iter.writerBufferLen++
		} else {
			iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
			switch iter.writerColumnTypes[fieldI] {
			case zProto.ColumnType_PB_NUMERIC:
				fallthrough
			case zProto.ColumnType_PB_STRING:
				switch field.(type){
				case string:
					iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, field.(string))
					iter.writerBufferLen += uint64(len(field.(string)));
				default:
					iter.PanicTypeAssert(fieldI, iter.readerExtRowNumber, field)
				}
			case zProto.ColumnType_PB_DOUBLE:
				switch field.(type){
				case float64:
					iter.writerEmitTable.DataDouble = append(iter.writerEmitTable.DataDouble, field.(float64))
					iter.writerBufferLen += 8
				default:
					iter.PanicTypeAssert(fieldI, iter.readerExtRowNumber, field)
				}
			case zProto.ColumnType_PB_BOOLEAN:
				switch field.(type){
				case bool:
					iter.writerEmitTable.DataBool = append(iter.writerEmitTable.DataBool, field.(bool))
					iter.writerBufferLen += 1
				default:
					iter.PanicTypeAssert(fieldI, iter.readerExtRowNumber, field)
				}
			case zProto.ColumnType_PB_INT32:
				switch field.(type){
				case int32:
					iter.writerEmitTable.DataInt32 = append(iter.writerEmitTable.DataInt32, field.(int32))
					iter.writerBufferLen += 4
				default:
					iter.PanicTypeAssert(fieldI, iter.readerExtRowNumber, field)
				}
			case zProto.ColumnType_PB_INT64:
				switch field.(type){
				case int64:
					iter.writerEmitTable.DataInt64 = append(iter.writerEmitTable.DataInt64, field.(int64))
					iter.writerBufferLen += 8
				default:
					iter.PanicTypeAssert(fieldI, iter.readerExtRowNumber, field)
				}
			case zProto.ColumnType_PB_TIMESTAMP:
				switch field.(type) {
				case float64:
					sec, dec := math.Modf(field.(float64));
					timeFormatted := (time.Unix(int64(sec), int64(dec*(1e9)))).Format("2006-01-02 15:04:05.999")
					iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, timeFormatted)
					iter.writerBufferLen += uint64(len(timeFormatted));
				case int64:
					timeFormatted := (time.Unix(field.(int64), 0)).Format("2006-01-02 15:04:05.999")
					iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, timeFormatted)
					iter.writerBufferLen += uint64(len(timeFormatted));
				case time.Time:
					timeFormatted := (field.(time.Time)).Format("2006-01-02 15:04:05.999")
					iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, timeFormatted)
					iter.writerBufferLen += uint64(len(timeFormatted));
				case string:
					iter.writerEmitTable.DataString = append(iter.writerZMsg.Emit.Table.DataString, field.(string))
					iter.writerBufferLen += uint64(len(field.(string)));
				default:
					iter.PanicTypeAssert(fieldI, iter.readerExtRowNumber, field)
				}
			case zProto.ColumnType_PB_DATE:
				switch field.(type) {
				case time.Time:
					timeFormatted := (field.(time.Time)).Format("2006-01-02")
					iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, timeFormatted)
					iter.writerBufferLen += uint64(len(timeFormatted));
				case string:
					iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, field.(string))
					iter.writerBufferLen += uint64(len(field.(string)));
				default:
					iter.PanicTypeAssert(fieldI, iter.readerExtRowNumber, field)
				}
			default:
				log.Panic("Undefined outcolumn type: ", iter.writerColumnTypes[fieldI])
			}
		}
	}
	iter.writerRows++;
}

func (iter *ExaIter) EmitFlush() {
	Comm(iter.exaContext, zProto.MessageType_MT_EMIT, []zProto.MessageType{zProto.MessageType_MT_EMIT}, iter.writerZMsg)
	iter.ClearResultData();
}

func (iter *ExaIter) EmitValueInt64(i int64) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_INT64 {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, i)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	iter.writerEmitTable.DataInt64 = append(iter.writerEmitTable.DataInt64, i)
	iter.writerBufferLen += 8 + 1
	iter.afterWriteValue()
	if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueInt32(i int32) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_INT32 {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, i)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	iter.writerEmitTable.DataInt32 = append(iter.writerEmitTable.DataInt32, i)
	iter.writerBufferLen += 4 + 1
	iter.afterWriteValue()
	if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
		iter.EmitFlush();
	}
}


func (iter *ExaIter) EmitValueBool(b bool) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_BOOLEAN {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, b)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	iter.writerEmitTable.DataBool = append(iter.writerEmitTable.DataBool, b)
	iter.writerBufferLen += 1 + 1
	iter.afterWriteValue()
	if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueTime(t time.Time) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] == zProto.ColumnType_PB_TIMESTAMP {
		iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, t.Format("2006-01-02 15:04:05.999"))
		iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
		iter.writerBufferLen += 24;
	} else if iter.writerColumnTypes[iter.writerRowColumnIndex] == zProto.ColumnType_PB_DATE {
		iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, t.Format("2006-01-02"))
		iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
		iter.writerBufferLen += 11;
	} else {
		iter.PanicTypeAssert(iter.writerRowColumnIndex,  iter.readerExtRowNumber, t)
	}
	iter.afterWriteValue()
	if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
		iter.EmitFlush();
	}
}


func (iter *ExaIter) EmitValueFloat64(f float64) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_DOUBLE {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, f)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	iter.writerEmitTable.DataDouble = append(iter.writerEmitTable.DataDouble, f)
	iter.writerBufferLen += 8
	iter.afterWriteValue()
	if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueNull() {
	iter.beforeWriteValue()
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, true)
	iter.writerBufferLen += 1;
	iter.afterWriteValue()
	if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueString(s string) {
	iter.beforeWriteValue()
	if (iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_STRING &&
		iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_TIMESTAMP &&
		iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_DATE &&
		iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_NUMERIC) {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, s)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, s)
	iter.writerBufferLen += uint64(len(s));
	iter.afterWriteValue()
	if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueDecimalApd(d apd.Decimal) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_NUMERIC {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, d)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	strVal := d.Text('f')
	iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, strVal)
	iter.writerBufferLen += uint64(len(strVal));
	iter.afterWriteValue()
	if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) EmitValueIntBig(i big.Int) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_NUMERIC {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, i)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	strVal := i.String()
	iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, strVal)
	iter.writerBufferLen += uint64(len(strVal));
	iter.afterWriteValue()
	if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
		iter.EmitFlush();
	}
}

func (iter *ExaIter) Emit(out ...interface{}) {
	// emits one row
	// if size of all emmited rows in the buffer is larger then out buf size, then flush the buffer.
	if len(out) == iter.writerRowSize {
		iter.writeRow(&out)
	} else {
		log.Panic("Incorrect number of rows - expecting ", iter.writerRowSize, ", got ", len(out))
	}
	if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
		iter.EmitFlush();
	}
}
