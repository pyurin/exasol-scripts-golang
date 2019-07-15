package exago

import (
	zProto "zmqcontainer"
	"time"
	"math/big"
	apd "github.com/cockroachdb/apd"
)


func (iter *ExaIter) ClearResultData() {
	if iter.writerZMsg.Emit == nil {
		iter.writerZMsg.Emit = new (zProto.ExascriptEmitDataReq);
		iter.writerZMsg.Emit.Table = new (zProto.ExascriptTableData);
	}
	// lets try to reuse the message as of optimization. so we'll have to reset the values manually
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

/**
 * Before writing a single value
 */
func (iter *ExaIter) beforeWriteValue() {
	if iter.writerRowColumnIndex == 0 {
		// first column in row
		iter.writerEmitTable.RowNumber = append(iter.writerEmitTable.RowNumber, iter.readerExtRowNumber)
		iter.writerBufferLen += 8;
	}
}

/**
 * Before writing a single value - if it's last column - flush emit buf
 */
func (iter *ExaIter) afterWriteValue() {
	iter.writerRowColumnIndex++
	if iter.writerRowColumnIndex == iter.writerRowSize {
		// last field in row
		iter.writerRows++;
		iter.writerRowColumnIndex = 0
		if (iter.writerBufferLen > OUTBUF_MAX_SIZE) {
			iter.EmitFlush();
		}
	}
}

func (iter *ExaIter) EmitFlush() {
	Comm(iter.exaContext, zProto.MessageType_MT_EMIT, []zProto.MessageType{zProto.MessageType_MT_EMIT}, iter.writerZMsg)
	iter.ClearResultData();
}

func (iter *ExaIter) EmitInt64(i int64) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_INT64 {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, i)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	iter.writerEmitTable.DataInt64 = append(iter.writerEmitTable.DataInt64, i)
	iter.writerBufferLen += 8 + 1
	iter.afterWriteValue()
}

func (iter *ExaIter) EmitInt32(i int32) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_INT32 {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, i)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	iter.writerEmitTable.DataInt32 = append(iter.writerEmitTable.DataInt32, i)
	iter.writerBufferLen += 4 + 1
	iter.afterWriteValue()
}


func (iter *ExaIter) EmitBool(b bool) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_BOOLEAN {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, b)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	iter.writerEmitTable.DataBool = append(iter.writerEmitTable.DataBool, b)
	iter.writerBufferLen += 1 + 1
	iter.afterWriteValue()
}

func (iter *ExaIter) EmitTime(t time.Time) {
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
}


func (iter *ExaIter) EmitFloat64(f float64) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_DOUBLE {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, f)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	iter.writerEmitTable.DataDouble = append(iter.writerEmitTable.DataDouble, f)
	iter.writerBufferLen += 8
	iter.afterWriteValue()
}

func (iter *ExaIter) EmitNull() {
	iter.beforeWriteValue()
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, true)
	iter.writerBufferLen += 1;
	iter.afterWriteValue()
}

func (iter *ExaIter) EmitString(s string) {
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
}

func (iter *ExaIter) EmitDecimalApd(d apd.Decimal) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_NUMERIC {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, d)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	strVal := d.Text('f')
	iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, strVal)
	iter.writerBufferLen += uint64(len(strVal));
	iter.afterWriteValue()
}

func (iter *ExaIter) EmitIntBig(i big.Int) {
	iter.beforeWriteValue()
	if iter.writerColumnTypes[iter.writerRowColumnIndex] != zProto.ColumnType_PB_NUMERIC {
		iter.PanicTypeAssert(iter.writerRowColumnIndex, iter.readerExtRowNumber, i)
	}
	iter.writerEmitTable.DataNulls = append(iter.writerEmitTable.DataNulls, false)
	strVal := i.String()
	iter.writerEmitTable.DataString = append(iter.writerEmitTable.DataString, strVal)
	iter.writerBufferLen += uint64(len(strVal));
	iter.afterWriteValue()
}
