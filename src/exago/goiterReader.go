package exago

import (
	zProto "zmqcontainer"
	"reflect"
	"log"
	"time"
	"unsafe"
	"math/big"
	apd "github.com/cockroachdb/apd"
)

const ERROR_READING_COLUMN = "Error reading column ";

type ExaIterInputOffsets struct {
	Nulls uint64
	Strings uint64
	Int32s uint64
	Int64s uint64
	Bools uint64
	Doubles uint64
}

func (iter *ExaIter) CleanupInput() {
	iter.readerIsFinished = false;
	iter.readerZMsg = nil;
	iter.readerZMsgRowIndex = 0;
}

func (iter *ExaIter) Reset() bool {
	iter.readerZMsg = Comm(iter.exaContext, zProto.MessageType_MT_RESET, []zProto.MessageType{zProto.MessageType_MT_RESET, zProto.MessageType_MT_DONE}, nil)
	if *iter.readerZMsg.Type == zProto.MessageType_MT_DONE {
		for i, _ := range in_row {
			in_row[i] = nil;
		}
		iter.readerIsFinished = true;
		return false
	}
	// reset input offsets
	p := reflect.ValueOf(&iter.readerInputOffsets).Elem()
	p.Set(reflect.Zero(p.Type()))

	iter.readerIsFinished = false;
	iter.readerZMsgRowIndex = 0
	iter.readerExtRowNumber = iter.readerZMsg.Next.Table.RowNumber[iter.readerZMsgRowIndex]
	iter.readRow()
	return true;
}

var in_row []unsafe.Pointer
var in_rowColumns map[string]*unsafe.Pointer

/**
 * I'm not sure how golang works with unsafe.Pointer to var within func in terms of safety and resource consumpt. - let's use a single row buffer for it
 */
var in_rowDataTimeBuf []time.Time

func (iter *ExaIter) initInputData() {
	in_row = make([]unsafe.Pointer, len(iter.exaContext.ZMetaMsg.Meta.InputColumns))
	in_rowDataTimeBuf = make([]time.Time, len(iter.exaContext.ZMetaMsg.Meta.InputColumns))
	in_rowColumns = make(map[string]*unsafe.Pointer)
	for colI, colInfo := range iter.exaContext.ZMetaMsg.Meta.InputColumns {
		in_rowColumns[*colInfo.Name] = &in_row[colI];
	}
}

func (iter *ExaIter) Next() bool {
	if (iter.readerIsFinished) {
		return false;
	}
	if iter.readerZMsgRowIndex++; iter.readerZMsg == nil || iter.readerZMsgRowIndex >= *iter.readerZMsg.Next.Table.Rows {
		//need to read first row
		iter.readerZMsg = Comm(iter.exaContext, zProto.MessageType_MT_NEXT, []zProto.MessageType{zProto.MessageType_MT_NEXT, zProto.MessageType_MT_DONE}, nil)

		if *iter.readerZMsg.Type == zProto.MessageType_MT_DONE {
			log.Println("ITER.", "iterNext", " - finished")
			for i, _ := range in_row {
				in_row[i] = nil;
			}
			iter.readerIsFinished = true;
			return false
		}
		// reset input offsets
		p := reflect.ValueOf(&iter.readerInputOffsets).Elem()
		p.Set(reflect.Zero(p.Type()))

		iter.readerZMsgRowIndex = 0
		iter.readerExtRowNumber = iter.readerZMsg.Next.Table.RowNumber[iter.readerZMsgRowIndex]
		iter.readRow()
		return true;
	} else {
		//reading next row
		iter.readerExtRowNumber = iter.readerZMsg.Next.Table.RowNumber[iter.readerZMsgRowIndex]
		iter.readRow()
		return true;
	}
}

func (iter *ExaIter) GetRowIndex() uint64 {
	return iter.readerZMsgRowIndex
}

func (iter *ExaIter) Size() uint64 {
	if iter.readerZMsg == nil {
		log.Panic("Can't get Size() because readerZMsg is empty");
	}
	if iter.readerZMsg.Next == nil {
		return 0;
	} else {
		return *iter.readerZMsg.Next.Table.Rows;
	}
}


func (iter *ExaIter) ReadInt64(colI int) *int64 {
	if colI < 0 || colI >= iter.readerRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.readerRowSize)
	}
	switch *iter.readerColumnsMeta[colI].Type {
		case zProto.ColumnType_PB_INT64:
			return (*int64)(in_row[colI])
		default:
			log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read int64 from ", *iter.readerColumnsMeta[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.readerColumnsMeta[colI].Type)])
			return nil;
	}
}

func (iter *ExaIter) ReadDecimalApd(colI int) *apd.Decimal {
	if colI < 0 || colI >= iter.readerRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.readerRowSize)
	}
	switch *iter.readerColumnsMeta[colI].Type {
	case zProto.ColumnType_PB_NUMERIC:
		var d apd.Decimal;
		d.SetString(*(*string)(in_row[colI]))
		return &d
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read decimalApd from ", *iter.readerColumnsMeta[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.readerColumnsMeta[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadIntBig(colI int) *big.Int {
	if colI < 0 || colI >= iter.readerRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.readerRowSize)
	}
	switch *iter.readerColumnsMeta[colI].Type {
	case zProto.ColumnType_PB_NUMERIC:
		if *iter.readerColumnsMeta[colI].Scale == 0 {
			i := new (big.Int)
			i.SetString(*(*string)(in_row[colI]), 10)
			return i
		} else {
			log.Panic("Decimal with scale is not supported. Read as string");
			return nil
		}
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read intBig from ", *iter.readerColumnsMeta[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.readerColumnsMeta[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadInt32(colI int) *int32 {
	if colI < 0 || colI >= iter.readerRowSize {
		log.Panic(ERROR_READING_COLUMN, "Index out of bounds, trying to read col ", colI, " in row with size ", iter.readerRowSize)
	}
	switch *iter.readerColumnsMeta[colI].Type {
	case zProto.ColumnType_PB_INT32:
		return (*int32)(in_row[colI])
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read int32 from ", *iter.readerColumnsMeta[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.readerColumnsMeta[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadBool(colI int) *bool {
	if colI < 0 || colI >= iter.readerRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.readerRowSize)
	}
	switch *iter.readerColumnsMeta[colI].Type {
	case zProto.ColumnType_PB_BOOLEAN:
		return (*bool)(in_row[colI])
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read bool from ", *iter.readerColumnsMeta[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.readerColumnsMeta[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadFloat64(colI int) *float64 {
	if colI < 0 || colI >= iter.readerRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.readerRowSize)
	}
	switch *iter.readerColumnsMeta[colI].Type {
	case zProto.ColumnType_PB_DOUBLE:
		return (*float64)(in_row[colI])
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read float64 from ", *iter.readerColumnsMeta[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.readerColumnsMeta[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadIsNull(colI int) bool {
	if colI < 0 || colI >= iter.readerRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.readerRowSize)
	}
	return (in_row[colI] == nil)
}

func (iter *ExaIter) ReadTime(colI int) *time.Time {
	if colI < 0 || colI >= iter.readerRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.readerRowSize)
	}
	switch *iter.readerColumnsMeta[colI].Type {
	case zProto.ColumnType_PB_DATE:
		return (*time.Time)(in_row[colI]);
	case zProto.ColumnType_PB_TIMESTAMP:
		return (*time.Time)(in_row[colI]);
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read string from ", *iter.readerColumnsMeta[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.readerColumnsMeta[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadString(colI int) *string {
	if colI < 0 || colI >= iter.readerRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.readerRowSize)
	}
	switch *iter.readerColumnsMeta[colI].Type {
	case zProto.ColumnType_PB_DATE:
		fallthrough
	case zProto.ColumnType_PB_TIMESTAMP:
		fallthrough
	case zProto.ColumnType_PB_NUMERIC:
		fallthrough
	case zProto.ColumnType_PB_STRING:
		return (*string)(in_row[colI]);
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read string from ", *iter.readerColumnsMeta[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.readerColumnsMeta[colI].Type)])
		return nil;
	}
}



//read next row from ZMessage
func (iter *ExaIter) readRow() {
	for colI, colInfo := range iter.readerColumnsMeta {
		if (iter.readerZMsg.Next.Table.DataNulls[ iter.readerInputOffsets.Nulls ]) {
			iter.readerInputOffsets.Nulls++;
			in_row[colI] = nil;
		} else {
			iter.readerInputOffsets.Nulls++;
			switch *colInfo.Type {
			case zProto.ColumnType_PB_DOUBLE:
				in_row[colI] = unsafe.Pointer(&iter.readerZMsg.Next.Table.DataDouble[ iter.readerInputOffsets.Doubles ]);
				iter.readerInputOffsets.Doubles++;
			case zProto.ColumnType_PB_INT32:
				in_row[colI] = unsafe.Pointer(&iter.readerZMsg.Next.Table.DataInt32[ iter.readerInputOffsets.Int32s ]);
				iter.readerInputOffsets.Int32s++;
			case zProto.ColumnType_PB_INT64:
				in_row[colI] = unsafe.Pointer(&iter.readerZMsg.Next.Table.DataInt64[ iter.readerInputOffsets.Int64s ]);
				iter.readerInputOffsets.Int64s++;
			case zProto.ColumnType_PB_BOOLEAN:
				in_row[colI] = unsafe.Pointer(&iter.readerZMsg.Next.Table.DataBool[ iter.readerInputOffsets.Bools ]);
				iter.readerInputOffsets.Bools++;
			case zProto.ColumnType_PB_NUMERIC:
				in_row[colI] = unsafe.Pointer(&iter.readerZMsg.Next.Table.DataString[ iter.readerInputOffsets.Strings ]);
				iter.readerInputOffsets.Strings++;
			case zProto.ColumnType_PB_TIMESTAMP:
				var err error
				in_rowDataTimeBuf[colI], err = time.Parse("2006-01-02 15:04:05.999999", iter.readerZMsg.Next.Table.DataString[ iter.readerInputOffsets.Strings ])
				if err != nil {
					log.Panic("Could not parse time ", iter.readerZMsg.Next.Table.DataString[ iter.readerInputOffsets.Strings ], "; ", err)
				}
				in_row[colI] = unsafe.Pointer(&in_rowDataTimeBuf[colI])
				iter.readerInputOffsets.Strings++;
			case zProto.ColumnType_PB_DATE:
				var err error
				in_rowDataTimeBuf[colI], err = time.Parse("2006-01-02", iter.readerZMsg.Next.Table.DataString[ iter.readerInputOffsets.Strings ])
				if err != nil {
					log.Panic("Could not parse date ", iter.readerZMsg.Next.Table.DataString[ iter.readerInputOffsets.Strings ], "; ", err)
				}
				in_row[colI] = unsafe.Pointer(&in_rowDataTimeBuf[colI])
				iter.readerInputOffsets.Strings++;
			case zProto.ColumnType_PB_STRING:
				in_row[colI] = unsafe.Pointer(&iter.readerZMsg.Next.Table.DataString[ iter.readerInputOffsets.Strings ]);
				iter.readerInputOffsets.Strings++;
			default:
				log.Panic("Unknown column type: ", colInfo.Type);
			}
		}
	}
}
