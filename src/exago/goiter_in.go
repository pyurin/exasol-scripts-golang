package exago

import (
	zProto "zmqcontainer"
	"reflect"
	"log"
	"time"
	"unsafe"
	"math/big"
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
	iter.IsFinished = false;
	iter.InZMessage = nil;
	iter.inZMsgRowIndex = 0;
}

func (iter *ExaIter) Reset() bool {
	iter.InZMessage = Comm(iter.exaContext, zProto.MessageType_MT_RESET, []zProto.MessageType{zProto.MessageType_MT_RESET, zProto.MessageType_MT_DONE}, nil)
	if *iter.InZMessage.Type == zProto.MessageType_MT_DONE {
		for i, _ := range in_row {
			in_row[i] = nil;
		}
		iter.IsFinished = true;
		return false
	}
	iter.inTable = iter.InZMessage.Next.Table
	// reset input offsets
	p := reflect.ValueOf(&iter.inGlobalInputOffsets).Elem()
	p.Set(reflect.Zero(p.Type()))

	iter.IsFinished = false;
	iter.inZMsgRowIndex = 0
	iter.ExternalRowNumber = iter.InZMessage.Next.Table.RowNumber[iter.inZMsgRowIndex]
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
	if (iter.IsFinished) {
		return false;
	}
	if iter.inZMsgRowIndex++; iter.InZMessage == nil || iter.inZMsgRowIndex >= *iter.InZMessage.Next.Table.Rows {
		//need to read first row
		iter.InZMessage = Comm(iter.exaContext, zProto.MessageType_MT_NEXT, []zProto.MessageType{zProto.MessageType_MT_NEXT, zProto.MessageType_MT_DONE}, nil)

		if *iter.InZMessage.Type == zProto.MessageType_MT_DONE {
			log.Println("ITER.", "iterNext", " - finished")
			for i, _ := range in_row {
				in_row[i] = nil;
			}
			iter.IsFinished = true;
			return false
		}
		iter.inTable = iter.InZMessage.Next.Table
		// reset input offsets
		p := reflect.ValueOf(&iter.inGlobalInputOffsets).Elem()
		p.Set(reflect.Zero(p.Type()))

		iter.inZMsgRowIndex = 0
		iter.ExternalRowNumber = iter.InZMessage.Next.Table.RowNumber[iter.inZMsgRowIndex]
		iter.readRow()
		return true;
	} else {
		//reading next row
		iter.ExternalRowNumber = iter.InZMessage.Next.Table.RowNumber[iter.inZMsgRowIndex]
		iter.readRow()
		return true;
	}
}

func (iter *ExaIter) GetRowIndex() uint64 {
	return iter.inZMsgRowIndex
}

func (iter *ExaIter) Size() uint64 {
	if iter.InZMessage == nil {
		log.Panic("Can't get Size() because InZMessage is empty");
	}
	if iter.InZMessage.Next == nil {
		return 0;
	} else {
		return *iter.InZMessage.Next.Table.Rows;
	}
}


func (iter *ExaIter) ReadInt64(colI int) *int64 {
	if colI < 0 || colI >= iter.MetaInRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.MetaInRowSize)
	}
	switch *iter.metaInColumns[colI].Type {
		case zProto.ColumnType_PB_INT64:
			return (*int64)(in_row[colI])
		default:
			log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read int64 from ", *iter.metaInColumns[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.metaInColumns[colI].Type)])
			return nil;
	}
}

func (iter *ExaIter) ReadIntBig(colI int) *big.Int {
	if colI < 0 || colI >= iter.MetaInRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.MetaInRowSize)
	}
	switch *iter.metaInColumns[colI].Type {
	case zProto.ColumnType_PB_NUMERIC:
		if *iter.metaInColumns[colI].Scale == 0 {
			i := new (big.Int)
			i.SetString(*(*string)(in_row[colI]), 10)
			return i
		} else {
			log.Panic("Decimal with scale is not supported. Read as string");
			return nil
		}
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read intBig from ", *iter.metaInColumns[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.metaInColumns[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadInt32(colI int) *int32 {
	if colI < 0 || colI >= iter.MetaInRowSize {
		log.Panic(ERROR_READING_COLUMN, "Index out of bounds, trying to read col ", colI, " in row with size ", iter.MetaInRowSize)
	}
	switch *iter.metaInColumns[colI].Type {
	case zProto.ColumnType_PB_INT32:
		return (*int32)(in_row[colI])
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read int32 from ", *iter.metaInColumns[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.metaInColumns[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadBool(colI int) *bool {
	if colI < 0 || colI >= iter.MetaInRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.MetaInRowSize)
	}
	switch *iter.metaInColumns[colI].Type {
	case zProto.ColumnType_PB_BOOLEAN:
		return (*bool)(in_row[colI])
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read bool from ", *iter.metaInColumns[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.metaInColumns[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadFloat64(colI int) *float64 {
	if colI < 0 || colI >= iter.MetaInRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.MetaInRowSize)
	}
	switch *iter.metaInColumns[colI].Type {
	case zProto.ColumnType_PB_DOUBLE:
		return (*float64)(in_row[colI])
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read float64 from ", *iter.metaInColumns[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.metaInColumns[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadIsNull(colI int) bool {
	if colI < 0 || colI >= iter.MetaInRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.MetaInRowSize)
	}
	return (in_row[colI] == nil)
}

func (iter *ExaIter) ReadTime(colI int) *time.Time {
	if colI < 0 || colI >= iter.MetaInRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.MetaInRowSize)
	}
	switch *iter.metaInColumns[colI].Type {
	case zProto.ColumnType_PB_DATE:
		return (*time.Time)(in_row[colI]);
	case zProto.ColumnType_PB_TIMESTAMP:
		return (*time.Time)(in_row[colI]);
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read string from ", *iter.metaInColumns[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.metaInColumns[colI].Type)])
		return nil;
	}
}

func (iter *ExaIter) ReadString(colI int) *string {
	if colI < 0 || colI >= iter.MetaInRowSize {
		log.Panic(ERROR_READING_COLUMN, ", index out of bounds, trying to read col ", colI, " in row with size ", iter.MetaInRowSize)
	}
	switch *iter.metaInColumns[colI].Type {
	case zProto.ColumnType_PB_DATE:
		fallthrough
	case zProto.ColumnType_PB_TIMESTAMP:
		fallthrough
	case zProto.ColumnType_PB_NUMERIC:
		fallthrough
	case zProto.ColumnType_PB_STRING:
		return (*string)(in_row[colI]);
	default:
		log.Panic(ERROR_READING_COLUMN, ", incorrect column ", colI, " type, can't read string from ", *iter.metaInColumns[colI].TypeName, " / ", zProto.ColumnType_name[int32(*iter.metaInColumns[colI].Type)])
		return nil;
	}
}



//read next row from ZMessage
func (iter *ExaIter) readRow() {
	for colI, colInfo := range iter.metaInColumns {
		if (iter.InZMessage.Next.Table.DataNulls[ iter.inGlobalInputOffsets.Nulls ]) {
			iter.inGlobalInputOffsets.Nulls++;
			in_row[colI] = nil;
		} else {
			iter.inGlobalInputOffsets.Nulls++;
			switch *colInfo.Type {
			case zProto.ColumnType_PB_DOUBLE:
				in_row[colI] = unsafe.Pointer(&iter.InZMessage.Next.Table.DataDouble[ iter.inGlobalInputOffsets.Doubles ]);
				iter.inGlobalInputOffsets.Doubles++;
			case zProto.ColumnType_PB_INT32:
				in_row[colI] = unsafe.Pointer(&iter.InZMessage.Next.Table.DataInt32[ iter.inGlobalInputOffsets.Int32s ]);
				iter.inGlobalInputOffsets.Int32s++;
			case zProto.ColumnType_PB_INT64:
				in_row[colI] = unsafe.Pointer(&iter.InZMessage.Next.Table.DataInt64[ iter.inGlobalInputOffsets.Int64s ]);
				iter.inGlobalInputOffsets.Int64s++;
			case zProto.ColumnType_PB_BOOLEAN:
				in_row[colI] = unsafe.Pointer(&iter.InZMessage.Next.Table.DataBool[ iter.inGlobalInputOffsets.Bools ]);
				iter.inGlobalInputOffsets.Bools++;
			case zProto.ColumnType_PB_NUMERIC:
				in_row[colI] = unsafe.Pointer(&iter.InZMessage.Next.Table.DataString[ iter.inGlobalInputOffsets.Strings ]);
				iter.inGlobalInputOffsets.Strings++;
			case zProto.ColumnType_PB_TIMESTAMP:
				var err error
				in_rowDataTimeBuf[colI], err = time.Parse("2006-01-02 15:04:05.999999", iter.InZMessage.Next.Table.DataString[ iter.inGlobalInputOffsets.Strings ])
				if err != nil {
					log.Panic("Could not parse time ", iter.InZMessage.Next.Table.DataString[ iter.inGlobalInputOffsets.Strings ], "; ", err)
				}
				in_row[colI] = unsafe.Pointer(&in_rowDataTimeBuf[colI])
				iter.inGlobalInputOffsets.Strings++;
			case zProto.ColumnType_PB_DATE:
				var err error
				in_rowDataTimeBuf[colI], err = time.Parse("2006-01-02", iter.InZMessage.Next.Table.DataString[ iter.inGlobalInputOffsets.Strings ])
				if err != nil {
					log.Panic("Could not parse date ", iter.InZMessage.Next.Table.DataString[ iter.inGlobalInputOffsets.Strings ], "; ", err)
				}
				in_row[colI] = unsafe.Pointer(&in_rowDataTimeBuf[colI])
				iter.inGlobalInputOffsets.Strings++;
			case zProto.ColumnType_PB_STRING:
				in_row[colI] = unsafe.Pointer(&iter.InZMessage.Next.Table.DataString[ iter.inGlobalInputOffsets.Strings ]);
				iter.inGlobalInputOffsets.Strings++;
			default:
				log.Panic("Unknown column type: ", colInfo.Type);
			}
		}
	}
}
