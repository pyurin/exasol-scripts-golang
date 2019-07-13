package exago

import (
	zProto "zmqcontainer"
	"reflect"
	"log"
	"time"
)


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