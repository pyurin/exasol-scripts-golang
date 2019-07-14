package exago

import (
	zProto "zmqcontainer"
	"log"
	"reflect"
)

const MAX_DATASIZE = 100 * 1000 * 1000;

const ERROR_INCOMPATIBLE_RETURN_TYPE = "Return/emit incompatible type";
const ERROR_INCOMPATIBLE_FUNCTION_FORMAT = "Script function type is not compatible";

type ExaIter struct {
	IsFinished bool
	inZMsgRowIndex uint64
	InZMessage *zProto.ExascriptResponse
	ExternalRowNumber uint64
	exaContext ExaContext
	WriteBufferBytes uint64
	ResultZMsg *zProto.ExascriptRequest
	ResultRowsInGroup uint64
	ResultRows uint64
	MetaInRowSize int
	MetaOutRowSize int
	MetaOutColumnTypes []zProto.ColumnType
	OutRowColumnIndex int
	ResultTable *zProto.ExascriptTableData
	inTable *zProto.ExascriptTableData
	metaInColumns []*zProto.ExascriptMetadataColumnDefinition
	inGlobalInputOffsets ExaIterInputOffsets
}

/**
  * Usage: 

  *

  **/

func NewExaIter(exaContext ExaContext) *ExaIter {
	iter := &ExaIter{
		exaContext: exaContext,
		ResultZMsg: new(zProto.ExascriptRequest),
	}
	iter.ClearResultData();
	iter.MetaInRowSize = len(iter.exaContext.ZMetaMsg.Meta.InputColumns)
	iter.metaInColumns = iter.exaContext.ZMetaMsg.Meta.InputColumns
	for _, colInfo := range iter.exaContext.ZMetaMsg.Meta.OutputColumns {
		iter.MetaOutColumnTypes = append(iter.MetaOutColumnTypes, *colInfo.Type);
	}
	iter.MetaOutRowSize = len(iter.exaContext.ZMetaMsg.Meta.OutputColumns)
	iter.initInputData();
	return iter;
}


func (iter *ExaIter) PanicTypeAssert(fieldI int, extRowNum uint64, realVal interface{}) {
	if  *iter.exaContext.ZMetaMsg.Meta.OutputIterType == zProto.IterType_PB_EXACTLY_ONCE {
		//return
		log.Panic(
			ERROR_INCOMPATIBLE_RETURN_TYPE, "`", reflect.TypeOf(realVal), "`",
			" while expecting ",
			*iter.exaContext.ZMetaMsg.Meta.OutputColumns[fieldI].TypeName,
			"\n");
	} else {
		//emits
		log.Panic(
			ERROR_INCOMPATIBLE_RETURN_TYPE, "`", reflect.TypeOf(realVal), "`",
			" for column ",
			fieldI,
			" (name=", *iter.exaContext.ZMetaMsg.Meta.OutputColumns[fieldI].Name, "), ",
			" while expecting ",
			*iter.exaContext.ZMetaMsg.Meta.OutputColumns[fieldI].TypeName,
			"\n");
	}
}
