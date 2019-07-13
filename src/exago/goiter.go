package exago

import (
	zProto "zmqcontainer"
	"log"
	"reflect"
)

const MAX_DATASIZE = 100 * 1000 * 1000;

const ERROR_INCOMPATIBLE_RETURN_TYPE = "Return/emit incompatible type";

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
	exaContext ExaContext
	WriteBufferBytes uint64
	ResultZMsg *zProto.ExascriptRequest
	ResultRowsInGroup uint64
	ResultRows uint64
	InputOffsets ExaIterInputOffsets
	MetaOutRowSize int
	MetaOutColumnTypes []zProto.ColumnType
	OutRowColumnIndex int
	ResultTable *zProto.ExascriptTableData
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
	for _, colInfo := range iter.exaContext.ZMetaMsg.Meta.OutputColumns {
		iter.MetaOutColumnTypes = append(iter.MetaOutColumnTypes, *colInfo.Type);
	}
	iter.MetaOutRowSize = len(iter.exaContext.ZMetaMsg.Meta.OutputColumns)
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
