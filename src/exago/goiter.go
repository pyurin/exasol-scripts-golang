package exago

import (
	zProto "exago/zmqcontainer"
	"log"
	"reflect"
	"time"
	"unsafe"
)

const OUTBUF_MAX_SIZE = 10 * 1000 * 1000;

const ERROR_INCOMPATIBLE_RETURN_TYPE = "Return/emit incompatible type";
const ERROR_INCOMPATIBLE_FUNCTION_FORMAT = "Script function type is not compatible";

type ExaIter struct {
	exaContext ExaContext
	// reader related vars
	readerIsFinished bool // dataset iteration finished flag
	readerZMsgRowIndex uint64 // iteration index within input zmsg
	readerZMsg *zProto.ExascriptResponse // input zmsg
	readerExtRowNumber uint64 // current db row number - set by reader and used by writer
	readerRowSize int // length of input row = len(exaContext.ZMetaMsg.Meta.InputColumns)
	readerColumnsMeta []*zProto.ExascriptMetadataColumnDefinition // = exaContext.ZMetaMsg.Meta.InputColumns
	readerInputOffsets ExaIterInputOffsets // iterator offsets within data types
	readerRowDataTimeBuf []time.Time // I'm not sure how golang works with unsafe.Pointer to var within func in terms of safety and resource consumpt. - let's use a single row buffer for it
	readerRow []unsafe.Pointer
	readerRowColumns map[string]int

	// writer related vars
	writerBufferLen uint64 // expected len of output msg
	writerZMsg *zProto.ExascriptRequest // buffer for out msg
	writerRowsInGroup uint64 // not used yet
	writerRows uint64 // counter of written rows
	writerRowColumnIndex int // used for value-by-value emit
	writerRowSize int
	writerColumnTypes []zProto.ColumnType // shortcut of PB output column types = exaContext.ZMetaMsg.Meta.OutputColumns[x].Type
	writerEmitTable *zProto.ExascriptTableData // ref of writerZMsg.Emit.Table
}


/**
 * Iterator used by run script
 */
func NewExaIter(exaContext ExaContext) *ExaIter {
	iter := &ExaIter{
		exaContext: exaContext,
		writerZMsg: new(zProto.ExascriptRequest),
	}
	iter.ClearResultData();
	iter.readerRowSize = len(iter.exaContext.ZMetaMsg.Meta.InputColumns)
	iter.readerColumnsMeta = iter.exaContext.ZMetaMsg.Meta.InputColumns
	for _, colInfo := range iter.exaContext.ZMetaMsg.Meta.OutputColumns {
		iter.writerColumnTypes = append(iter.writerColumnTypes, *colInfo.Type);
	}
	iter.writerRowSize = len(iter.exaContext.ZMetaMsg.Meta.OutputColumns)
	iter.initInputData();
	return iter;
}


func (iter *ExaIter) GetWriterColumnTypes() []zProto.ColumnType {
	return iter.writerColumnTypes
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
