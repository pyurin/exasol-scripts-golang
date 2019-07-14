package main

import (
	zmq "github.com/pebbe/zmq2"
	zProto "zmqcontainer"
	"exago"
	"os"
	"os/exec"
	"io/ioutil"
	"bytes"
	"plugin"
	"log"
	"fmt"
	"runtime/debug"
	"reflect"
	"time"
	"math/big"
	apd "github.com/cockroachdb/apd"
)

var exaContext exago.ExaContext;
var goPath string;
var goCache string;

func loadScriptFunction(scriptSrc *string, scriptName *string) plugin.Symbol {
	pluginFile := loadScriptFunction_compilePluginUncached(scriptSrc, scriptName)
	p, err := plugin.Open(pluginFile)
	if err != nil {
		log.Panic("Failed loading plugin file ", pluginFile, "\n", err)
	}
	scriptFuncSym, err := p.Lookup("Run")
	if err != nil {
		log.Panic("Plugin file does not contain Run function")
	}
	return scriptFuncSym;
}

func loadScriptFunction_compilePluginUncached(scriptSrc *string, scriptName *string) string {
	tmpDir, err := ioutil.TempDir("", "golang")
	if err != nil {
		log.Panic("Can't create temp dir");
	}
	srcFile := tmpDir + "/golang_script.go";
	pluginFile := tmpDir + "/golang_script.so";
	//log.Println("Writing source to ", srcFile);
	if err := ioutil.WriteFile(srcFile, []byte(*scriptSrc), 0777); err != nil {
		log.Panic("Can't write go source file for ", *scriptName, ": ", srcFile);
	}
	log.Println("go", "build", "-buildmode=plugin", "-o", pluginFile, srcFile)
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", pluginFile, srcFile)
	cmd.Env = os.Environ()
	//log.Println("Gopath = " + os.Getenv("GOPATH"))
	if goPath != "" {
		cmd.Env = append(cmd.Env, "GOPATH=" + goPath + ":" + os.Getenv("GOPATH"))
	}
	if goCache != "" {
		cmd.Env = append(cmd.Env, "GOCACHE=" + goCache)
	}
	//log.Println("ENV = ", cmd.Env);
	var out bytes.Buffer
	cmd.Stderr = &out
	cmd.Stdout = &out
	err2 := cmd.Run()
	if len(out.String()) > 0 {
		log.Println("Compiler stdout / stderr: ", out.String())
	}
	if err2 != nil {
		log.Panic("Failed compiling script ", *scriptName, "\n", err2.Error(), "\n", out.String())
	}
	log.Println("Complied")
	return pluginFile;
}

func init() {
	zProto.Initialize();
}

func runProcess(connectionString string) {
	exaContext.ExaMeta = make(map[string]string);
	exaContext.ZSocket, _ = zmq.NewSocket(zmq.REQ)
	err := exaContext.ZSocket.Connect(connectionString)
	if (err != nil) {
		log.Panic("Failed connecting zmq at ", connectionString, ": ", err)
	}
	defer func() {
		if r := recover(); r != nil {
			var ZErrorMsg zProto.ExascriptRequest
			errMsg := fmt.Sprint(r, "\nStack trace:\n", string(debug.Stack()))
			ZErrorMsg.Close = new (zProto.ExascriptClose)
			ZErrorMsg.Close.ExceptionMessage = &errMsg
			exago.Comm(exaContext, zProto.MessageType_MT_CLOSE, []zProto.MessageType{zProto.MessageType_MT_CLOSE, zProto.MessageType_MT_FINISHED}, &ZErrorMsg)
		}
		exaContext.ZSocket.Close()
	}()

	infoM := *exago.Comm(exaContext, zProto.MessageType_MT_CLIENT, []zProto.MessageType{zProto.MessageType_MT_INFO}, nil);
	exaContext.ZInfoMsg = &infoM
	exaContext.ConnectionId = *exaContext.ZInfoMsg.ConnectionId;
	exaContext.ExaMeta["ScriptName"] = *exaContext.ZInfoMsg.Info.ScriptName;
	exaContext.ExaMeta["SourceCode"] = *exaContext.ZInfoMsg.Info.SourceCode;
	log.Println("Loaded info: ", *exaContext.ZInfoMsg);

	metaM := *exago.Comm(exaContext, zProto.MessageType_MT_META, []zProto.MessageType{zProto.MessageType_MT_META}, nil);
	exaContext.ZMetaMsg = &metaM
	log.Println("Loaded meta: ", *exaContext.ZMetaMsg);

	var scriptFuncSym = loadScriptFunction(exaContext.ZInfoMsg.Info.SourceCode, exaContext.ZInfoMsg.Info.ScriptName)
	if *exaContext.ZMetaMsg.Meta.SingleCallMode {
		singleCallIteration(scriptFuncSym);
	} else {
		multiCallIteration(scriptFuncSym);
	}
	exago.Comm(exaContext, zProto.MessageType_MT_FINISHED, []zProto.MessageType{zProto.MessageType_MT_FINISHED}, nil)
}

func main() {
	/*
	v := `
			package main

		import (
			"exago"
			"reflect"
			"fmt"
		)

	loadScriptFunction(&v)

	return;
	*/
	if len(os.Args) == 1 {
		log.Panic("Program run, but no arguments given")
	}
	runProcess(os.Args[1])
	goPath = os.Args[2]
	goCache = os.Args[3]
}

func getExecuteScriptFunc(iter *exago.ExaIter, scriptFuncSym plugin.Symbol) (func()){
	if *exaContext.ZMetaMsg.Meta.OutputIterType == zProto.IterType_PB_EXACTLY_ONCE {
		// function has return
		switch iter.MetaOutColumnTypes[0] {
			case zProto.ColumnType_PB_NUMERIC:
				if *exaContext.ZMetaMsg.Meta.OutputColumns[0].Scale == 0 {
					if reflect.TypeOf(scriptFuncSym) == reflect.TypeOf(func(*exago.ExaIter)(*big.Int){return nil}) {
						scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(*big.Int) )
						return func() {
							if result := scriptFunc(iter); result != nil {
								iter.EmitValueIntBig(*result)
							} else {
								iter.EmitValueNull()
							}
						}
					} else {
						log.Panic(exago.ERROR_INCOMPATIBLE_FUNCTION_FORMAT, " It must be `func(*exago.ExaIter)(*big.Int)`, but it's \n", reflect.TypeOf(scriptFuncSym))
					}
				} else {
					// for decimal with no-zero scale
					if reflect.TypeOf(scriptFuncSym) == reflect.TypeOf(func(*exago.ExaIter)(*apd.Decimal){return nil}) {
						scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(*apd.Decimal) )
						return func() {
							if result := scriptFunc(iter); result != nil {
								iter.EmitValueDecimalApd(*result)
							} else {
								iter.EmitValueNull()
							}
						}
					} else if reflect.TypeOf(scriptFuncSym) == reflect.TypeOf(func(*exago.ExaIter)(*string){return nil}) {
						scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(*string) )
						return func() {
							if result := scriptFunc(iter); result != nil {
								iter.EmitValueString(*result)
							} else {
								iter.EmitValueNull()
							}
						}
					} else{
						log.Panic(exago.ERROR_INCOMPATIBLE_FUNCTION_FORMAT, " It must be `func(*exago.ExaIter)(*string)` or `func(*exago.ExaIter)(*apd.Decimal)`, but it's \n", reflect.TypeOf(scriptFuncSym))
					}
				}
				fallthrough
			case zProto.ColumnType_PB_STRING:
				if reflect.TypeOf(scriptFuncSym) != reflect.TypeOf(func(*exago.ExaIter)(*string){return nil}) {
					log.Panic(exago.ERROR_INCOMPATIBLE_FUNCTION_FORMAT, " It must be `func(*exago.ExaIter)(*string)`, but it's \n", reflect.TypeOf(scriptFuncSym))
				}
				scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(*string) )
				return func() {
					if result := scriptFunc(iter); result != nil {
						iter.EmitValueString(*result)
					} else {
						iter.EmitValueNull()
					}
				}
			case zProto.ColumnType_PB_DOUBLE:
				if reflect.TypeOf(scriptFuncSym) != reflect.TypeOf(func(*exago.ExaIter)(*float64){return nil}) {
					log.Panic(exago.ERROR_INCOMPATIBLE_FUNCTION_FORMAT, " It must be `func(*exago.ExaIter)(*float64)`, but it's \n", reflect.TypeOf(scriptFuncSym))
				}
				scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(*float64) )
				return func() {
					if result := scriptFunc(iter); result != nil {
						iter.EmitValueFloat64(*result)
					} else {
						iter.EmitValueNull()
					}
				}
			case zProto.ColumnType_PB_INT32:
				if reflect.TypeOf(scriptFuncSym) != reflect.TypeOf(func(*exago.ExaIter)(*int32){return nil}) {
					log.Panic(exago.ERROR_INCOMPATIBLE_FUNCTION_FORMAT, " It must be `func(*exago.ExaIter)(*int32)`, but it's \n", reflect.TypeOf(scriptFuncSym))
				}
				scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(*int32) )
				return func() {
					if result := scriptFunc(iter); result != nil {
						iter.EmitValueInt32(*result)
					} else {
						iter.EmitValueNull()
					}
				}
			case zProto.ColumnType_PB_BOOLEAN:
				if reflect.TypeOf(scriptFuncSym) != reflect.TypeOf(func(*exago.ExaIter)(*bool){return nil}) {
					log.Panic(exago.ERROR_INCOMPATIBLE_FUNCTION_FORMAT, " It must be `func(*exago.ExaIter)(*bool)`, but it's \n", reflect.TypeOf(scriptFuncSym))
				}
				scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(*bool) )
				return func() {
					if result := scriptFunc(iter); result != nil {
						iter.EmitValueBool(*result)
					} else {
						iter.EmitValueNull()
					}
				}
			case zProto.ColumnType_PB_INT64:
				if reflect.TypeOf(scriptFuncSym) != reflect.TypeOf(func(*exago.ExaIter)(*int64){return nil}) {
					log.Panic(exago.ERROR_INCOMPATIBLE_FUNCTION_FORMAT, " It must be `func(*exago.ExaIter)(*int64)`, but it's \n", reflect.TypeOf(scriptFuncSym))
				}
				scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(*int64) )
				return func() {
					if result := scriptFunc(iter); result != nil {
						iter.EmitValueInt64(*result)
					} else {
						iter.EmitValueNull()
					}
				}
			case zProto.ColumnType_PB_DATE:
				fallthrough
			case zProto.ColumnType_PB_TIMESTAMP:
				if reflect.TypeOf(scriptFuncSym) != reflect.TypeOf(func(*exago.ExaIter)(*time.Time){return nil}) {
					log.Panic(exago.ERROR_INCOMPATIBLE_FUNCTION_FORMAT, " It must be `func(*exago.ExaIter)(*time.Time)`, but it's \n", reflect.TypeOf(scriptFuncSym))
				}
				scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(*time.Time) )
				return func() {
					if result := scriptFunc(iter); result != nil {
						iter.EmitValueTime(*result)
					} else {
						iter.EmitValueNull()
					}
				}
			default:
				log.Panic("Unexpected return type logic: ", iter.MetaOutColumnTypes[0]);
		}
	} else {
		if reflect.TypeOf(scriptFuncSym) != reflect.TypeOf(func(*exago.ExaIter)(){}) {
			log.Panic(exago.ERROR_INCOMPATIBLE_FUNCTION_FORMAT, " It must be `func(*exago.ExaIter)` (w/o return), but it's \n", reflect.TypeOf(scriptFuncSym))
		}
		scriptFunc := scriptFuncSym.(func(*exago.ExaIter)() )
		return func() {
			scriptFunc(iter)
		}
	}
	return func() {
		log.Panic("Unexpected execute script logic")
	};
}

func multiCallIteration(scriptFuncSym plugin.Symbol) {
	iter := exago.NewExaIter(exaContext)
	localExecuteScriptFunc := getExecuteScriptFunc(iter, scriptFuncSym)
	for true {
		resp := exago.Comm(exaContext, zProto.MessageType_MT_RUN, []zProto.MessageType{zProto.MessageType_MT_RUN,zProto.MessageType_MT_CLEANUP}, nil)
		if *resp.Type == zProto.MessageType_MT_CLEANUP {
			break;
		} else if *resp.Type == zProto.MessageType_MT_RUN {
			iter.CleanupInput()
			if iter.Next() == false {
				log.Panic("Failed reading first row")
			}
			if *exaContext.ZMetaMsg.Meta.InputIterType == zProto.IterType_PB_EXACTLY_ONCE {
				if *exaContext.ZMetaMsg.Meta.OutputIterType == zProto.IterType_PB_EXACTLY_ONCE {
					// script (ROW) RETURNS
					for true {
						localExecuteScriptFunc();
						if iter.Next() {
							//log.Println("Fetching next row - row found")
						} else {
							//log.Println("Fetching next row - no more rows")
							break
						}
					}
				}
				if *exaContext.ZMetaMsg.Meta.OutputIterType == zProto.IterType_PB_MULTIPLE {
					// script(ROW) EMITS
					for true {
						//scriptFunc := scriptFuncSym.(func(*exago.ExaIter)() )
						//scriptFunc(iter);
						localExecuteScriptFunc();
						if iter.Next() == false {
							break;
						}
					}
				}
			}
			if *exaContext.ZMetaMsg.Meta.InputIterType == zProto.IterType_PB_MULTIPLE {
				if *exaContext.ZMetaMsg.Meta.OutputIterType == zProto.IterType_PB_EXACTLY_ONCE {
					localExecuteScriptFunc();
				}
				if *exaContext.ZMetaMsg.Meta.OutputIterType == zProto.IterType_PB_MULTIPLE {
					localExecuteScriptFunc();
				}
			}
			iter.EmitFlush()
		}
		resp = exago.Comm(exaContext, zProto.MessageType_MT_DONE, []zProto.MessageType{zProto.MessageType_MT_DONE,zProto.MessageType_MT_CLEANUP}, nil)
		if *resp.Type == zProto.MessageType_MT_CLEANUP {
			break;
		}
	}
}

func singleCallIteration(scriptFuncSym plugin.Symbol) {
	log.Panic("Single call mode not implemented");
}

