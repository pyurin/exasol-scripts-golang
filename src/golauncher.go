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
	exaContext.ExaMeta = make(map[string]string);
	exaContext.ZSocket, _ = zmq.NewSocket(zmq.REQ)
}

func runProcess(connectionString string) {
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
			exago.Comm(exaContext, zProto.MessageType_MT_CLOSE, []zProto.MessageType{}, &ZErrorMsg)
			exaContext.ZSocket.Close()
		}
	}()

	exaContext.ZInfoMsg = exago.Comm(exaContext, zProto.MessageType_MT_CLIENT, []zProto.MessageType{zProto.MessageType_MT_INFO}, nil);
	exaContext.ConnectionId = *exaContext.ZInfoMsg.ConnectionId;
	exaContext.ExaMeta["ScriptName"] = *exaContext.ZInfoMsg.Info.ScriptName;
	exaContext.ExaMeta["SourceCode"] = *exaContext.ZInfoMsg.Info.SourceCode;
	log.Println("Loaded meta: ", exaContext.ExaMeta);

	exaContext.ZMetaMsg = exago.Comm(exaContext, zProto.MessageType_MT_META, []zProto.MessageType{zProto.MessageType_MT_META}, nil);

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

		func Run(iter *exago.ExaIter) interface{} {
		        return fmt.Sprintln("Type: ", reflect.TypeOf(*iter.RowColumn["kvaal"]), ", row: ", iter.Row, " / ", iter.RowColumn)
		}`
	loadScriptFunction(&v)

	return;
	*/
	runProcess(os.Args[1])
	goPath = os.Args[2]
	goCache = os.Args[3]
	defer func() {
		exaContext.ZSocket.Close()
	}()
}

func executeScriptFunc(scriptFuncSym plugin.Symbol, iter *exago.ExaIter, expectResult bool) []interface{} {
	defer func() {
		if r := recover(); r != nil {
			log.Panic("Failed executing script ", *exaContext.ZInfoMsg.Info.ScriptName, ":\n", fmt.Sprint(r))
		}
	}()
	if expectResult {
		if reflect.TypeOf(scriptFuncSym) != reflect.TypeOf(func(*exago.ExaIter)(interface{}){return nil}) {
			log.Panic("Script function type is not compatible. It must be `func(*exago.ExaIter) interface{} `, but it's \n", reflect.TypeOf(scriptFuncSym))
		}
		scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(interface{}) )
		return []interface{}{scriptFunc(iter)};
	} else {
		if reflect.TypeOf(scriptFuncSym) != reflect.TypeOf(func(*exago.ExaIter)(){}) {
			log.Panic("Script function type is not compatible. It must be `func(*exago.ExaIter) (w/o return)`, but it's \n", reflect.TypeOf(scriptFuncSym))
		}
		scriptFunc := scriptFuncSym.(func(*exago.ExaIter)() )
		scriptFunc(iter);
		return nil;
	}
}

func multiCallIteration(scriptFuncSym plugin.Symbol) {
	iter := exago.NewExaIter(exaContext)
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
						//log.Println("Running script start")
						//scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(interface{}) )
						//iter.Emit([]interface{}{scriptFunc(iter)}...)
						iter.Emit(executeScriptFunc(scriptFuncSym, iter, true)...)
						//log.Println("Running script finished")
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
						executeScriptFunc(scriptFuncSym, iter, false)
						if iter.Next() == false {
							break;
						}
					}
				}
			}
			if *exaContext.ZMetaMsg.Meta.InputIterType == zProto.IterType_PB_MULTIPLE {
				if *exaContext.ZMetaMsg.Meta.OutputIterType == zProto.IterType_PB_EXACTLY_ONCE {
					// script(SET) RETURNS
					//scriptFunc := scriptFuncSym.(func(*exago.ExaIter)(interface{}) )
					//iter.Emit([]interface{}{scriptFunc(iter)}...)
					iter.Emit(executeScriptFunc(scriptFuncSym, iter, true)...)
				}
				if *exaContext.ZMetaMsg.Meta.OutputIterType == zProto.IterType_PB_MULTIPLE {
					// script(SET) EMITS
					//scriptFunc := scriptFuncSym.(func(*exago.ExaIter)() )
					//scriptFunc(iter);
					executeScriptFunc(scriptFuncSym, iter, false)
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

