package kvsrv

import (
	"log"
	"sync"
	"os"
	"fmt"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ClientRequestInfo struct {
	request_id int
	value string
}
type KVServer struct {
	mu sync.Mutex
	kv_map map[string]string
	request_info sync.Map
	client_request_infos map[int]ClientRequestInfo
}

// check whether this request is duplicate
// request_id: id of this request
// client_request_state store id of the last done request, it must less than request_id or equal
func (kv *KVServer) CheckDuplicateRequest(client_id int, request_id int) (bool, string) {
	request_info, ok := kv.client_request_infos[client_id]
	if ok {			
		if request_info.request_id == request_id {	// a duplicate request
			return true, request_info.value
		} else {									// unduplicate request
			return false, ""
		}
	} else {										// this client request first time, not duplicate
		return false, ""
	}
}


// Get(key) fetches the current value for the key
// A Get for a non-existent key should return an empty string
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value, ok := kv.kv_map[key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

// Put(key, value) installs or replaces the value for a particular key in the map
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, hit := kv.request_info.Load(args.ClientId)
	if !hit {						// duplicate request
		kv.request_info.Store(args.ClientId, "")
		key := args.Key
		value := args.Value
		kv.kv_map[key] = value
	}


	// // check whether this request is duplicate
	// duplicate, _:= kv.CheckDuplicateRequest(args.ClientId, args.RequestId)
	// if duplicate {	// duplicate request

	// } else {		// a unduplicate request
	// 	key := args.Key
	// 	value := args.Value
	// 	kv.kv_map[key] = value
	// 	kv.client_request_infos[args.ClientId] = ClientRequestInfo{request_id: args.RequestId}
	// }
}

// Append(key, arg) appends arg to key's value and returns the old value. 
// An Append to a non-existent key should act as if the existing value were a zero-length string. 
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	old_value, hit := kv.request_info.Load(args.ClientId)
	
	if hit {			// duplicate, return directly
		reply.Value = old_value.(string)
	} else {			// unduplicate, exec
		key := args.Key
		value := args.Value
		old_value, ok := kv.kv_map[key]
		if !ok {
			old_value = ""
		}
		kv.kv_map[key] = old_value + value
		kv.request_info.Store(args.ClientId, old_value)
		reply.Value = old_value
	}

	// // check whether this request is duplicate
	// var value string
	// duplicate, value := kv.CheckDuplicateRequest(args.ClientId, args.RequestId)
	// if duplicate {				// duplicate request
	// 	reply.Value = value
	// } else {					// unduplicate request
	// 	key := args.Key
	// 	value := args.Value
	// 	old_value := kv.kv_map[key]
	// 	kv.kv_map[key] = old_value + value
	// 	kv.client_request_infos[args.ClientId] = ClientRequestInfo{request_id: args.RequestId, value: old_value}
	// 	reply.Value = old_value
	// }
}

func (kv *KVServer) Clean(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.request_info.Delete(args.ClientId)


	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	// delete (kv.client_request_infos, args.ClientId)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kv_map = make(map[string]string)
	kv.client_request_infos = make(map[int]ClientRequestInfo)
	log_file, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("open log file failed")
		os.Exit(-1)
	}
	log.SetOutput(log_file)
	return kv
}
