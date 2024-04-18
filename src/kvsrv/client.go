package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
// import "log"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	client_id int			// unique identification of client
	request_id int			// id of this request 
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd, next_client_id int) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.client_id = next_client_id
	ck.request_id = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs 
	var reply GetReply
	for {
		args = GetArgs{Key: key}
		reply = GetReply{}
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			break
		} 
		// message lost, retry
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	var args PutAppendArgs
	var reply PutAppendReply

	for {
		args = PutAppendArgs{Key: key, Value: value, ClientId: ck.client_id}
		reply = PutAppendReply{}
		ok := ck.server.Call("KVServer." + op, &args, &reply)
		if ok {
			break
		} 
	}
	ck.request_id++

	for {
		clean_args := PutAppendArgs{ClientId: ck.client_id}
		clean_reply := PutAppendReply{}
		ok := ck.server.Call("KVServer.Clean", &clean_args, &clean_reply)
		if ok {
			break
		}
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
