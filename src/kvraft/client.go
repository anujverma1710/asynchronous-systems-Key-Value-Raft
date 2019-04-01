package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderID int
	clientID     int64
	opRequestID  int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeaderID = 0
	ck.clientID = nrand()
	ck.opRequestID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	// incrementing Client RequestID
	ck.opRequestID++

	result := ""

	//sending RPC for each server
	for {
		args := GetArgs{key, ck.clientID, ck.opRequestID}
		reply := GetReply{}
		ok := ck.servers[ck.lastLeaderID].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			if reply.Err == ErrNoKey {
				break
			}
			if reply.Err == OK {
				result = reply.Value
				break
			}
		}
		if reply.Err != "TimeOut" {
			ck.lastLeaderID = (ck.lastLeaderID + 1) % len(ck.servers)
		}
		time.Sleep(20 * time.Millisecond)
	}

	return result
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// incrementing Client RequestID
	ck.opRequestID++

	//sending RPC for each server
	for {
		args := PutAppendArgs{key, value, op, ck.clientID, ck.opRequestID}
		reply := PutAppendReply{}
		ok := ck.servers[ck.lastLeaderID].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			break
		}
		if reply.Err != "TimeOut" {
			ck.lastLeaderID = (ck.lastLeaderID + 1) % len(ck.servers)
		}
		time.Sleep(20 * time.Millisecond)
	}

	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
