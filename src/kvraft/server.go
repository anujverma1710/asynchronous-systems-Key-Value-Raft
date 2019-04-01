package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key           string
	Value         string
	ClientId      int64
	OperationType string
	RequestID     int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dataStore               map[string]string     // for storing key value data
	resultTransferChannel   map[int64]chan Result // for transfering result according to request of Client (ClientId)
	lastCommitedOpRequestID map[int64]int64       // for recording requestId of every clients

	latestCommitedLogIndex int
}

type Result struct {
	OpRequestID int64
	Value       string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState() //server needs to detect that whether it is a leader or not

	if !isLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		//For entering an Op in the Raft log
		kv.rf.Start(Op{args.Key, "", args.ClientId, "Get", args.OpRequestIndex})

		kv.mu.Lock()
		_, ok := kv.resultTransferChannel[args.ClientId]
		if !ok {
			kv.resultTransferChannel[args.ClientId] = make(chan Result)
		}
		kv.mu.Unlock()

		select {
		case <-time.After(1 * time.Second):
			reply.Err = "TimeOut"
			reply.WrongLeader = true
		case result := <-kv.resultTransferChannel[args.ClientId]:
			if result.OpRequestID == args.OpRequestIndex {
				if result.Value == "" {
					reply.Err = ErrNoKey
				} else {
					reply.Err = OK
					reply.Value = result.Value
				}
			}

		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState() //server needs to detect that whether it is a leader or not

	if !isLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		//For entering an Op in the Raft log
		kv.rf.Start(Op{args.Key, args.Value, args.ClientId, args.Op, args.OpRequestIndex})

		kv.mu.Lock()
		_, ok := kv.resultTransferChannel[args.ClientId]
		if !ok {
			kv.resultTransferChannel[args.ClientId] = make(chan Result)
		}
		kv.mu.Unlock()

		select {
		case <-time.After(1 * time.Second):
			reply.Err = "TimeOut"
			reply.WrongLeader = true
		case result := <-kv.resultTransferChannel[args.ClientId]:
			if result.OpRequestID >= args.OpRequestIndex {
				reply.Err = OK
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.dataStore = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.resultTransferChannel = make(map[int64]chan Result)
	kv.lastCommitedOpRequestID = make(map[int64]int64)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go func() {
		for {
			msg := <-kv.applyCh
			if !msg.UseSnapshot {
				command := msg.Command.(Op)
				result := Result{command.RequestID, ""}

				if kv.lastCommitedOpRequestID[command.ClientId] < command.RequestID {
					if command.OperationType == "Append" {
						kv.dataStore[command.Key] = kv.dataStore[command.Key] + command.Value
					} else if command.OperationType == "Put" {
						kv.dataStore[command.Key] = command.Value
					}
					kv.lastCommitedOpRequestID[command.ClientId] = command.RequestID
				}
				if msg.CommandIndex > kv.latestCommitedLogIndex {
					kv.latestCommitedLogIndex = msg.CommandIndex
				}
				value, ok := kv.dataStore[command.Key]
				if ok {
					result.Value = value
				}

				select {
				case kv.resultTransferChannel[command.ClientId] <- result:
				default:
				}

			} else {
				data := msg.Snapshot
				r := bytes.NewBuffer(data)
				d := labgob.NewDecoder(r)
				d.Decode(&kv.lastCommitedOpRequestID)
				d.Decode(&kv.dataStore)
				kv.latestCommitedLogIndex = msg.CommandIndex
			}
		}
	}()

	return kv
}
