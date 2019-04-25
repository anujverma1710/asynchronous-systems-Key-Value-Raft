package shardmaster

import "raft"
import "labrpc"
import "sync"
import "labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs         []Config // indexed by config num
	notifyChannels  map[int]chan struct{}
	checkDuplicates map[int64]*RecentReply
}

type Op struct {
	// Your data here.
	ClientID int64
	OpIdx    int
	Op       string // "Join", "Leave", "Move" and "Query" operations

	Servers map[int][]string
	GIDs    []int
	Shard   int
	GID     int
	Num     int
}

type RecentReply struct {
	Seq int
}

type GroupStatus struct {
	group, count int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	operation := Op{
		ClientID: args.ClientID,
		OpIdx:    args.OperIndex,
		Op:       "Join",
		Servers:  args.Servers}

	sm.checkOperationRequest(&operation, func(success bool) {
		if !success {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
		}
	})
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	operation := Op{
		ClientID: args.ClientID,
		OpIdx:    args.OperIndex,
		Op:       "Leave",
		GIDs:     args.GIDs}

	sm.checkOperationRequest(&operation, func(success bool) {
		if !success {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
		}
	})
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	operation := Op{
		ClientID: args.ClientID,
		OpIdx:    args.OperIndex,
		Op:       "Move",
		Shard:    args.Shard, GID: args.GID}
	sm.checkOperationRequest(&operation, func(success bool) {
		if !success {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
		}
	})
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	operation := Op{
		ClientID: args.ClientID,
		OpIdx:    args.OperIndex,
		Op:       "Query",
		Num:      args.Num}

	sm.checkOperationRequest(&operation, func(success bool) {
		if !success {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
			// on success, past or current Config is copied
			sm.mu.Lock()
			sm.ConfigCopying(args.Num, &reply.Config)
			sm.mu.Unlock()
		}
	})
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) checkOperationRequest(cmd *Op, replySuccessStatus func(success bool)) {

	_, isLeader := sm.rf.GetState()
	if !isLeader {
		replySuccessStatus(false)
		return
	}

	sm.mu.Lock()
	dup, ok := sm.checkDuplicates[cmd.ClientID]
	if ok {
		if cmd.OpIdx <= dup.Seq {
			sm.mu.Unlock()
			replySuccessStatus(true)
			return
		}
	}

	// notify raft to agreement
	index, term, _ := sm.rf.Start(cmd)

	ch := make(chan struct{})
	sm.notifyChannels[index] = ch
	sm.mu.Unlock()

	replySuccessStatus(true)

	// wait for Raft to complete agreement
	select {
	case <-ch:
		curTerm, isLeader := sm.rf.GetState()

		// what if still leader, but different term? just let client retry
		if !isLeader || term != curTerm {
			replySuccessStatus(false)
			return
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(&Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	sm.notifyChannels = make(map[int]chan struct{})
	sm.checkDuplicates = make(map[int64]*RecentReply)

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for {
			select {
			case msg, ok := <-sm.applyCh:
				if ok && msg.Command != nil {
					cmd := msg.Command.(*Op)
					sm.mu.Lock()
					dup, ok := sm.checkDuplicates[cmd.ClientID]
					if !ok || dup.Seq < cmd.OpIdx {
						sm.checkDuplicates[cmd.ClientID] = &RecentReply{cmd.OpIdx}
						if cmd.Op == "Leave" {

							config := Config{}
							sm.ConfigCopying(-1, &config)
							config.Num++

							for _, k := range cmd.GIDs {
								delete(config.Groups, k)
							}

							sm.BalancingOfShard(&config)
							sm.configs = append(sm.configs, config)

						} else if cmd.Op == "Move" {

							config := Config{}
							sm.ConfigCopying(-1, &config)
							config.Num++
							config.Shards[cmd.Shard] = cmd.GID

							sm.configs = append(sm.configs, config)

						} else if cmd.Op == "Join" {

							config := Config{}
							sm.ConfigCopying(-1, &config)
							config.Num++

							for k, v := range cmd.Servers {
								var servers = make([]string, len(v))
								copy(servers, v)
								config.Groups[k] = servers
							}

							sm.BalancingOfShard(&config)
							sm.configs = append(sm.configs, config)

						} else if cmd.Op == "Query" {
							// no configuration change
						} else {
							panic("Operation is not valid")
						}
					}

					notifyCh, ok := sm.notifyChannels[msg.CommandIndex]
					if ok && notifyCh != nil {
						close(notifyCh)
						delete(sm.notifyChannels, msg.CommandIndex)
					}
					sm.mu.Unlock()
				}
			}
		}
	}()

	return sm
}

func (sm *ShardMaster) ConfigCopying(index int, config *Config) {
	if index == -1 || index >= len(sm.configs) {
		index = len(sm.configs) - 1
	}

	config.Groups = make(map[int][]string)

	config.Num = sm.configs[index].Num
	config.Shards = sm.configs[index].Shards
	for key, val := range sm.configs[index].Groups {
		var servers = make([]string, len(val))
		copy(servers, val)
		config.Groups[key] = servers
	}
}

func (sm *ShardMaster) BalancingOfShard(config *Config) {

	replicaGroups := len(config.Groups)

	if replicaGroups == 0 {
		return
	}

	average := len(config.Shards) / replicaGroups // shards per replica group

	if average == 0 {
		return
	}

	//  mapping of group to shard
	var reverseMapping = make(map[int][]int)
	for key, value := range config.Shards {
		reverseMapping[value] = append(reverseMapping[value], key)
	}

	var extraGroups []GroupStatus
	var lacking []GroupStatus

	count1, count2 := 0, 0
	// for new group
	for group := range config.Groups {
		_, ok := reverseMapping[group]
		if !ok {
			lacking = append(lacking, GroupStatus{group, average})
			count1 += average
		}
	}

	leftShards := len(config.Shards) - replicaGroups*average // extra shards after average

	// if some group is removed
	for key, val := range reverseMapping {
		_, ok := config.Groups[key]
		if ok {
			if len(val) < average {
				lacking = append(lacking, GroupStatus{key, average - len(val)})
				count1 += average - len(val)
			}
			if len(val) > average {

				if leftShards == 0 {
					extraGroups = append(extraGroups, GroupStatus{key, len(val) - average})
					count2 += len(val) - average
				} else if len(val) > average+1 {
					extraGroups = append(extraGroups, GroupStatus{key, len(val) - average - 1})
					count2 += len(val) - average - 1
					leftShards--
				} else if len(val) == average+1 {
					leftShards--
				}
			}

		} else {
			extraGroups = append(extraGroups, GroupStatus{key, len(val)})
			count2 += len(val)
		}
	}

	// compensation for lacking
	diff := count2 - count1
	if diff > 0 {
		if len(lacking) < diff {
			count := diff - len(lacking)
			for k := range config.Groups {
				if len(reverseMapping[k]) == average {
					lacking = append(lacking, GroupStatus{k, 0})
					count = count - 1
					if count == 0 {
						break
					}
				}
			}
		}
		for i := range lacking {
			lacking[i].count++
			diff = diff - 1
			if diff == 0 {
				break
			}
		}
	}

	// modify reverse
	for len(lacking) != 0 && len(extraGroups) != 0 {
		e := extraGroups[0]
		l := lacking[0]
		src := e.group
		dst := l.group
		if e.count < l.count {
			BalanceGroup(reverseMapping, src, dst, e.count)
			extraGroups = extraGroups[1:]
			lacking[0].count -= e.count
		} else if e.count > l.count {
			BalanceGroup(reverseMapping, src, dst, l.count)
			lacking = lacking[1:]
			extraGroups[0].count -= l.count
		} else {
			BalanceGroup(reverseMapping, src, dst, e.count)
			lacking = lacking[1:]
			extraGroups = extraGroups[1:]
		}
	}

	for k, v := range reverseMapping {
		for _, s := range v {
			config.Shards[s] = k
		}
	}
}

func BalanceGroup(data map[int][]int, source, destination, count int) {
	srcData, dstData := data[source], data[destination]
	if count > len(srcData) {
		panic("Error Occured while balancing Shard")
	}
	e := srcData[:count]
	dstData = append(dstData, e...)
	srcData = srcData[count:]

	data[source] = srcData
	data[destination] = dstData
}
