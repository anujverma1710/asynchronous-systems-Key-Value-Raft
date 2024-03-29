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
	configs         []Config
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

	success := sm.checkOperationRequest(&operation)
	if !success {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = "OK"
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	operation := Op{
		ClientID: args.ClientID,
		OpIdx:    args.OperIndex,
		Op:       "Leave",
		GIDs:     args.GIDs}

	success := sm.checkOperationRequest(&operation)
	if !success {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = "OK"
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	operation := Op{
		ClientID: args.ClientID,
		OpIdx:    args.OperIndex,
		Op:       "Move",
		Shard:    args.Shard, GID: args.GID}
	success := sm.checkOperationRequest(&operation)
	if !success {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = "OK"
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	operation := Op{
		ClientID: args.ClientID,
		OpIdx:    args.OperIndex,
		Op:       "Query",
		Num:      args.Num}

	success := sm.checkOperationRequest(&operation)
	if !success {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = "OK"
		sm.mu.Lock()
		sm.ConfigCopying(args.Num, &reply.Config)
		sm.mu.Unlock()
	}
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

func (sm *ShardMaster) checkOperationRequest(cmd *Op) bool {

	_, isLeader := sm.rf.GetState()
	if !isLeader {
		return false
	}

	sm.mu.Lock()
	dup, ok := sm.checkDuplicates[cmd.ClientID]
	if ok {
		if dup.Seq >= cmd.OpIdx {
			sm.mu.Unlock()
			return true
		}
	}

	index, term, _ := sm.rf.Start(cmd)

	ch := make(chan struct{})
	sm.notifyChannels[index] = ch
	sm.mu.Unlock()

	select {
	case <-ch:
		curTerm, isLeader := sm.rf.GetState()
		if term != curTerm || !isLeader {
			return false
		}
	}
	return true
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
				if ok {
					if msg.Command != nil {
						cmd := msg.Command.(*Op)
						sm.mu.Lock()
						dup, ok := sm.checkDuplicates[cmd.ClientID]
						if !ok || dup.Seq < cmd.OpIdx {
							sm.checkDuplicates[cmd.ClientID] = &RecentReply{Seq: cmd.OpIdx}
							if cmd.Op == "Leave" {

								config := Config{}
								sm.ConfigCopying(-1, &config)
								config.Num = config.Num + 1

								for _, k := range cmd.GIDs {
									delete(config.Groups, k)
								}

								sm.BalancingOfShard(&config)
								sm.configs = append(sm.configs, config)

							} else if cmd.Op == "Move" {

								config := Config{}
								sm.ConfigCopying(-1, &config)
								config.Num = config.Num + 1
								config.Shards[cmd.Shard] = cmd.GID

								sm.configs = append(sm.configs, config)

							} else if cmd.Op == "Join" {

								config := Config{}
								sm.ConfigCopying(-1, &config)
								config.Num = config.Num + 1

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
						if ok {
							if notifyCh != nil {
								close(notifyCh)
								delete(sm.notifyChannels, msg.CommandIndex)
							}
						}
						sm.mu.Unlock()
					}
				}
			}
		}
	}()

	return sm
}

func (sm *ShardMaster) ConfigCopying(idx int, config *Config) {
	if idx >= len(sm.configs) || idx == -1 {
		idx = len(sm.configs) - 1
	}

	config.Groups = make(map[int][]string)

	config.Num = sm.configs[idx].Num
	config.Shards = sm.configs[idx].Shards
	for key, val := range sm.configs[idx].Groups {
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

	shardsPerReplicaGroup := len(config.Shards) / replicaGroups

	if shardsPerReplicaGroup == 0 {
		return
	}

	var reverseMappingOfGroupToShard = make(map[int][]int)
	for key, value := range config.Shards {
		reverseMappingOfGroupToShard[value] = append(reverseMappingOfGroupToShard[value], key)
	}

	count1, count2, lacking, extraGroups := UpdateConfigForNewGroup(reverseMappingOfGroupToShard, shardsPerReplicaGroup, config)

	count2, count1, lacking, extraGroups = UpdateWhenGroupRemoved(config, shardsPerReplicaGroup, reverseMappingOfGroupToShard, lacking, extraGroups, replicaGroups, count2, count1)

	lacking, reverseMappingOfGroupToShard = Compensate(count2, count1, lacking, reverseMappingOfGroupToShard, config, shardsPerReplicaGroup)

	reverseMappingOfGroupToShard = RebalanceReverse(lacking, extraGroups, reverseMappingOfGroupToShard)

	for k, v := range reverseMappingOfGroupToShard {
		for _, s := range v {
			config.Shards[s] = k
		}
	}
}

func BalanceGroup(data map[int][]int, source, destination, count int) {
	srcData, dstData := data[source], data[destination]

	sAppend := srcData[:count]
	dstData = append(dstData, sAppend...)
	srcData = srcData[count:]

	data[source] = srcData
	data[destination] = dstData
}

func RebalanceReverse(lacking []GroupStatus, extraGroups []GroupStatus, reverseMapping map[int][]int) map[int][]int {
	for len(lacking) != 0 && len(extraGroups) != 0 {
		exG := extraGroups[0]
		lack := lacking[0]
		src := exG.group
		dst := lack.group
		if lack.count > exG.count {
			extraGroups = extraGroups[1:]

			BalanceGroup(reverseMapping, src, dst, exG.count)

			lacking[0].count = lacking[0].count - exG.count

		} else if lack.count < exG.count {
			lacking = lacking[1:]

			BalanceGroup(reverseMapping, src, dst, lack.count)

			extraGroups[0].count = extraGroups[0].count - lack.count

		} else {
			lacking = lacking[1:]

			BalanceGroup(reverseMapping, src, dst, exG.count)

			extraGroups = extraGroups[1:]
		}
	}
	return reverseMapping
}

func Compensate(count2 int, count1 int, lacking []GroupStatus, reverseMapping map[int][]int, config *Config, shardsPerReplicaGroup int) ([]GroupStatus, map[int][]int) {
	difference := count2 - count1
	if difference > 0 {
		if difference > len(lacking) {
			count := difference - len(lacking)
			for k := range config.Groups {
				if shardsPerReplicaGroup == len(reverseMapping[k]) {
					lacking = append(lacking, GroupStatus{group: k, count: 0})
					count = count - 1
					if count == 0 {
						break
					}
				}
			}
		}
		for i := range lacking {
			lacking[i].count = lacking[i].count + 1
			difference = difference - 1
			if difference == 0 {
				break
			}
		}
	}
	return lacking, reverseMapping
}

func UpdateWhenGroupRemoved(config *Config, shardsPerReplicaGroup int, reverseMapping map[int][]int,
	lacking []GroupStatus, extraGroups []GroupStatus, replicaGroups int, count2 int, count1 int) (int, int, []GroupStatus, []GroupStatus) {
	leftShards := len(config.Shards) - replicaGroups*shardsPerReplicaGroup // extra shards after shardsPerReplicaGroup
	for key, val := range reverseMapping {
		_, ok := config.Groups[key]
		if ok {
			if shardsPerReplicaGroup > len(val) {
				lacking = append(lacking, GroupStatus{group: key, count: shardsPerReplicaGroup - len(val)})
				count1 = count1 + shardsPerReplicaGroup - len(val)
			}
			if shardsPerReplicaGroup < len(val) {

				if leftShards == 0 {
					extraGroups = append(extraGroups, GroupStatus{group: key, count: len(val) - shardsPerReplicaGroup})
					count2 = count2 + len(val) - shardsPerReplicaGroup
				} else if shardsPerReplicaGroup+1 < len(val) {
					extraGroups = append(extraGroups, GroupStatus{group: key, count: len(val) - shardsPerReplicaGroup - 1})
					count2 = count2 + len(val) - shardsPerReplicaGroup - 1
					leftShards = leftShards - 1
				} else if shardsPerReplicaGroup+1 == len(val) {
					leftShards = leftShards - 1
				}
			}

		} else {
			extraGroups = append(extraGroups, GroupStatus{group: key, count: len(val)})
			count2 = count2 + len(val)
		}
	}

	return count2, count1, lacking, extraGroups
}

func UpdateConfigForNewGroup(reverseMapping map[int][]int, shardsPerReplicaGroup int, config *Config) (int, int, []GroupStatus, []GroupStatus) {
	var extraGroups []GroupStatus
	var lacking []GroupStatus

	count1, count2 := 0, 0

	for group := range config.Groups {
		_, ok := reverseMapping[group]
		if !ok {
			lacking = append(lacking, GroupStatus{group: group, count: shardsPerReplicaGroup})
			count1 = count1 + shardsPerReplicaGroup
		}
	}

	return count1, count2, lacking, extraGroups
}
