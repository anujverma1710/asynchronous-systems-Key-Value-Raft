package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

const (
	Follower State = 1 + iota
	Candidate
	Leader
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on Leaders, reinitialized after election
	nextIndex  []int
	matchIndex []int

	// additional properties used for restting timeout, for tracking state changes
	state            State
	isLeaderElection bool

	stateChangeChannel chan bool
	applyCh            chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var term int
	if d.Decode(&log) != nil ||
		d.Decode(&term) != nil {
		panic("[Problem occured in persistance state in log or current term]")
	} else {
		rf.log = log
		rf.currentTerm = term
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// Invoked By Leader to replicate log entries; also used as heartbeat
//
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	//Receiver implementation
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.stateChangeChannel <- true

		//If votedFor is null or candidateId
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.isLeaderElection = false
			reply.VoteGranted = true
		}

		isLogUpToDate := false
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index {
			isLogUpToDate = true
		}
		// If candidate’s log is atleast as up-to-date as receiver’s log
		if isLogUpToDate {
			rf.votedFor = args.CandidateId
			rf.isLeaderElection = false
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}

		rf.persist()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
//Invoked by leader to replicate log entries; also used as
//heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		rf.votedFor = args.LeaderId
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.isLeaderElection = false

		if len(args.Entries) > 0 {
			isLogUpdate := true
			if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Index != args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				isLogUpdate = false
				reply.Success = false
			}
			if isLogUpdate {
				reply.Success = true

				rf.log = rf.log[:args.PrevLogIndex+1]

				//appending any new entries not already in Log
				for i := 0; i < len(args.Entries); i++ {
					rf.log = append(rf.log, args.Entries[i])
				}
				rf.commitIndex = args.LeaderCommit
				if len(rf.log)-1 < args.LeaderCommit {
					rf.commitIndex = len(rf.log) - 1
				}

				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					rf.lastApplied++
					rf.applyCh <- ApplyMsg{Command: rf.log[i].Command, CommandIndex: rf.log[i].Index, CommandValid: true}
				}
			}

		} else if rf.log[len(rf.log)-1].Index == args.PrevLogIndex && rf.log[len(rf.log)-1].Term == args.PrevLogTerm {
			reply.Success = true
			rf.commitIndex = args.LeaderCommit
			if len(rf.log)-1 < args.LeaderCommit {
				rf.commitIndex = len(rf.log) - 1
			}
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.lastApplied++
				rf.applyCh <- ApplyMsg{Command: rf.log[i].Command, CommandIndex: rf.log[i].Index, CommandValid: true}
			}
		}
		if args.Term > rf.currentTerm {
			rf.stateChangeChannel <- true
		}
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	index = len(rf.log)
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
	}

	if isLeader {
		rf.log = append(rf.log, LogEntry{index, term, command})
		rf.persist()
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.state = Follower
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.stateChangeChannel = make(chan bool, 60)

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)

	go func() {
		defer waitGroup.Done()

		for {
			select {
			default:
				rf.mu.Lock()
				switch rf.state {
				case Follower:
					rf.checkFollowerState()
				case Candidate:
					rf.checkCandidateState()
				case Leader:
					rf.checkLeaderState()
				}
			}
		}
	}()

	return rf
}

func (rf *Raft) checkLeaderState() {
	for index := range rf.peers {

		if index != rf.me {

			reply := AppendEntriesReply{}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      rf.log[rf.nextIndex[index]:],
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.log[rf.nextIndex[index]-1].Index,
				PrevLogTerm:  rf.log[rf.nextIndex[index]-1].Term}

			go func(index int) {
				//Upon election: send initial empty AppendEntries RPCs
				//(heartbeat) to each server; repeat during idle periods to
				//prevent election timeouts
				isSuccessAppendEntriesRPC := rf.sendAppendEntries(index, &args, &reply)
				if isSuccessAppendEntriesRPC {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Success {
						//command received from client: append entry to local log,
						//respond after entry applied to state machine
						if len(args.Entries) > 0 && args.Term == rf.currentTerm {
							rf.nextIndex[index] = rf.nextIndex[index] + len(args.Entries)
							if rf.nextIndex[index] > len(rf.log) {
								rf.nextIndex[index] = len(rf.log)
							}
							rf.updateCommittedIndex()
							for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
								rf.lastApplied++
								rf.applyCh <- ApplyMsg{Command: rf.log[i].Command, CommandIndex: rf.log[i].Index, CommandValid: true}
							}
						}
					} else {

						if reply.Term == rf.currentTerm {
							if rf.nextIndex[index] = 1; rf.nextIndex[index]/2 > 1 {
								rf.nextIndex[index] = rf.nextIndex[index] / 2
							}
						} else {
							rf.makeFollower()
						}
					}
				}
			}(index)
		}
	}
	rf.mu.Unlock()
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		timeout <- true
	}()
	select {
	case <-timeout:
	case <-rf.stateChangeChannel: //resetting timeout
	}
}

func (rf *Raft) checkCandidateState() {

	rf.currentTerm++    //Increment currentTerm
	rf.votedFor = rf.me //Vote for self
	voteCount := 1

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term}

	rf.persist()
	for index := range rf.peers {

		if index != rf.me {
			go func(index int) {
				reply := RequestVoteReply{}
				isSuccessRequestVoteRPC := rf.sendRequestVote(index, &args, &reply) //Send RequestVote RPCs to all other servers

				if isSuccessRequestVoteRPC {
					if reply.VoteGranted {
						rf.mu.Lock()
						voteCount = voteCount + 1
						//If votes received from majority of servers: become leader
						if rf.state == Candidate && voteCount > (len(rf.peers)/2) && args.Term == rf.currentTerm {
							rf.makeLeader()
							rf.updateNextIndexOfAllServers()
						}
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						//If AppendEntries RPC received from new leader: convert to follower
						if rf.state == Candidate && reply.Term > rf.currentTerm && args.Term == rf.currentTerm {
							rf.makeFollower()
						}
						rf.mu.Unlock()
					}
				}
			}(index)
		}
	}

	rf.mu.Unlock()
	rf.getRandomTimeout()
}

func (rf *Raft) checkFollowerState() {
	rf.isLeaderElection = true
	rf.mu.Unlock()
	rf.getRandomTimeout()
	rf.mu.Lock()
	//If election timeout elapses without receiving AppendEntries
	//RPC from current leader or granting vote to candidate:
	//convert to candidate
	if rf.state == Follower && rf.isLeaderElection {
		rf.state = Candidate
	}
	rf.mu.Unlock()
}

func (rf *Raft) updateCommittedIndex() {

	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N

	rf.matchIndex = make([]int, len(rf.nextIndex))
	copy(rf.matchIndex, rf.nextIndex)
	rf.matchIndex[rf.me] = len(rf.log)

	sort.Ints(rf.matchIndex)

	N := rf.matchIndex[len(rf.peers)/2] - 1

	if rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N
	}
}

func (rf *Raft) updateNextIndexOfAllServers() {
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
	}
}

func (rf *Raft) makeLeader() {
	rf.state = Leader
	rf.stateChangeChannel <- true
}

func (rf *Raft) makeFollower() {
	rf.isLeaderElection = false
	rf.stateChangeChannel <- true
	rf.state = Follower
}

func (rf *Raft) getRandomTimeout() {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(time.Duration(300+rand.Intn(300)) * time.Millisecond)
		timeout <- true
	}()
	select {
	case <-rf.stateChangeChannel:
	case <-timeout:
	}
}
