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
	UseSnapshot  bool
	Snapshot     []byte
}

//
// State of the server
//
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

	applyCh      chan ApplyMsg
	snapshotData []byte
}

func (rf *Raft) getLog(index int) LogEntry {
	return rf.log[index-rf.log[0].Index]
}

func (rf *Raft) getLogsByRange(startIndex int, endIndex int) []LogEntry {

	start := startIndex - rf.log[0].Index
	if start < 0 {
		start = 0
	}
	end := endIndex - rf.log[0].Index
	if end > len(rf.log) {
		end = len(rf.log)
	}

	return rf.log[start:end]
}

//
// GetRaftStateSize :Get Raft State Size
//
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
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
	Term          int
	Success       bool
	ConflictIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.updateCurrentTermAndState(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Receiver implementation
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// If candidate’s log is atleast as up-to-date as receiver’s log
	lastIndex := len(rf.log) + rf.log[0].Index - 1
	if rf.getLog(lastIndex).Term > args.LastLogTerm ||
		(rf.getLog(lastIndex).Term == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		return
	}
	if args.Term < rf.currentTerm {
		return
	}
	//If votedFor is null or candidateId
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	if rf.state == Follower {
		rf.isLeaderElection = true
	}
	rf.persist()
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
	rf.updateCurrentTermAndState(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.ConflictIndex = args.PrevLogIndex

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.PrevLogIndex < rf.log[0].Index || args.PrevLogIndex >= len(rf.log)+rf.log[0].Index || rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		if args.PrevLogIndex < rf.log[0].Index || args.PrevLogIndex >= len(rf.log)+rf.log[0].Index {
			reply.ConflictIndex = len(rf.log) + rf.log[0].Index
		} else {
			index := args.PrevLogIndex
			targetTerm := rf.getLog(args.PrevLogIndex).Term
			for index > rf.log[0].Index && rf.getLog(index-1).Term == targetTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		reply.Success = false
		return
	}

	conflictIndex := len(args.Entries) + args.PrevLogIndex + 1
	if conflictIndex > len(rf.log)+rf.log[0].Index {
		conflictIndex = len(rf.log) + rf.log[0].Index
	}

	for i := args.PrevLogIndex + 1; i < len(rf.log)+rf.log[0].Index && i < len(args.Entries)+args.PrevLogIndex+1; i++ {
		if rf.getLog(i).Term != args.Entries[i-args.PrevLogIndex-1].Term {
			conflictIndex = i
			break
		}
	}

	//appending any new entries not already in Log
	if conflictIndex != len(args.Entries)+args.PrevLogIndex+1 {
		rf.log = append(rf.getLogsByRange(0, conflictIndex), args.Entries[conflictIndex-(args.PrevLogIndex+1):]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		commitIdx := len(rf.log) + rf.log[0].Index - 1
		if commitIdx > args.LeaderCommit {
			commitIdx = args.LeaderCommit
		}

		rf.commitIndex = commitIdx
	}
	rf.isLeaderElection = true
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type Snapshot struct {
	Data              []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// InstallSnapshot : If existing log entry has same index and term as snapshot’s
// last included entry, retain log entries following it and reply
// Discard the entire log
// Reset state machine using snapshot contents (and load
// snapshot’s cluster configuration)
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.updateCurrentTermAndState(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if len(rf.log)+rf.log[0].Index > args.LastIncludedIndex && rf.log[0].Index <= args.LastIncludedIndex &&
		rf.getLog(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.log = rf.getLogsByRange(args.LastIncludedIndex, len(rf.log)+rf.log[0].Index)
	} else {
		rf.log = []LogEntry{LogEntry{args.LastIncludedIndex, args.LastIncludedTerm, nil}}
	}
	rf.log[0].Index = args.LastIncludedIndex
	rf.isLeaderElection = true

	msg := ApplyMsg{CommandIndex: args.LastIncludedIndex, UseSnapshot: true, Snapshot: args.Data}

	rf.applyCh <- msg
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.snapshotData = args.Data
	rf.persist()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(Snapshot{Data: rf.snapshotData, LastIncludedIndex: rf.log[0].Index, LastIncludedTerm: rf.log[0].Term})
	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, w.Bytes())
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	index = len(rf.log) + rf.log[0].Index
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

	rf.isLeaderElection = false
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go rf.runElectionOfLeader()

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		for {
			select {
			default:
				switch rf.state {
				case Leader:
					rf.checkLeaderState()
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	go rf.updateApplyMsg()

	return rf
}

func (rf *Raft) checkLeaderState() {
	for index := range rf.peers {

		if index != rf.me {
			go func(servIdx int) {
				nextIndex := rf.nextIndex[servIdx]
				for nextIndex > 0 {
					rf.mu.Lock()
					if nextIndex > rf.log[0].Index {

						reply := AppendEntriesReply{}

						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							Entries:      rf.getLogsByRange(nextIndex, len(rf.log)+rf.log[0].Index),
							LeaderCommit: rf.commitIndex,
							PrevLogIndex: rf.getLog(nextIndex - 1).Index,
							PrevLogTerm:  rf.getLog(nextIndex - 1).Term}

						rf.mu.Unlock()
						if rf.state != Leader {
							return
						}
						//Upon election: send initial empty AppendEntries RPCs
						//(heartbeat) to each server; repeat during idle periods to
						//prevent election timeouts
						isSuccessAppendEntriesRPC := rf.sendAppendEntries(servIdx, &args, &reply)
						rf.updateCurrentTermAndState(reply.Term)
						if isSuccessAppendEntriesRPC && reply.Success {
							//command received from client: append entry to local log,
							//respond after entry applied to state machine
							rf.mu.Lock()
							rf.nextIndex[servIdx] = nextIndex + len(args.Entries)
							rf.matchIndex[servIdx] = rf.nextIndex[servIdx] - 1
							rf.mu.Unlock()
							break
						} else if !reply.Success {
							nextIndex = reply.ConflictIndex
						}
					} else {
						reply := InstallSnapshotReply{}
						args := InstallSnapshotArgs{
							Term:              rf.currentTerm,
							LastIncludedIndex: rf.log[0].Index,
							LastIncludedTerm:  rf.log[0].Term,
							Data:              rf.snapshotData}
						rf.mu.Unlock()
						if rf.state != Leader {
							return
						}

						isSuccessInstallSnapshptRPC := rf.sendInstallSnapshot(servIdx, &args, &reply)
						rf.updateCurrentTermAndState(reply.Term)
						if isSuccessInstallSnapshptRPC {
							rf.mu.Lock()
							rf.nextIndex[servIdx] = rf.log[0].Index + 1
							rf.matchIndex[servIdx] = rf.nextIndex[servIdx] - 1
							rf.mu.Unlock()
							break
						}
					}
				}
			}(index)
		}
	}
}

func (rf *Raft) handleElection(term int) {

	votedChan := make(chan bool)
	args := RequestVoteArgs{term, rf.me, len(rf.log) + rf.log[0].Index - 1, rf.getLog(len(rf.log) + rf.log[0].Index - 1).Term}
	for idx := range rf.peers { // send vote request to everyone
		if idx != rf.me {
			go func(server int) {
				reply := RequestVoteReply{-1, false}
				ok := rf.sendRequestVote(server, &args, &reply)
				rf.updateCurrentTermAndState(reply.Term)
				select {
				case votedChan <- ok && reply.VoteGranted:
				case <-time.After(100 * time.Millisecond):
				}
			}(idx)
		}
	}

	noOfServers := len(rf.peers)
	//Vote for self
	receivedVotes := 1
	for i := 0; i < noOfServers-1; i++ {
		if <-votedChan {
			receivedVotes = receivedVotes + 1
		}
		if receivedVotes >= (noOfServers+1)/2 {
			break
		}
	}
	//If votes received from majority of servers: become leader
	if receivedVotes >= (noOfServers+1)/2 {
		rf.mu.Lock()
		if rf.state == Candidate && term >= rf.currentTerm {
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for idx := range rf.nextIndex {
				rf.nextIndex[idx] = len(rf.log) + rf.log[0].Index
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) runElectionOfLeader() {
	for {
		rf.getRandomTimeout()
		rf.mu.Lock()
		if rf.state != Leader && !rf.isLeaderElection {
			rf.state = Candidate
			rf.currentTerm = rf.currentTerm + 1
			rf.votedFor = rf.me
			rf.persist()
			go rf.handleElection(rf.currentTerm)
		}
		rf.isLeaderElection = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCurrentTermAndState(term int) {
	rf.mu.Lock()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	rf.mu.Unlock()
}

func (rf *Raft) updateCommittedIndex() {

	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N

	rf.mu.Lock()
	if rf.state == Leader {
		total := len(rf.matchIndex)
		sortedMatchIndex := make([]int, total)
		copy(sortedMatchIndex, rf.matchIndex)
		sort.Ints(sortedMatchIndex)
		N := sortedMatchIndex[(total+1)/2]

		for N > rf.commitIndex {
			if N < len(rf.log)+rf.log[0].Index && rf.getLog(N).Term == rf.currentTerm {
				rf.commitIndex = N
				break
			}
			N--
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) updateApplyMsg() {
	for {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			msg := ApplyMsg{CommandValid: true, CommandIndex: rf.lastApplied + 1, Command: rf.getLog(rf.lastApplied + 1).Command, UseSnapshot: false, Snapshot: []byte{}}
			rf.lastApplied++
			rf.applyCh <- msg
		}
		rf.mu.Unlock()
		rf.updateCommittedIndex()
		time.Sleep(10 * time.Millisecond)
	}
}

//
// UpdateSnapshot : Write data into snapshot file at given offset
//
func (rf *Raft) UpdateSnapshot(data []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < rf.log[0].Index || index >= len(rf.log)+rf.log[0].Index {
		return
	}
	//Save snapshot file, discard any existing or partial snapshot
	//with a smaller index
	rf.snapshotData = data
	rf.log = rf.getLogsByRange(index, len(rf.log)+rf.log[0].Index)
	rf.log[0].Index = index
	rf.lastApplied = index
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(Snapshot{Data: rf.snapshotData, LastIncludedIndex: rf.log[0].Index, LastIncludedTerm: rf.log[0].Term})
	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, w.Bytes())
	rf.persist()
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	snapshot := Snapshot{[]byte{}, 0, 0}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&snapshot)

	rf.log[0].Index = snapshot.LastIncludedIndex
	rf.lastApplied = snapshot.LastIncludedIndex
	rf.commitIndex = snapshot.LastIncludedIndex
	rf.snapshotData = snapshot.Data

	msg := ApplyMsg{CommandIndex: snapshot.LastIncludedIndex, UseSnapshot: true, Snapshot: snapshot.Data}
	go func() {
		rf.applyCh <- msg
	}()
}
func (rf *Raft) getRandomTimeout() {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(time.Duration(300+rand.Intn(300)) * time.Millisecond)
		timeout <- true
	}()
	select {
	case <-timeout:
	}
}
