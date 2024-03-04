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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

/**
 * The following code is about the Raft object and its basic methods.
 */
// A Go object implementing a single Raft peer.
type Raft struct {
	mu               sync.RWMutex        // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	persister        *Persister          // Object to hold this peer's persisted state
	me               int                 // this peer's index into peers[]
	dead             int32               // set by Kill()
	currentTerm      int
	votedFor         int
	state            NodeState
	heartbeatTimeout *time.Timer
	electionTimeout  *time.Timer

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		dead:             0,
		currentTerm:      0,
		votedFor:         -1,
		state:            Follower,
		heartbeatTimeout: time.NewTimer(time.Duration(StableHeartbeatTimeout())),
		electionTimeout:  time.NewTimer(time.Duration(RandomizedElectionTimeout())),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	if me == 0 {
		Debug(dInfo, "peerNum %v", len(peers))
	}
	go rf.ticker()

	return rf
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.state != Leader {
		return -1, -1, false
	}
	// Your code here (2B).

	return -1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

/**
 * The following code is the ticker function.
 * It is about the election and heartbeat ticker.
 */
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.heartbeatTimeout.C:
			rf.mu.Lock()
			if rf.state == Leader {
				Debug(dTimer, "S%d Leader, the heartbeat timer timeout", rf.me)
				rf.broadcastHeartbeat()
				rf.heartbeatTimeout.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		case <-rf.electionTimeout.C:
			rf.mu.Lock()
			Debug(dTimer, "S%d {state: %v, term: %v}, the election timer timeout", rf.me, rf.state, rf.currentTerm)
			rf.changeState(Candidate)
			rf.currentTerm++
			rf.startElection()
			rf.electionTimeout.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		}
	}
}

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

const (
	HeartbeatTimeout = 125
	ElectionTimeout  = 1000
)

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}

/**
 * The following code is about the election process.
 */
func (rf *Raft) startElection() {
	request := rf.genRequestVoteRequest()
	Debug(dVote, "S%v starts election with RequestVoteRequest %v", rf.me, request)
	rf.votedFor = rf.me
	grantedVoteNum := 1

	// Your code here (2A, 2B).
	for peer := range rf.peers {
		if peer != rf.me {
			if peer == rf.me {
				continue
			}

			go rf.electionRequestOnce(peer, &grantedVoteNum, request)
		}
	}
}

func (rf *Raft) electionRequestOnce(peer int, grantedVoteNum *int, request *RequestVoteArgs) {
	reply := new(RequestVoteReply)
	if rf.sendRequestVote(peer, request, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dVote, "S%v received RequestVoteReply {%v} from S%v", rf.me, reply, peer)
		if rf.currentTerm == request.Term && rf.state == Candidate {
			if reply.VoteGranted {
				*grantedVoteNum++
				if *grantedVoteNum > len(rf.peers)/2 {
					rf.changeState(Leader)
					rf.broadcastHeartbeat()
				}
			}
		} else if reply.Term > rf.currentTerm {
			Debug(dVote, "S%v found higher term %v in RequestVoteReply %v from S%v", rf.me, reply.Term, reply, peer)
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.changeState(Follower)
		}
	}
}

func (rf *Raft) changeState(newState NodeState) {
	if rf.state == newState {
		return
	}
	Debug(dInfo, "S%v changes state from %s to %s", rf.me, rf.state, newState)
	rf.state = newState

	switch newState {
	case Follower:
		rf.heartbeatTimeout.Stop()
		rf.electionTimeout.Reset(RandomizedElectionTimeout())
	case Candidate:
	case Leader:
		rf.broadcastHeartbeat()
		rf.heartbeatTimeout.Reset(StableHeartbeatTimeout())
		rf.electionTimeout.Stop()
	}
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
}

// used by RequestVote Handler to judge which log is newer
func (rf *Raft) isLogUpToDate(term, index int) bool {
	return true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer Debug(dVote, "S%v state is {state: %v, term: %v}, the RequestVoteReply is {%v}", rf.me, rf.state, rf.currentTerm, reply)

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.changeState(Follower)
	}
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.votedFor = args.CandidateId
	// now the term of the candidate must equal to the current term of the rf
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

/**
 * The following code is about the log replication process and the heartbeat broadcast.
 */
func (rf *Raft) broadcastHeartbeat() {
	Debug(dLeader, "S%v broadcasts heartbeat", rf.me)
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.replicateOneRound(peer)
		}
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	if rf.state != Leader {
		return
	}

	request := rf.genAppendEntriesRequest(peer)
	reply := new(AppendEntriesReply)
	if rf.sendAppendEntries(peer, request, reply) {
		Debug(dLeader, "S%v received AppendEntriesReply {%v} from S%v", rf.me, reply, peer)
	}
}

func (rf *Raft) genAppendEntriesRequest(peer int) *AppendEntriesRequest {
	return &AppendEntriesRequest{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesReply) {
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	Debug(dLog, "S%v received AppendEntriesRequest {%v}", rf.me, args)
	rf.changeState(Follower)
	rf.electionTimeout.Reset(RandomizedElectionTimeout())
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

/**
 * The following code is about the persistent state and snapshots.
 */
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}
