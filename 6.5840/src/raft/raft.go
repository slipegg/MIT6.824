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
	"sort"
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
	applyCh          chan ApplyMsg
	logs             []LogEntry
	commitIndex      int
	lastApplied      int
	matchIndex       []int
	nextIndex        []int //Sometimes, there are conflicts between the logs of the leader and the followers. The leader needs adjust the nextIndex and send the logs again.
	applyCond        *sync.Cond
	replicatorCond   []*sync.Cond
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
		applyCh:          applyCh,
		logs:             make([]LogEntry, 1),
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		replicatorCond:   make([]*sync.Cond, len(peers)),
		commitIndex:      0,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLog.Index + 1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections
	if me == 0 {
		Debug(dInfo, "peerNum %v", len(peers))
	}
	go rf.ticker()
	go rf.applier()

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	// Your code here (2B).
	newLog := rf.appendNewLog(command)
	Debug(dLog, "S%v(leader) receives a new log entry {%v}", rf.me, newLog)
	rf.broadcastHeartbeat(false)
	return newLog.Index, newLog.Term, true
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) appendNewLog(command interface{}) LogEntry {
	lastLog := rf.getLastLog()
	newLog := LogEntry{lastLog.Index + 1, rf.currentTerm, command}
	rf.logs = append(rf.logs, newLog)
	rf.matchIndex[rf.me] = newLog.Index
	rf.nextIndex[rf.me] = newLog.Index + 1
	return newLog
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
				rf.broadcastHeartbeat(true)
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
		lastLog := rf.getLastLog()
		for i := range rf.peers {
			rf.nextIndex[i] = lastLog.Index + 1
			rf.matchIndex[i] = 0
		}
		rf.broadcastHeartbeat(true)
		rf.heartbeatTimeout.Reset(StableHeartbeatTimeout())
		rf.electionTimeout.Stop()
	}
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// used by RequestVote Handler to judge which log is newer
func (rf *Raft) isLogUpToDate(term, index int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer Debug(dVote, "S%v state is {state: %v, term: %v}, the RequestVoteReply is {%v}", rf.me, rf.state, rf.currentTerm, reply)

	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
		rf.changeState(Follower)
	}
	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.votedFor = request.CandidateId
	rf.electionTimeout.Reset(RandomizedElectionTimeout())
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
func (rf *Raft) sendRequestVote(server int, request *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", request, reply)
	return ok
}

/**
 * The following code is about the log replication process and the heartbeat broadcast.
 */
func (rf *Raft) broadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer != rf.me {
			if isHeartbeat {
				Debug(dLeader, "S%v broadcasts heartbeat", rf.me)
				go rf.replicateOneRound(peer)
			} else {
				rf.replicatorCond[peer].Signal()
			}
		}
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}

	lastSendIndex := rf.nextIndex[peer] - 1

	request := rf.genAppendEntriesRequest(lastSendIndex)
	rf.mu.RUnlock()
	reply := new(AppendEntriesReply)
	if rf.sendAppendEntries(peer, request, reply) {
		rf.mu.Lock()
		rf.handleAppendEntriesResponse(peer, request, reply)
		rf.mu.Unlock()
		// Debug(dLeader, "S%v received AppendEntriesReply {%v} from S%v", rf.me, reply, peer)
	}
}

// the leader maybe try replicate logs to the follower many times
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) genAppendEntriesRequest(lastSendIndex int) *AppendEntriesRequest {
	entries := make([]LogEntry, len(rf.logs)-1-lastSendIndex)
	copy(entries, rf.logs[lastSendIndex+1:])
	return &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastSendIndex,
		PrevLogTerm:  rf.logs[lastSendIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, reply *AppendEntriesReply) {
	if rf.state != Leader || request.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		if request.Entries != nil && len(request.Entries) > 0 {
			Debug(dLog, "S%v has append log entries(startId: %v, length: %v) to S%v. Now the S%v's nextIndex is %v", rf.me, request.PrevLogIndex+1, len(request.Entries), peer, peer, rf.nextIndex[peer])
		} else {
			Debug(dInfo, "S%v has received heartbeat reply from S%v", rf.me, peer)
		}
		rf.updateCommitIndexForLeader()
	} else {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			Debug(dWarn, "S%v found higher term %v in AppendEntriesReply %v from S%v", rf.me, reply.Term, reply, peer)
			rf.changeState(Follower)
		} else if reply.Term == rf.currentTerm {
			if reply.LastMatchTerm == -1 {
				rf.nextIndex[peer] = reply.LastMatchIndex + 1
				Debug(dLog, "S%v failed to append log entries to S%v, because S%v's log is too short. Now the S%v's nextIndex is %v", rf.me, peer, peer, peer, rf.nextIndex[peer])
			} else {
				for i := request.PrevLogIndex; i > 0; i-- {
					if rf.logs[i].Term == reply.LastMatchTerm {
						rf.nextIndex[peer] = i + 1
						Debug(dLog, "S%v failed to append log entries to S%v, because S%v's log(%v) does not match the leader's log(%v). Now the S%v's nextIndex is %v with the same term S%v's log(%v)", rf.me, peer, peer, reply.LastMatchIndex, i, peer, rf.nextIndex[peer], peer, rf.logs[rf.nextIndex[peer]].Term)
						break
					}
				}
			}
		}
	}
}

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if request.Entries != nil && len(request.Entries) > 0 {
		Debug(dLog, "S%v received AppendEntriesRequest {%v}", rf.me, request)
	} else {
		Debug(dInfo, "S%v received heartbeat from S%v", rf.me, request.LeaderId)
	}
	if request.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	rf.changeState(Follower)
	rf.electionTimeout.Reset(RandomizedElectionTimeout())

	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex { // the follower's log is too short
			reply.LastMatchIndex, reply.LastMatchTerm = lastIndex, -1
		} else {
			// the follower's log with PrevLogIndex is not match the term,
			// it's term is smaller than the term of leader's log with PrevLogIndex.
			// Next time, we need to send the log entries from the leader's log with follower's smaller term.
			reply.LastMatchIndex, reply.LastMatchTerm = -1, rf.logs[request.PrevLogIndex].Term
		}

		return
	}
	for index, entry := range request.Entries {
		// May be there are some log entries already exist in the follower's log. We don't need to copy it again.
		if entry.Index >= len(rf.logs) || rf.logs[entry.Index].Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index], request.Entries[index:]...)
			break
		}
	}

	rf.updateCommitIndexForFollower(request.LeaderCommit)

	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) matchLog(term, index int) bool {
	return index <= rf.getLastLog().Index && rf.logs[index].Term == term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateCommitIndexForLeader() {
	n := len(rf.matchIndex)
	srt := make([]int, n)
	copy(srt, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(srt)))
	newCommitIndex := srt[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		// Only when the log of the leader's current term is accepted by the majority of people can it be submitted.
		// You cannot submit the logs of other terms separately in advance.
		if rf.matchLog(rf.currentTerm, newCommitIndex) {
			Debug(dLog, "S%v(leader) updates commitIndex from %v to %v", rf.me, rf.commitIndex, newCommitIndex)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		} else {
			Debug(dWarn, "S%v cannot commit log%v at index %v because it does not match the current term %v, rf.commitIndex: %v, srt: %v", rf.me, rf.logs[newCommitIndex], newCommitIndex, rf.currentTerm, rf.commitIndex, srt)
		}
	}

}

func (rf *Raft) updateCommitIndexForFollower(leaderCommit int) {
	newCommitIndex := Min(leaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
		Debug(dLog, "S%v updates commitIndex to %v", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1:commitIndex+1])
		Debug(dTest, "S%v commitIndex: %v, lastApplied: %v, entries: %v", rf.me, commitIndex, lastApplied, entries)
		rf.mu.Unlock()

		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}

		rf.mu.Lock()
		Debug(dCommit, "S%v applies log entries(startId: %v, length: %v)", rf.me, lastApplied+1, len(entries))
		rf.lastApplied = commitIndex
		rf.mu.Unlock()
	}
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
