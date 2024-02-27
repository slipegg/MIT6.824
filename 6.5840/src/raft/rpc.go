package raft

import "fmt"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (request RequestVoteArgs) String() string {
	return fmt.Sprintf("{term: %d, candidateId: %d, lastLogIndex: %d, lastLogTerm: %d}", request.Term, request.CandidateId, request.LastLogIndex, request.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("term: %d, voteGranted: %t", reply.Term, reply.VoteGranted)
}

type AppendEntriesRequest struct {
	Term     int
	LeaderId int
}

func (request AppendEntriesRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v", request.Term, request.LeaderId)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (reply AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v", reply.Term, reply.Success)
}
