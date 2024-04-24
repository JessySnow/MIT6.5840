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

const (
	follower = iota
	candidate
	leader
)

const copyLogsInterval = 100 * time.Millisecond
const selectionTimeout = 400 * time.Millisecond
const rpcTimeout = 20 * time.Millisecond
const unVoted = -1

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm, votedFor, state int
	selectionTicker              time.Time

	log                      []LogEntry
	commitIndex, lastApplied int

	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.state == leader
	term := rf.currentTerm
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
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
	// Your code here (3C).
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
	// Your code here (3D).

}

type RequestVoteArgs struct {
	Term, CandidateId, LastLogIndex, LastLogTerm int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Command     interface{}
	Index, Term int
}

type AppendEntriesArgs struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm, LeaderCommit int
	Entries                                                 []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = candidate
		rf.votedFor = args.CandidateId
		rf.selectionTicker = time.Now()
		reply.Term = args.Term
		reply.VoteGranted = true
		return
	}

	lastTerm := rf.log[len(rf.log)-1].Term
	lastIndex := rf.log[len(rf.log)-1].Index
	if lastTerm > args.LastLogTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.state != leader && (rf.votedFor == unVoted || rf.votedFor == args.CandidateId) {
		rf.currentTerm = args.Term
		rf.state = candidate
		rf.votedFor = args.CandidateId
		rf.selectionTicker = time.Now()
		reply.Term = args.Term
		reply.VoteGranted = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

// AppendEntries 接受 leader 的日志和心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.selectionTicker = time.Now()
		rf.votedFor = unVoted
		reply.Term = args.Term
		reply.Success = true
	} else if args.Entries == nil || len(args.Entries) == 0 {
		rf.selectionTicker = time.Now()
		reply.Term = args.Term
		reply.Success = true
	}

	//// 0. 检查在相同索引 prevLogIndex 上日志的任期是否相同
	//prevLogIndex := args.PrevLogIndex
	//prevTerm := args.PrevLogTerm
	//if prevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex].Term != prevTerm {
	//	reply.Term = args.Term
	//	reply.Success = false
	//	return
	//}
	//
	//// 如果有附加日志
	//if len(args.Entries) != 0 {
	//	// 1. 检查在相同的索引上，是否存在任期不同的条目，存在冲突即删除冲突日志及之后的日志
	//	argEntries := args.Entries
	//	for _, entry := range argEntries {
	//		index := entry.Index
	//		if index < len(rf.log) && rf.log[index].Term != entry.Term {
	//			rf.log = rf.log[:index]
	//			break
	//		}
	//	}
	//
	//	// 2. 向接受者的日志中添加原本不存在的日志项
	//	lastIndex := rf.log[len(rf.log)-1].Index
	//	newEntries := make([]LogEntry, 0)
	//	for i, entry := range args.Entries {
	//		if entry.Index > lastIndex {
	//			newEntries = argEntries[i:]
	//			break
	//		}
	//	}
	//	rf.log = append(rf.log, newEntries...)
	//}
	//
	//// 3. 同步 leader 的日志的提交情况
	//if args.LeaderCommit > rf.commitIndex {
	//	if args.LeaderCommit > rf.log[len(rf.log)-1].Index {
	//		rf.commitIndex = rf.log[len(rf.log)-1].Index
	//	} else {
	//		rf.commitIndex = args.LeaderCommit
	//	}
	//}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	c := make(chan bool)
	go func() {
		c <- rf.peers[server].Call("Raft.RequestVote", args, reply)
	}()

	select {
	case <-time.Tick(rpcTimeout):
		return false
	case ok := <-c:
		return ok
	}
}

// sendAppendEntries 发送日志
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	c := make(chan bool)
	go func() {
		c <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}()

	select {
	case <-time.Tick(rpcTimeout):
		return false
	case ok := <-c:
		return ok
	}
}

// copyLogEntries 将日志定期复制给 follower
func (rf *Raft) copyLogEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 定时复制日志的逻辑
		go func(index int) {
			for {
				rf.mu.Lock()
				startIndex := min(rf.nextIndex[index], len(rf.log))
				entries := rf.log[startIndex:]
				arg := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me,
					PrevLogIndex: rf.log[startIndex-1].Index, PrevLogTerm: rf.log[startIndex-1].Term,
					LeaderCommit: rf.commitIndex, Entries: entries}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()

				if !rf.sendAppendEntries(index, arg, reply) {
					time.Sleep(copyLogsInterval)
					continue
				}

				// 根据日志复制结果更新 Raft 状态
				rf.mu.Lock()

				if rf.state != leader {
					rf.mu.Unlock()
					break
				}
				if reply.Term < rf.currentTerm {
					rf.mu.Unlock()
					continue
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = follower
					rf.selectionTicker = time.Now()
					rf.mu.Unlock()
					break
				}

				//if reply.Success {
				//	lastIndex := 0
				//	if len(entries) > 0 {
				//		lastIndex = entries[len(entries)-1].Index
				//	}
				//	rf.nextIndex[index] = lastIndex + 1
				//	rf.matchIndex[index] = lastIndex
				//} else {
				//	if rf.nextIndex[index]-1 == 0 {
				//		rf.nextIndex[index] = 1
				//	} else {
				//		rf.nextIndex[index] = rf.nextIndex[index] - 1
				//	}
				//}

				rf.mu.Unlock()

				time.Sleep(copyLogsInterval)
			}
		}(i)
	}
}

func (rf *Raft) startElection(currentTerm, lastLogIndex, lastLogTerm int) {
	replyChan := make(chan *RequestVoteReply)
	stopChan := make(chan struct{})

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(index int) {
			select {
			case <-stopChan:
				return
			default:
			}

			arg := &RequestVoteArgs{Term: currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
			reply := &RequestVoteReply{}
			for !rf.sendRequestVote(index, arg, reply) {
			}

			select {
			case <-stopChan:
				return
			default:
				replyChan <- reply
			}
		}(i)
	}

	voteCount := 1
	voteTarget := int(float64(len(rf.peers))/2.0 + 0.5)
	for reply := range replyChan {
		rf.mu.Lock()

		if rf.state != candidate {
			rf.mu.Unlock()
			close(stopChan)
			break
		}
		if reply.Term > rf.currentTerm {
			rf.votedFor = unVoted
			rf.currentTerm = reply.Term
			rf.state = follower
			rf.selectionTicker = time.Now()

			rf.mu.Unlock()
			close(stopChan)
			break
		}
		if reply.Term == rf.currentTerm && reply.VoteGranted {
			voteCount++
		}

		if voteCount >= voteTarget {
			rf.state = leader
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			go rf.copyLogEntries()

			rf.mu.Unlock()
			close(stopChan)
			break
		}

		rf.mu.Unlock()
	}
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

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return -1, -1, false
	}

	// 新增日志条目
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command, Index: len(rf.log)})

	return len(rf.log), rf.currentTerm, rf.state == leader
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

// ticker 选举计时器，负责检查选举超时状态，并触发选举行为
func (rf *Raft) ticker() {
	for rf.killed() == false {

		rf.mu.Lock()
		now := time.Now()
		if (rf.state != leader) && (now.Sub(rf.selectionTicker) >= selectionTimeout) {
			rf.state = candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.selectionTicker = now
			go rf.startElection(rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
		}
		rf.mu.Unlock()

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = unVoted
	rf.state = follower
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
