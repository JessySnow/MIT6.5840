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

const heartbeatInterval = 100 * time.Millisecond
const selectionTimeout = 350 * time.Millisecond
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term, CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		// 拒绝任期小于自己的服务器的拉票请求
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.currentTerm < args.Term {
		// 服务器任期过期，重置服务器状态
		rf.currentTerm = args.Term
		rf.state = candidate
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else if rf.state != leader && (rf.votedFor == unVoted || rf.votedFor == args.CandidateId) {
		// 任期相同且服务器不是 leader，如果未进行投票或者投给的拉票的服务器，则同意头拉票请求，且将自己转变为候选人
		rf.state = candidate
		rf.selectionTicker = time.Now()
		reply.Term = rf.currentTerm
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

	// 卫语句
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.selectionTicker = time.Now()
		reply.Term = args.Term
		reply.Success = true
		return
	} else if args.Entries == nil || len(args.Entries) == 0 {
		rf.selectionTicker = time.Now()
		reply.Term = args.Term
		reply.Success = true
	}

	// 0. 检查在相同索引 prevLogIndex 上日志的任期是否相同
	prevLogIndex := args.PrevLogIndex
	prevTerm := args.PrevLogTerm
	if prevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex].Term != prevTerm {
		reply.Term = args.Term
		reply.Success = false
		return
	}

	// 1. 检查在相同的索引上，是否存在任期不同的条目，存在冲突即删除冲突日志及之后的日志
	argEntries := args.Entries
	for i := argEntries[0].Index; i < len(rf.log); i++ {
		if rf.log[i].Term != argEntries[i].Term {
			rf.log = rf.log[:i]
			break
		}
	}

	// 2. 向接受者的日志中添加原本不存在的日志项
	lastIndex := rf.log[len(rf.log)-1].Index
	newEntries := make([]LogEntry, 0)
	for i, entry := range args.Entries {
		if entry.Index > lastIndex {
			newEntries = argEntries[i:]
			break
		}
	}
	rf.log = append(rf.log, newEntries...)

	// 3. 更新日志的提交情况
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.log[len(rf.log)-1].Index {
			rf.commitIndex = rf.log[len(rf.log)-1].Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
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

// sendAppendEntries 发送日志复制，心跳
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// copyLogEntries 通过 sendAppendEntries 将日志定期复制给 follower
func (rf *Raft) copyLogEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 定时复制日志的逻辑
		go func(index int) {
			for {
				rf.mu.Lock()

				// 检查是否依然是 leader
				if rf.state != leader {
					return
				}

				startIndex := rf.nextIndex[index]
				entries := make([]LogEntry, 0)
				if startIndex < len(rf.log) {
					entries = rf.log[startIndex:]
				}
				lastIndex := 0
				if entries != nil && len(entries) > 0 {
					lastIndex = entries[len(entries)-1].Index
				}

				// 构造请求和响应
				prevLogIndex := rf.log[startIndex-1].Index
				prevLogTerm := rf.log[startIndex-1].Term
				arg := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me,
					PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
					LeaderCommit: rf.commitIndex, Entries: entries}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()

				// 不断尝试复制，直到成功
				for !rf.sendAppendEntries(index, arg, reply) {
					time.Sleep(1 * time.Millisecond)
				}

				// 根据日志复制结果更新 Raft 状态
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = follower
					rf.selectionTicker = time.Now()
					return
				}

				if reply.Success {
					rf.nextIndex[index] = lastIndex + 1
					rf.matchIndex[index] = lastIndex
				} else {
					rf.nextIndex[index] = rf.nextIndex[index] - 1
				}

				rf.mu.Unlock()

				time.Sleep(heartbeatInterval)
			}
		}(i)
	}
}

// startHeartBeat leader 循环地并发发送请求到所有的接受者
func (rf *Raft) startHeartBeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func() {
			for {
				for !rf.sendAppendEntries(rf.me, &AppendEntriesArgs{Term: term, LeaderId: rf.me}, &AppendEntriesReply{}) {
				}
				time.Sleep(heartbeatInterval)
			}
		}()
	}
}

func (rf *Raft) startElection(currentTerm, serverLength, me int) {
	replyChan := make(chan *RequestVoteReply)
	stopChan := make(chan struct{})

	// 并发拉票
	for i := 0; i < serverLength; i++ {
		if i == me {
			continue
		}

		go func(index int) {
			// 0. 判断计票是否已经结束,快速退出
			select {
			case <-stopChan:
				return
			default:
			}

			// 1. 发起 RPC 请求
			arg := &RequestVoteArgs{Term: currentTerm, CandidateId: me}
			reply := new(RequestVoteReply)
			for !rf.sendRequestVote(index, arg, reply) {
			}

			// 2. 判断计票是否已经结束，未结束将发送结果到计票逻辑中
			select {
			case <-stopChan:
				return
			default:
				replyChan <- reply
			}
		}(i)
	}

	// 计票
	voteCount := 1
	voteTarget := len(rf.peers) / 2
	for reply := range replyChan {
		if reply.Term > currentTerm {
			rf.mu.Lock()
			// 双重检查，防止被其他的协程修改了状态
			// term 过期切换到下一个任期，并修改状态为 follower
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.selectionTicker = time.Now()
			}
			rf.mu.Unlock()
			close(stopChan)
			return
		} else if reply.Term == currentTerm && reply.VoteGranted {
			voteCount += 1
			if voteCount > voteTarget {
				rf.mu.Lock()
				if reply.Term == rf.currentTerm {
					rf.state = leader
					// 成为 leader 后立即发送心跳
					go rf.initLeader()
				}
				rf.mu.Unlock()
				close(stopChan)
				return
			}
		}
	}
}

// initLeader 初始化服务器为 leader
func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 初始化每台服务器下一个要提交的日志索引的切片
	latestIndex := rf.log[len(rf.log)-1].Index
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = latestIndex
	}
	// 初始化每台服务器已知的已经复制的日志索引切片
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.copyLogEntries()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return -1, -1, false
	}

	return index, term, isLeader
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

		// Your code here (3A)
		// Check if a leader election should be started.
		now := time.Now()
		rf.mu.Lock()
		if (rf.state != leader) && now.Sub(rf.selectionTicker) > selectionTimeout {
			rf.currentTerm += 1
			rf.state = candidate
			rf.votedFor = rf.me
			go rf.startElection(rf.currentTerm, len(rf.peers), rf.me)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	//go rf.applyLog()

	return rf
}
