// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// a random election timeout
	// [electionTimeout, 2*electionTimeout - 1]
	randomElectionTimeout int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	pres := make(map[uint64]*Progress)
	raft := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		Prs:              pres,
		RaftLog:          newLog(c.Storage),
	}
	raft.becomeFollower(0, None)
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
}

// follower或者Candidate时，需要超时选举
// ticker为electionTick
func (r *Raft) electionTick() {
	// 选举次数++
	r.electionElapsed++
}

// leader 需要发送心跳
// 的ticker为heartbeat
func (r *Raft) heartbeatTick() {

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// electionTimeout 触发选举
	// 选举人需要将任期先+1
	r.Term++

	r.State = StateCandidate
	r.Lead = None

	// 选举人首先投自己一票
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateLeader {
		// 只有Candidate能成为leader
		// 因此直接将term更新

		r.Lead = r.id
		//更新Peers中的所有日志同步进度
		li := r.RaftLog.LastIndex()
		for i := range r.Prs {
			if i == r.id {
				// leader自己
				r.Prs[i] = &Progress{
					Next:  li + 1,
					Match: li,
				}
			} else {
				// 刚成为leader
				// 所有同步进度都为0
				r.Prs[i] = &Progress{
					Next:  li + 1,
					Match: 0,
				}
			}
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if _, exist := r.Prs[r.id]; !exist && m.MsgType == pb.MessageType_MsgTimeoutNow {
		return nil
	}
	// 同步term
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

/* ====================Step Handlers(follower/Candidate/leader)==================================  */

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	// 触发选举操作
	case pb.MessageType_MsgHup:
		r.handleElection()
	case pb.MessageType_MsgTimeoutNow:
		r.handleElection()

	// 处理投票请求
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)

	// 转发请求到Leader
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}

	// 不需要处理的事件
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgHeartbeatResponse:
	}

	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	// 触发选举操作
	case pb.MessageType_MsgHup:
		r.handleElection()

	// 处理投票请求
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	// 处理投票响应
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	// 不需要处理的事件
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgHeartbeatResponse:
	}

	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	// 不需要处理的事件
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgTimeoutNow:
	case pb.MessageType_MsgRequestVoteResponse:

	// 处理投票事件，到某个节点的消息时延过长，节点自身变为cadidate开始竞选
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)

	// 处理发送心跳事件
	case pb.MessageType_MsgHeartbeat:
		r.broadcastHeartbeat()
	}
	return nil
}

/* ====================Broadcast Events==================================  */

func (r *Raft) broadcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

/* ====================Msg Handlers==================================  */

func (r *Raft) handleVoteRequest(m pb.Message) {
	// Candidate的任期小于我当前任期
	// 直接拒绝
	if m.Term < r.Term {
		r.sendVoteResponse(m.From, true)
		return
	}

	// 当前节点已经参与过Vote
	if r.Vote != None && r.Vote != m.From {
		r.sendVoteResponse(m.From, true)
		return
	}

	lastIndex, lastLogTerm := r.RaftLog.LastIndex(), r.RaftLog.LastTerm()

	// Candidate 的最后一条日志的任期与当前节点的任期不符合
	// Candidate 的最后一条日志的任期与当前节点一致，但是Index不一致
	// 上述任意一种情况都拒绝投票
	if lastLogTerm > m.LogTerm ||
		(lastLogTerm == m.Term && lastIndex > m.Index) {
		r.sendVoteResponse(m.From, true)
		return
	}

	// 进行投票
	r.Vote = m.From
	//同时重置ticker
	r.resetElectionElapsed()
	r.resetElectionTimeout()

	r.sendVoteResponse(m.From, false)
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	// line:281
	// 在处理投票请求时，会同步term，因此投票者与投票发起者的任期应该一致
	if m.Term != None && m.Term < r.Term {
		return
	}

	r.votes[m.From] = !m.Reject
	var agree uint32
	total := len(r.votes)
	half := len(r.Prs) / 2
	for _, g := range r.votes {
		if g {
			agree++
		}
	}
	if agree > uint32(half) {
		r.becomeLeader()
	} else if total-int(agree) > half {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) handleElection() {
	r.becomeCandidate()
	r.heartbeatElapsed = 0
	r.resetHeartbeatElapsed()
	r.resetElectionTimeout()
	// 如果整个集群中只有自己一个节点
	// 那么选举人自己直接成为Leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
		log.Info("node:", r.id, "direct become leader")
		return
	}

	lastIndex := r.RaftLog.LastIndex()
	lastTerm := r.RaftLog.LastTerm()
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendVoteRequest(peer, lastIndex, lastTerm)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

/* ====================Req/Rsp Msg==================================  */

func (r *Raft) sendHeartbeat(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendVoteRequest(to, index, term uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   index,
		LogTerm: term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Reject:  reject,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

/* ====================Ticker handle==================================  */
func (r *Raft) resetElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) resetHeartbeatElapsed() {
	r.heartbeatElapsed = 0
}

func (r *Raft) resetElectionElapsed() {
	r.electionElapsed = 0
}
