package goPBFT

import (
	"errors"
	"fmt"
	pb "github.com/zballs/goPBFT/types"
	"log"
	"net"
)

const (
	CHECKPOINT_PERIOD uint64 = 128
	CONSTANT_FACTOR   uint64 = 2
)

var Fmt = fmt.Sprintf

type Replica struct {
	net.Listener

	ID       uint64
	replicas map[uint64]string

	activeView bool
	view       uint64
	sequence   uint64

	requestChan chan *pb.Request
	replyChan   chan *pb.Reply
	errChan     chan error

	requests  map[string][]*pb.Request
	replies   map[string][]*pb.Reply
	lastReply *pb.Reply

	// prep    []*pb.Entry
	// prePrep []*pb.Entry

	pendingVC []*pb.Request

	executed    []uint64
	checkpoints []*pb.Checkpoint
}

// Basic operations

func (rep *Replica) primary() uint64 {
	return rep.view % uint64(len(rep.replicas)+1)
}

func (rep *Replica) newPrimary(view uint64) uint64 {
	return view % uint64(len(rep.replicas)+1)
}

func (rep *Replica) isPrimary(ID uint64) bool {
	return ID == rep.primary()
}

func (rep *Replica) oneThird(count int) bool {
	return count >= (len(rep.replicas)+1)/3
}

func (rep *Replica) overOneThird(count int) bool {
	return count > (len(rep.replicas)+1)/3
}

func (rep *Replica) twoThirds(count int) bool {
	return count >= 2*(len(rep.replicas)+1)/3
}

func (rep *Replica) overTwoThirds(count int) bool {
	return count > 2*(len(rep.replicas)+1)/3
}

func (rep *Replica) lowWaterMark() uint64 {
	return rep.lastStable().Sequence
}

func (rep *Replica) highWaterMark() uint64 {
	return rep.lowWaterMark() + CHECKPOINT_PERIOD*CONSTANT_FACTOR
}

func (rep *Replica) sequenceInRange(sequence uint64) bool {
	return sequence > rep.lowWaterMark() && sequence <= rep.highWaterMark()
}

func (rep *Replica) lastExecuted() uint64 {
	return rep.executed[len(rep.executed)-1]
}

func (rep *Replica) lastStable() *pb.Checkpoint {
	return rep.checkpoints[len(rep.checkpoints)-1]
}

func (rep *Replica) theLastReply() *pb.Reply {
	lastReply := rep.lastReply
	for _, replies := range rep.replies {
		reply := replies[len(replies)-1]
		if reply.Timestamp > lastReply.Timestamp {
			lastReply = reply
		}
	}
	return lastReply
}

func (rep *Replica) lastReplyToClient(client string) *pb.Reply {
	return rep.replies[client][len(rep.replies[client])-1]
}

func (rep *Replica) stateDigest() []byte {
	return rep.theLastReply().Digest()
}

func (rep *Replica) isCheckpoint(sequence uint64) bool {
	return sequence%CHECKPOINT_PERIOD == 0
}

func (rep *Replica) addCheckpoint(checkpoint *pb.Checkpoint) {
	rep.checkpoints = append(rep.checkpoints, checkpoint)
}

func (rep *Replica) acceptConnections() {
	for {
		conn, err := rep.Accept()
		if err != nil {
			log.Panic(err)
		}
		req := &pb.Request{}
		err = pb.ReadMessage(conn, req)
		if err != nil {
			log.Panic(err)
		}
		rep.handleRequest(req)
	}
}

// Sends

func (rep *Replica) multicast(REQ *pb.Request) error {
	for _, replica := range rep.replicas {
		err := pb.WriteMessage(replica, REQ)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rep *Replica) sendRoutine() {
	for {
		select {
		case REQ := <-rep.requestChan:
			switch REQ.Value.(type) {
			case *pb.Request_Ack:
				view := REQ.GetAck().View
				primaryID := rep.newPrimary(view)
				primary := rep.replicas[primaryID]
				err := pb.WriteMessage(primary, REQ)
				if err != nil {
					go func() {
						rep.errChan <- err
					}()
				}
			default:
				err := rep.multicast(REQ)
				if err != nil {
					go func() {
						rep.errChan <- err
					}()
				}
			}
		case reply := <-rep.replyChan:
			client := reply.Client
			err := pb.WriteMessage(client, reply)
			if err != nil {
				go func() {
					rep.errChan <- err
				}()
			}
		}
	}
}

// Log
// !hasRequest before append?

func (rep *Replica) logRequest(REQ *pb.Request) {
	switch REQ.Value.(type) {
	case *pb.Request_Client:
		rep.requests["client"] = append(rep.requests["client"], REQ)
	case *pb.Request_Preprepare:
		rep.requests["pre-prepare"] = append(rep.requests["pre-prepare"], REQ)
	case *pb.Request_Prepare:
		rep.requests["prepare"] = append(rep.requests["prepare"], REQ)
	case *pb.Request_Commit:
		rep.requests["commit"] = append(rep.requests["commit"], REQ)
	case *pb.Request_Checkpoint:
		rep.requests["checkpoint"] = append(rep.requests["checkpoint"], REQ)
	case *pb.Request_Viewchange:
		rep.requests["view-change"] = append(rep.requests["view-change"], REQ)
	case *pb.Request_Ack:
		rep.requests["ack"] = append(rep.requests["ack"], REQ)
	case *pb.Request_Newview:
		rep.requests["new-view"] = append(rep.requests["new-view"], REQ)
	default:
		log.Printf("Replica %d tried logging unrecognized request type\n", rep.ID)
	}
}

func (rep *Replica) logPendingVC(REQ *pb.Request) error {
	switch REQ.Value.(type) {
	case *pb.Request_Viewchange:
		rep.pendingVC = append(rep.pendingVC, REQ)
		return nil
	default:
		return errors.New("Request is wrong type")
	}
}

func (rep *Replica) logReply(client string, reply *pb.Reply) {
	lastReplyToClient := rep.lastReplyToClient(client)
	if lastReplyToClient.Timestamp < reply.Timestamp {
		rep.replies[client] = append(rep.replies[client], reply)
	}
}

/*
func (rep *Replica) logPrep(prep *pb.RequestPrepare)
	rep.prep = append(rep.prep, prep)
}

func (rep *Replica) logPrePrep(prePrep *pb.RequestPrePrepare) {
	rep.prePrep = append(rep.prePrep, prePrep)
}
*/

// Has requests

func (rep *Replica) hasRequest(REQ *pb.Request) bool {

	switch REQ.Value.(type) {
	case *pb.Request_Preprepare:
		return rep.hasRequestPreprepare(REQ)
	case *pb.Request_Prepare:
		return rep.hasRequestPrepare(REQ)
	case *pb.Request_Commit:
		return rep.hasRequestCommit(REQ)
	case *pb.Request_Viewchange:
		return rep.hasRequestViewChange(REQ)
	case *pb.Request_Ack:
		return rep.hasRequestAck(REQ)
	case *pb.Request_Newview:
		return rep.hasRequestNewView(REQ)
	default:
		return false
	}
}

func (rep *Replica) hasRequestPreprepare(REQ *pb.Request) bool {
	view := REQ.GetPreprepare().View
	sequence := REQ.GetPreprepare().Sequence
	digest := REQ.GetPreprepare().Digest
	for _, req := range rep.requests["pre-prepare"] {
		v := req.GetPreprepare().View
		s := req.GetPreprepare().Sequence
		d := req.GetPreprepare().Digest
		if v == view && s == sequence && pb.EQ(d, digest) {
			return true
		}
	}
	return false
}

func (rep *Replica) hasRequestPrepare(REQ *pb.Request) bool {
	view := REQ.GetPrepare().View
	sequence := REQ.GetPrepare().Sequence
	digest := REQ.GetPrepare().Digest
	replica := REQ.GetPrepare().Replica
	for _, req := range rep.requests["prepare"] {
		v := req.GetPrepare().View
		s := req.GetPrepare().Sequence
		d := req.GetPrepare().Digest
		r := req.GetPrepare().Replica
		if v == view && s == sequence && pb.EQ(d, digest) && r == replica {
			return true
		}
	}
	return false
}

func (rep *Replica) hasRequestCommit(REQ *pb.Request) bool {
	view := REQ.GetCommit().View
	sequence := REQ.GetCommit().Sequence
	replica := REQ.GetCommit().Replica
	for _, req := range rep.requests["commit"] {
		v := req.GetCommit().View
		s := req.GetCommit().Sequence
		r := req.GetCommit().Replica
		if v == view && s == sequence && r == replica {
			return true
		}
	}
	return false
}

func (rep *Replica) hasRequestViewChange(REQ *pb.Request) bool {
	view := REQ.GetViewchange().View
	replica := REQ.GetViewchange().Replica
	for _, req := range rep.requests["view-change"] {
		v := req.GetViewchange().View
		r := req.GetViewchange().Replica
		if v == view && r == replica {
			return true
		}
	}
	return false
}

func (rep *Replica) hasRequestAck(REQ *pb.Request) bool {
	view := REQ.GetAck().View
	replica := REQ.GetAck().Replica
	viewchanger := REQ.GetAck().Viewchanger
	for _, req := range rep.requests["ack"] {
		v := req.GetAck().View
		r := req.GetAck().Replica
		vc := req.GetAck().Viewchanger
		if v == view && r == replica && vc == viewchanger {
			return true
		}
	}
	return false
}

func (rep *Replica) hasRequestNewView(REQ *pb.Request) bool {
	view := REQ.GetNewview().View
	for _, req := range rep.requests["new-view"] {
		v := req.GetNewview().View
		if v == view {
			return true
		}
	}
	return false
}

/*
func (rep *Replica) clearEntries(sequence uint64) {
	go rep.clearPrepared(sequence)
	go rep.clearPreprepared(sequence)
}

func (rep *Replica) clearPrepared(sequence uint64) {
	prepared := rep.prepared
	for idx, entry := range rep.prepared {
		s := entry.sequence
		if s <= sequence {
			prepared = append(prepared[:idx], prepared[idx+1:]...)
		}
	}
	rep.prepared = prepared
}

func (rep *Replica) clearPreprepared(sequence uint64) {
	prePrepared := rep.prePrepared
	for idx, entry := range rep.prePrepared {
		s := entry.sequence
		if s <= sequence {
			prePrepared = append(prePrepared[:idx], prePrepared[idx+1:]...)
		}
	}
	rep.prePrepared = prePrepared
}
*/

// Clear requests

func (rep *Replica) clearRequests(sequence uint64) {
	rep.clearRequestClients()
	rep.clearRequestPreprepares(sequence)
	rep.clearRequestPrepares(sequence)
	rep.clearRequestCommits(sequence)
	rep.clearRequestCheckpoints(sequence)
}

func (rep *Replica) clearRequestClients() {
	clientReqs := rep.requests["client"]
	lastTimestamp := rep.theLastReply().Timestamp
	for idx, req := range rep.requests["client"] {
		timestamp := req.GetClient().Timestamp
		if lastTimestamp >= timestamp {
			clientReqs = append(clientReqs[:idx], clientReqs[idx+1:]...)
		}
	}
	rep.requests["client"] = clientReqs
}

func (rep *Replica) clearRequestPreprepares(sequence uint64) {
	prePrepares := rep.requests["pre-prepare"]
	for idx, req := range rep.requests["pre-prepare"] {
		s := req.GetPreprepare().Sequence
		if s <= sequence {
			prePrepares = append(prePrepares[:idx], prePrepares[idx+1:]...)
		}
	}
	rep.requests["pre-prepare"] = prePrepares
}

func (rep *Replica) clearRequestPrepares(sequence uint64) {
	prepares := rep.requests["prepare"]
	for idx, req := range rep.requests["prepare"] {
		s := req.GetPrepare().Sequence
		if s <= sequence {
			prepares = append(prepares[:idx], prepares[idx+1:]...)
		}
	}
	rep.requests["prepare"] = prepares
}

func (rep *Replica) clearRequestCommits(sequence uint64) {
	commits := rep.requests["commit"]
	for idx, req := range rep.requests["commit"] {
		s := req.GetCommit().Sequence
		if s <= sequence {
			commits = append(commits[:idx], commits[idx+1:]...)
		}
	}
	rep.requests["commit"] = commits
}

func (rep *Replica) clearRequestCheckpoints(sequence uint64) {
	checkpoints := rep.requests["checkpoint"]
	for idx, req := range rep.requests["checkpoint"] {
		s := req.GetCheckpoint().Sequence
		if s <= sequence {
			checkpoints = append(checkpoints[:idx], checkpoints[idx+1:]...)
		}
	}
	rep.requests["checkpoint"] = checkpoints
}

// Handle requests

func (rep *Replica) handleRequest(REQ *pb.Request) {

	switch REQ.Value.(type) {
	case *pb.Request_Client:

		rep.handleRequestClient(REQ)

	case *pb.Request_Preprepare:

		rep.handleRequestPreprepare(REQ)

	case *pb.Request_Prepare:

		rep.handleRequestPrepare(REQ)

	case *pb.Request_Commit:

		rep.handleRequestCommit(REQ)

	case *pb.Request_Checkpoint:

		rep.handleRequestCheckpoint(REQ)

	case *pb.Request_Viewchange:

		rep.handleRequestViewChange(REQ)

	case *pb.Request_Ack:

		rep.handleRequestAck(REQ)

	default:
		fmt.Printf("Replica %d received unrecognized request type\n", rep.ID)
	}
}

func (rep *Replica) handleRequestClient(REQ *pb.Request) {

	client := REQ.GetClient().Client
	timestamp := REQ.GetClient().Timestamp
	lastReplyToClient := rep.lastReplyToClient(client)

	if lastReplyToClient.Timestamp == timestamp {
		reply := pb.ToReply(rep.view, timestamp, client, rep.ID, lastReplyToClient.Result)
		rep.logReply(client, reply)
		go func() {
			rep.replyChan <- reply
		}()
		return
	}

	rep.logRequest(REQ)

	if !rep.isPrimary(rep.ID) {
		return
	}

	req := pb.ToRequestPreprepare(rep.view, rep.sequence, REQ.Digest(), rep.ID)
	rep.logRequest(req)

	// prePrep := req.GetPreprepare()
	// rep.logPrePrep(prePrep)

	go func() {
		rep.requestChan <- req
	}()

	/*
		err := rep.multicast(prePrepare)
		if err != nil {
			return err
		}
	*/
}

func (rep *Replica) handleRequestPreprepare(REQ *pb.Request) {

	replica := REQ.GetPreprepare().Replica

	if !rep.isPrimary(replica) {
		return
	}

	view := REQ.GetPreprepare().View

	if rep.view != view {
		return
	}

	sequence := REQ.GetPreprepare().Sequence

	if !rep.sequenceInRange(sequence) {
		return
	}

	digest := REQ.GetPreprepare().Digest

	accept := true
	for _, req := range rep.requests["pre-prepare"] {
		v := req.GetPreprepare().View
		s := req.GetPreprepare().Sequence
		d := req.GetPreprepare().Digest

		if v == view && s == sequence && !pb.EQ(d, digest) {
			accept = false
			break
		}
	}

	if !accept {
		return
	}

	rep.logRequest(REQ)

	/*
		if rep.isPrimary() {
			return nil
		}
	*/

	req := pb.ToRequestPrepare(view, sequence, digest, rep.ID)

	if rep.hasRequest(req) {
		return
	}

	rep.logRequest(req)

	go func() {
		rep.requestChan <- req
	}()

	// prePrep := REQ.GetPreprepare()
	// rep.logPrePrep(prePrep)
}

func (rep *Replica) handleRequestPrepare(REQ *pb.Request) {

	replica := REQ.GetPrepare().Replica

	if rep.isPrimary(replica) {
		return
	}

	view := REQ.GetPrepare().View

	if rep.view != view {
		return
	}

	sequence := REQ.GetPrepare().Sequence

	if !rep.sequenceInRange(sequence) {
		return
	}

	digest := REQ.GetPrepare().Digest

	rep.logRequest(REQ)

	prePrepared := make(chan bool, 1)
	twoThirds := make(chan bool, 1)

	go func() {
		prePrepared <- rep.hasRequest(REQ)
	}()

	go func() {
		count := 0
		for _, req := range rep.requests["prepare"] {
			v := req.GetPrepare().View
			s := req.GetPrepare().Sequence
			d := req.GetPrepare().Digest
			r := req.GetPrepare().Replica
			if v != view || s != sequence || !pb.EQ(d, digest) {
				continue
			}
			if r == replica {
				fmt.Printf("Replica %d sent multiple prepare requests\n", replica)
				continue
			}
			count++
			if rep.twoThirds(count) {
				twoThirds <- true
				return
			}
		}
		twoThirds <- false
	}()

	if !<-twoThirds || !<-prePrepared {
		return
	}

	req := pb.ToRequestCommit(view, sequence, rep.ID)
	if rep.hasRequest(req) {
		return
	}

	rep.logRequest(req)
	go func() {
		rep.requestChan <- req
	}()
}

func (rep *Replica) handleRequestCommit(REQ *pb.Request) {

	view := REQ.GetCommit().View

	if rep.view != view {
		return
	}

	sequence := REQ.GetCommit().Sequence

	if !rep.sequenceInRange(sequence) {
		return
	}

	replica := REQ.GetCommit().Replica

	rep.logRequest(REQ)

	count := 0
	for _, req := range rep.requests["commit"] {
		v := req.GetCommit().View
		s := req.GetCommit().Sequence
		rr := req.GetCommit().Replica
		if v != view || s != sequence {
			continue
		}
		if rr == replica {
			fmt.Printf("Replica %d sent multiple commit requests\n", replica)
			continue
		}
		count++
		if !rep.overTwoThirds(count) {
			continue
		}
		var digest []byte
		digests := make(map[string]struct{})
		for _, req := range rep.requests["pre-prepare"] {
			v := req.GetPreprepare().View
			s := req.GetPreprepare().Sequence
			if v == view && s <= sequence {
				digests[string(req.Digest())] = struct{}{}
				if s == sequence {
					digest = req.Digest()
					break
				}
			}
		}
		for _, req := range rep.requests["client"] {
			d := req.Digest()
			if _, exists := digests[string(d)]; !exists {
				continue
			}
			if !pb.EQ(d, digest) {
				// Unexecuted message op with
				// lower sequence number...
				break
			}

			// op := req.GetClient().Op
			timestamp := req.GetClient().Timestamp
			client := req.GetClient().Client
			result := &pb.Result{"OK"}

			rep.executed = append(rep.executed, sequence)
			reply := pb.ToReply(view, timestamp, client, rep.ID, result)

			rep.logReply(client, reply)
			rep.lastReply = reply

			go func() {
				rep.replyChan <- reply
			}()

			if !rep.isCheckpoint(sequence) {
				return
			}
			stateDigest := rep.stateDigest()
			req := pb.ToRequestCheckpoint(sequence, stateDigest, rep.ID)
			rep.logRequest(req)
			checkpoint := pb.ToCheckpoint(sequence, stateDigest)
			rep.addCheckpoint(checkpoint)
			go func() {
				rep.requestChan <- req
			}()
			return
		}
	}
	return
}

func (rep *Replica) handleRequestCheckpoint(REQ *pb.Request) {

	sequence := REQ.GetCheckpoint().Sequence

	if !rep.sequenceInRange(sequence) {
		return
	}

	digest := REQ.GetCheckpoint().Digest
	replica := REQ.GetCheckpoint().Replica

	count := 0
	for _, req := range rep.requests["checkpoint"] {
		s := req.GetCheckpoint().Sequence
		d := req.GetCheckpoint().Digest
		r := req.GetCheckpoint().Replica
		if s != sequence || !pb.EQ(d, digest) {
			continue
		}
		if r == replica {
			fmt.Printf("Replica %d sent multiple checkpoint requests\n", replica)
			continue
		}
		count++
		if !rep.overTwoThirds(count) {
			continue
		}
		// rep.clearEntries(sequence)
		rep.clearRequests(sequence)
		return
	}
	return
}

func (rep *Replica) handleRequestViewChange(REQ *pb.Request) {

	view := REQ.GetViewchange().View

	if view < rep.view {
		return
	}

	reqViewChange := REQ.GetViewchange()

	for _, prep := range reqViewChange.GetPreps() {
		v := prep.View
		s := prep.Sequence
		if v >= view || !rep.sequenceInRange(s) {
			return
		}
	}

	for _, prePrep := range reqViewChange.GetPrepreps() {
		v := prePrep.View
		s := prePrep.Sequence
		if v >= view || !rep.sequenceInRange(s) {
			return
		}
	}

	for _, checkpoint := range reqViewChange.GetCheckpoints() {
		s := checkpoint.Sequence
		if !rep.sequenceInRange(s) {
			return
		}
	}

	if rep.hasRequest(REQ) {
		return
	}

	rep.logRequest(REQ)

	viewchanger := reqViewChange.Replica

	req := pb.ToRequestAck(
		view,
		rep.ID,
		viewchanger,
		REQ.Digest())

	go func() {
		rep.requestChan <- req
	}()
}

func (rep *Replica) handleRequestAck(REQ *pb.Request) {

	view := REQ.GetAck().View
	primaryID := rep.newPrimary(view)

	if rep.ID != primaryID {
		return
	}

	if rep.hasRequest(REQ) {
		return
	}

	rep.logRequest(REQ)

	replica := REQ.GetAck().Replica
	viewchanger := REQ.GetAck().Viewchanger
	digest := REQ.GetAck().Digest

	reqViewChange := make(chan *pb.Request, 1)
	twoThirds := make(chan bool, 1)

	go func() {
		for _, req := range rep.requests["view-change"] {
			v := req.GetViewchange().View
			vc := req.GetViewchange().Replica
			if v == view && vc == viewchanger {
				reqViewChange <- req
			}
		}
		reqViewChange <- nil
	}()

	go func() {
		count := 0
		for _, req := range rep.requests["ack"] {
			v := req.GetAck().View
			r := req.GetAck().Replica
			vc := req.GetAck().Viewchanger
			d := req.GetAck().Digest
			if v != view || vc != viewchanger || !pb.EQ(d, digest) {
				continue
			}
			if r == replica {
				fmt.Printf("Replica %d sent multiple ack requests\n", replica)
				continue
			}
			count++
			if rep.twoThirds(count) {
				twoThirds <- true
				return
			}
		}
		twoThirds <- false
	}()

	req := <-reqViewChange

	if req == nil || !<-twoThirds {
		return
	}

	rep.logPendingVC(req)

	// When to send new view?
	rep.requestNewView(view)
}

func (rep *Replica) handleRequestNewView(REQ *pb.Request) {

	view := REQ.GetNewview().View

	if view == 0 || view < rep.view {
		return
	}

	replica := REQ.GetNewview().Replica
	primary := rep.newPrimary(view)

	if replica != primary || rep.ID == primary {
		return
	}

	if rep.hasRequest(REQ) {
		return
	}

	rep.logRequest(REQ)
	rep.processNewView(REQ)
}

func (rep *Replica) processNewView(REQ *pb.Request) {

	if rep.activeView {
		return
	}

	var h uint64
	for _, checkpoint := range rep.checkpoints {
		if checkpoint.Sequence < h || h == uint64(0) {
			h = checkpoint.Sequence
		}
	}

	reqNewView := REQ.GetNewview()

	var start uint64
	var DIGEST []byte
	digests := make(map[uint64][]byte)

	for _, summary := range reqNewView.GetSummaries() {
		s := summary.Sequence
		d := summary.Digest
		if _d, ok := digests[s]; ok && !pb.EQ(_d, d) {
			return
		} else if !ok {
			digests[s] = d
		}
		if h < s {
			match := false
			for _, req := range rep.requests["view-change"] {
				if pb.EQ(req.Digest(), d) {
					match = true
					break
				}
			}
			if !match {
				return
			}
		}
		if s < start || start == uint64(0) {
			start = s
			DIGEST = d
		}
	}

	if h < start {
		return
	}

	var S1 []*pb.Request
	var S2 []*pb.Request

	view := rep.view //??

	for _, vc := range reqNewView.GetViewchanges() {
		digest := vc.Digest
		for _, req := range rep.requests["view-change"] {
			reqViewChange := req.GetViewchange()
			v := reqViewChange.View
			s := reqViewChange.Sequence
			d := req.Digest()
			if v == view && pb.EQ(d, digest) {
				if s <= start {
					S1 = append(S1, req)
				}
				for _, checkpoint := range reqViewChange.GetCheckpoints() {
					if checkpoint.Sequence == h && pb.EQ(checkpoint.Digest, DIGEST) {
						S2 = append(S2, req)
						break
					}
				}
				// break
			}
		}
	}

	if !rep.twoThirds(len(S1)) || !rep.oneThird(len(S2)) {
		return
	}

	end := start + CHECKPOINT_PERIOD*CHECKPOINT_PERIOD

	var correct bool

	for seq := start; seq <= end; seq++ {

		correct = false

		for _, summary := range reqNewView.GetSummaries() {

			sequence := summary.Sequence

			if sequence != seq {
				continue
			}

			if summary.Digest != nil {

				var view uint64

				for _, req := range rep.requests["view-change"] {
					reqViewChange := req.GetViewchange()
					for _, prep := range reqViewChange.GetPreps() {
						s := prep.Sequence
						d := prep.Digest
						if s != sequence || !pb.EQ(d, summary.Digest) {
							continue
						}
						v := prep.View
						if v > view {
							view = v
						}
					}
				}

				verifiedA1 := make(chan bool, 1)

				// Verify A1
				go func() {

					var A1 []*pb.Request

				FOR_LOOP:
					for _, req := range rep.requests["view-change"] {
						reqViewChange := req.GetViewchange()
						h := req.LowWaterMark()
						if h >= sequence {
							continue
						}
						for _, prep := range reqViewChange.GetPreps() {
							s := prep.Sequence
							if s != sequence {
								continue
							}
							d := prep.Digest
							v := prep.View
							if v > view || (v == view && !pb.EQ(d, summary.Digest)) {
								continue FOR_LOOP
							}
						}
						A1 = append(A1, req)
						if rep.twoThirds(len(A1)) {
							verifiedA1 <- true
							return
						}
					}
					verifiedA1 <- false
				}()

				verifiedA2 := make(chan bool, 1)

				// Verify A2
				go func() {
					var A2 []*pb.Request
					for _, req := range rep.requests["view-change"] {
						reqViewChange := req.GetViewchange()
						for _, prePrep := range reqViewChange.GetPrepreps() {
							s := prePrep.Sequence
							d := prePrep.Digest
							v := prePrep.View
							if s == sequence && pb.EQ(d, summary.Digest) && v >= view {
								A2 = append(A2, req)
								break
							}
						}
						if rep.oneThird(len(A2)) {
							verifiedA2 <- true
							return
						}
					}
					verifiedA2 <- false
				}()

				if !<-verifiedA1 || !<-verifiedA2 {
					continue
				}

				correct = true
				break

			} else {

				var A1 []*pb.Request

			FOR_LOOP:

				for _, req := range rep.requests["view-change"] {

					reqViewChange := req.GetViewchange()

					s := reqViewChange.Sequence

					if s >= sequence {
						continue
					}

					for _, prep := range reqViewChange.GetPreps() {
						if prep.Sequence == sequence {
							continue FOR_LOOP
						}
					}

					A1 = append(A1, req)
				}

				if rep.twoThirds(len(A1)) {
					correct = true
					break
				}
			}
		}

		if !correct {
			return
		}
	}

	// Process new view
	rep.activeView = true

	for _, summary := range reqNewView.GetSummaries() {

		req := pb.ToRequestPrepare(
			rep.view,
			summary.Sequence,
			summary.Digest,
			rep.ID) // the backup sends/logs prepare

		go func() {
			if !rep.hasRequest(req) {
				rep.requestChan <- req
			}
		}()

		if summary.Sequence <= h {
			continue
		}

		if !rep.hasRequest(req) {
			rep.logRequest(req)
		}

		req = pb.ToRequestPreprepare(
			rep.view,
			summary.Sequence,
			summary.Digest,
			reqNewView.Replica) // new primary pre-prepares

		if !rep.hasRequest(req) {
			rep.logRequest(req)
		}
	}

	var maxSequence uint64
	for _, req := range rep.requests["pre-prepare"] {
		reqPrePrepare := req.GetPreprepare()
		if reqPrePrepare.Sequence > maxSequence {
			maxSequence = reqPrePrepare.Sequence
		}
	}
	rep.sequence = maxSequence
}

func (rep *Replica) prepBySequence(sequence uint64) *pb.Entry {
	var REQ *pb.Request
	var view uint64
	// Looking for a commit that
	// replica sent with sequence#
	for _, req := range rep.requests["commit"] {
		v := req.GetCommit().View
		s := req.GetCommit().Sequence
		r := req.GetCommit().Replica
		if v >= view && s == sequence && r == rep.ID {
			REQ = req
			view = v
		}
	}
	if REQ == nil {
		return nil
	}
	entry := pb.ToEntry(sequence, nil, view)
	return entry
}

func (rep *Replica) prePrepBySequence(sequence uint64) *pb.Entry {
	var REQ *pb.Request
	var view uint64
	// Looking for prepare/pre-prepare that
	// replica sent with matching sequence
	for _, req := range rep.requests["pre-prepare"] {
		v := req.GetPreprepare().View
		s := req.GetPreprepare().Sequence
		r := req.GetPreprepare().Replica
		if v >= view && s == sequence && r == rep.ID {
			REQ = req
			view = v
		}
	}
	for _, req := range rep.requests["prepare"] {
		v := req.GetPreprepare().View
		s := req.GetPreprepare().Sequence
		r := req.GetPreprepare().Replica
		if v >= view && s == sequence && r == rep.ID {
			REQ = req
			view = v
		}
	}
	if REQ == nil {
		return nil
	}
	var digest []byte
	switch REQ.Value.(type) {
	case *pb.Request_Preprepare:
		digest = REQ.GetPreprepare().Digest
	case *pb.Request_Prepare:
		digest = REQ.GetPrepare().Digest
	}
	entry := pb.ToEntry(sequence, digest, view)
	return entry
}

func (rep *Replica) requestViewChange() {

	rep.view += 1
	rep.activeView = false

	var preps []*pb.Entry
	var prePreps []*pb.Entry

	start := rep.lowWaterMark() + 1
	end := rep.highWaterMark()

	for s := start; s <= end; s++ {
		entry := rep.prepBySequence(s)
		if entry != nil {
			preps = append(preps, entry)
		}
		entry = rep.prePrepBySequence(s)
		if entry != nil {
			prePreps = append(prePreps, entry)
		}
	}

	sequence := rep.lowWaterMark()

	req := pb.ToRequestViewChange(
		rep.view,
		sequence,
		rep.checkpoints,
		preps,
		prePreps,
		rep.ID)

	rep.logRequest(req)

	go func() {
		rep.requestChan <- req
	}()

	rep.clearRequestPreprepares(sequence)
	rep.clearRequestPrepares(sequence)
	rep.clearRequestCommits(sequence)
}

func (rep *Replica) requestNewView(VIEW uint64) {

	viewChanges := make([]*pb.ViewChange, len(rep.pendingVC))

	for idx, _ := range viewChanges {
		req := rep.pendingVC[idx]
		viewchanger := req.GetViewchange().Replica
		vc := pb.ToViewChange(viewchanger, req.Digest())
		viewChanges[idx] = vc
	}

	// select starting checkpoint
	start := rep.lowWaterMark() + 1
	end := rep.highWaterMark()

	var CHECKPOINTS []*pb.Checkpoint

	for sequence := start; sequence <= end; sequence++ {

		overLWM := 0
		var checkpoints []*pb.Checkpoint

		for _, req := range rep.pendingVC {
			h := req.LowWaterMark()
			if h <= sequence {
				overLWM++
			}
			reqViewChange := req.GetViewchange()
			for _, checkpoint := range reqViewChange.GetCheckpoints() {
				s := checkpoint.Sequence
				if s == sequence {
					checkpoints = append(checkpoints, checkpoint)
					break
				}
			}
		}
		if !rep.twoThirds(overLWM) {
			continue
		}
		digests := make(map[string]int)
		for _, checkpoint := range checkpoints {
			d := string(checkpoint.Digest)
			digests[d]++
			if rep.oneThird(digests[d]) {
				CHECKPOINTS = append(CHECKPOINTS, checkpoint)
				break
			}
		}
	}

	var checkpoint *pb.Checkpoint
	var sequence uint64

	for _, c := range CHECKPOINTS {
		s := c.Sequence
		if s >= sequence {
			checkpoint = c
			sequence = s
		}
	}

	// select request for each sequence#
	start = checkpoint.Sequence
	end = start + CHECKPOINT_PERIOD*CONSTANT_FACTOR

	var correct bool
	var summaries []*pb.Summary

	for seq := start; seq <= end; seq++ {

		correct = false

		for _, c := range CHECKPOINTS { //??

			sequence := c.Sequence

			if sequence != seq {
				continue
			}

			if c.Digest != nil {

				var REQ *pb.Request
				var view uint64

				for _, req := range rep.pendingVC {
					reqViewChange := req.GetViewchange()
					for _, prep := range reqViewChange.GetPreps() {
						s := prep.Sequence
						d := prep.Digest
						if s != sequence || !pb.EQ(d, c.Digest) {
							continue
						}
						v := prep.View
						if v > view {
							REQ = req
							view = v
						}
					}
				}

				if REQ == nil {
					continue
				}

				verifiedA1 := make(chan bool)

				go func() {

					var A1 []*pb.Request

				FOR_LOOP:
					for _, req := range rep.pendingVC {
						h := req.LowWaterMark()
						if h >= sequence {
							continue
						}
						reqViewChange := req.GetViewchange()
						for _, prep := range reqViewChange.GetPreps() {
							s := prep.Sequence
							if s != sequence {
								continue
							}
							v := prep.View
							d := prep.Digest
							if v > view || (v == view && !pb.EQ(d, c.Digest)) {
								continue FOR_LOOP
							}
						}
						A1 = append(A1, req)
						if rep.twoThirds(len(A1)) {
							verifiedA1 <- true
							return
						}
					}
					verifiedA1 <- false
				}()

				verifiedA2 := make(chan bool)

				go func() {

					var A2 []*pb.Request

					for _, req := range rep.pendingVC {
						reqViewChange := req.GetViewchange()
						for _, prePrep := range reqViewChange.GetPrepreps() {
							v := prePrep.View
							s := prePrep.Sequence
							d := prePrep.Digest
							if s == sequence && pb.EQ(d, c.Digest) && v >= view {
								A2 = append(A2, req)
								break
							}
						}
						if rep.oneThird(len(A2)) {
							verifiedA2 <- true
							return
						}
					}
					verifiedA2 <- false
				}()

				if !<-verifiedA1 || !<-verifiedA2 {
					continue
				}

				summary := pb.ToSummary(sequence, REQ.Digest())
				summaries = append(summaries, summary)
				correct = true
				break

			} else {

				var A1 []*pb.Request

			FOR_LOOP:
				for _, req := range rep.pendingVC {
					h := req.LowWaterMark()
					if h >= sequence {
						continue
					}
					reqViewChange := req.GetViewchange()
					for _, prep := range reqViewChange.GetPreps() {
						s := prep.Sequence
						if s == sequence {
							continue FOR_LOOP
						}
					}
					A1 = append(A1, req)
				}

				if rep.twoThirds(len(A1)) {
					summary := pb.ToSummary(sequence, nil)
					summaries = append(summaries, summary)
					correct = true
					break
				}
			}
		}

		if !correct {
			return
		}
	}

	req := pb.ToRequestNewView(
		VIEW,
		checkpoint,
		viewChanges,
		summaries,
		rep.ID)

	if rep.hasRequest(req) {
		return
	}

	rep.logRequest(req)
	go func() {
		rep.requestChan <- req
	}()

	// Process new view

	var h uint64
	for _, checkpoint := range rep.checkpoints {
		if checkpoint.Sequence < h || h == uint64(0) {
			h = checkpoint.Sequence
		}
	}

	for _, summary := range summaries {

		if summary.Sequence <= h {
			break
		}

		req = pb.ToRequestPreprepare(
			VIEW,
			summary.Sequence,
			summary.Digest,
			rep.ID)

		if !rep.hasRequest(req) {
			rep.logRequest(req)
		}
	}

	var maxSequence uint64
	for _, req := range rep.requests["pre-prepare"] {

		reqPrePrepare := req.GetPreprepare()

		if reqPrePrepare.Sequence > maxSequence {
			maxSequence = reqPrePrepare.Sequence
		}
	}
	rep.sequence = maxSequence
}
