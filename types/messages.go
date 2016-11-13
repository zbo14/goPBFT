package types

import (
	"github.com/golang/protobuf/proto"
	"github.com/tendermint/go-wire"
	"net"
)

// Checkpoint

func ToCheckpoint(sequence uint64, digest string) *Checkpoint {
	return &Checkpoint{sequence, digest}
}

// Entry

func ToEntry(sequence uint64, digest string, view uint64) *Entry {
	return &Entry{sequence, digest, view}
}

// ViewChange

func ToViewChange(view, sequence uint64, checkpoints []*Checkpoint, prepares, prePrepares []*Entry, viewchanger uint64) *ViewChange {
	return &ViewChange{view, sequence, checkpoints, prepares, prePrepares, viewchanger}
}

func (vc *ViewChange) LowWaterMark() uint64 {
	checkpoints := vc.Checkpoints
	lastStable := checkpoints[len(checkpoints)-1]
	lwm := lastStable.Sequence
	return lwm
}

// Request

func ToRequestClient(op *Operation, timestamp, client string) *Request {
	return &Request{
		Value: &Request_Client{
			&RequestClient{op, timestamp, client}},
	}
}

func ToRequestPreprepare(view, sequence uint64, digest string, replica uint64) *Request {
	return &Request{
		Value: &Request_Preprepare{
			&RequestPrePrepare{view, sequence, digest, replica}},
	}
}

func ToRequestPrepare(view, sequence uint64, digest string, replica uint64) *Request {
	return &Request{
		Value: &Request_Prepare{
			&RequestPrepare{view, sequence, digest, replica}},
	}
}

func ToRequestCommit(view, sequence, replica uint64) *Request {
	return &Request{
		Value: &Request_Commit{
			&RequestCommit{view, sequence, replica}},
	}
}

func ToRequestCheckpoint(sequence uint64, digest string, replica uint64) *Request {
	return &Request{
		Value: &Request_Checkpoint{
			&RequestCheckpoint{sequence, digest, replica}},
	}
}

func ToRequestViewChange(view, sequence uint64, checkpoints []*Checkpoint, prepares, prePrepares []*Entry, replica uint64) *Request {
	return &Request{
		Value: &Request_Viewchange{
			&RequestViewChange{view, sequence, checkpoints, prepares, prePrepares, replica}},
	}
}

func ToRequestAck(view, replica, viewchanger uint64, digest string) *Request {
	return &Request{
		Value: &Request_Ack{
			&RequestAck{view, replica, viewchanger, digest}},
	}
}

func ToRequestNewView(view uint64, v []*ViewChange, checkpoint *Checkpoint, x []*ViewChange) *Request {
	return &Request{
		Value: &Request_Newview{
			&RequestNewView{view, v, checkpoint, x}},
	}
}

// Reply

func ToReply(view uint64, timestamp, client string, replica uint64, result *Result) *Reply {
	return &Reply{view, timestamp, client, replica, result}
}

// Write proto message

func WriteMessage(addr string, msg proto.Message) error {
	conn, err := net.Dial("tcp", addr)
	defer conn.Close()
	if err != nil {
		return err
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	var n int
	wire.WriteBinary(bz, conn, &n, &err)
	return err
}

// Read proto message

func ReadMessage(conn net.Conn, msg proto.Message) error {
	n, err := int(0), error(nil)
	buf := wire.ReadByteSlice(conn, 0, &n, &err)
	if err != nil {
		return err
	}
	err = proto.Unmarshal(buf, msg)
	return err
}
