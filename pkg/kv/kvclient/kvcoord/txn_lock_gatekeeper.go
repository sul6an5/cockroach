// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// lockedSender is like a client.Sender but requires the caller to hold the
// TxnCoordSender lock to send requests.
type lockedSender interface {
	// SendLocked sends the batch request and receives a batch response. It
	// requires that the TxnCoordSender lock be held when called, but this lock
	// is not held for the entire duration of the call. Instead, the lock is
	// released immediately before the batch is sent to a lower-level Sender and
	// is re-acquired when the response is returned.
	// WARNING: because the lock is released when calling this method and
	// re-acquired before it returned, callers cannot rely on a single mutual
	// exclusion zone mainted across the call.
	SendLocked(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
}

// txnLockGatekeeper is a lockedSender that sits at the bottom of the
// TxnCoordSender's interceptor stack and handles unlocking the TxnCoordSender's
// mutex when sending a request and locking the TxnCoordSender's mutex when
// receiving a response. It allows the entire txnInterceptor stack to operate
// under lock without needing to worry about unlocking at the correct time.
type txnLockGatekeeper struct {
	wrapped kv.Sender
	mu      sync.Locker // shared with TxnCoordSender

	// If set, concurrent requests are allowed. If not set, concurrent requests
	// result in an assertion error. Only leaf transactions are supposed allow
	// concurrent requests - leaves don't restart the transaction and they don't
	// bump the read timestamp through refreshes.
	allowConcurrentRequests bool
	// requestInFlight is set while a request is being processed by the wrapped
	// sender. Used to detect and prevent concurrent txn use.
	requestInFlight bool
}

// SendLocked implements the lockedSender interface.
func (gs *txnLockGatekeeper) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	// If so configured, protect against concurrent use of the txn. Concurrent
	// requests don't work generally because of races between clients sending
	// requests and the TxnCoordSender restarting the transaction, and also
	// concurrent requests are not compatible with the span refresher in
	// particular since refreshing is invalid if done concurrently with requests
	// in flight whose spans haven't been accounted for.
	//
	// As a special case, allow for async heartbeats to be sent whenever.
	if !gs.allowConcurrentRequests && !ba.IsSingleHeartbeatTxnRequest() {
		if gs.requestInFlight {
			return nil, kvpb.NewError(
				errors.AssertionFailedf("concurrent txn use detected. ba: %s", ba))
		}
		gs.requestInFlight = true
		defer func() {
			gs.requestInFlight = false
		}()
	}

	isTransactionAbort := ba.Requests[len(ba.Requests)-1].GetEndTxn() != nil && !ba.Requests[len(ba.Requests)-1].GetEndTxn().Commit
	isTransactionCommit := ba.Requests[len(ba.Requests)-1].GetEndTxn() != nil && ba.Requests[len(ba.Requests)-1].GetEndTxn().Commit
	if isTransactionAbort {
		// fmt.Printf("JUICER - TXN_LOCK_GATEKEEPER Transaction Abort %v\n", *ba.Header.Txn)
		kvpb.TotalTransactionAborts.Add(1)
		log.Errorf(ctx, "JUICER - TXN_LOCK_GATEKEEPER Transaction Abort %d %s", kvpb.TotalTransactionAborts.Load(), &ba.Header.Txn.ID)
	}
	if isTransactionCommit {
		// fmt.Printf("JUICER - TXN_LOCK_GATEKEEPER Transaction Abort %v\n", *ba.Header.Txn)
		kvpb.TotalTransactionCommits.Add(1)
		log.Errorf(ctx, "JUICER - TXN_LOCK_GATEKEEPER Transaction Commit %d %s", kvpb.TotalTransactionCommits.Load(), &ba.Header.Txn.ID)
	}

	// Note the funky locking here: we unlock for the duration of the call and the
	// lock again.
	gs.mu.Unlock()
	defer gs.mu.Lock()
	return gs.wrapped.Send(ctx, ba)
}
