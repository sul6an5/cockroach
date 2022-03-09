// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/errors"
)

// forwarder is used to forward pgwire messages from the client to the server,
// and vice-versa. At the moment, this does a direct proxying, and there is
// no intercepting. Once https://github.com/cockroachdb/cockroach/issues/76000
// has been addressed, we will start intercepting pgwire messages at their
// boundaries here.
//
// The forwarder instance should always be constructed through the forward
// function, which also starts the forwarder.
type forwarder struct {
	// ctx is a single context used to control all goroutines spawned by the
	// forwarder.
	ctx       context.Context
	ctxCancel context.CancelFunc

	// clientConn and serverConn provide a convenient way to read and forward
	// Postgres messages, while minimizing IO reads and memory allocations.
	//
	// clientConn is set once during initialization, and stays the same
	// throughout the lifetime of the forwarder.
	//
	// serverConn is only set after the authentication phase for the initial
	// connection. In the context of a connection migration, serverConn is only
	// replaced once the session has successfully been deserialized, and the old
	// connection will be closed. Whenever serverConn gets updated, both
	// clientMessageTypeSent and isServerMsgReadyReceived fields have to reset
	// to their initial values.
	//
	// All reads from these connections must go through the interceptors. It is
	// not safe to call Read directly as the interceptors may have buffered data.
	clientConn *interceptor.BackendConn  // client <-> proxy
	serverConn *interceptor.FrontendConn // proxy <-> server

	// errChan is a buffered channel that contains the first forwarder error.
	// This channel may receive nil errors.
	errChan chan error
}

// forward returns a new instance of forwarder, and starts forwarding messages
// from clientConn to serverConn. When this is called, it is expected that the
// caller passes ownership of serverConn to the forwarder, which implies that
// the forwarder will clean up serverConn. clientConn and serverConn must not
// be nil in all cases except for testing.
//
// Note that callers MUST call Close in all cases, even if ctx was cancelled.
func forward(ctx context.Context, clientConn, serverConn net.Conn) *forwarder {
	ctx, cancelFn := context.WithCancel(ctx)

	f := &forwarder{
		ctx:       ctx,
		ctxCancel: cancelFn,
		errChan:   make(chan error, 1),
	}

	// The net.Conn object for the client is switched to a net.Conn that
	// unblocks Read every second on idle to check for exit conditions.
	// This is mainly used to unblock the request processor whenever the
	// forwarder has stopped, or a transfer has been requested.
	clientConn = pgwire.NewReadTimeoutConn(clientConn, func() error {
		// Context was cancelled.
		if f.ctx.Err() != nil {
			return f.ctx.Err()
		}
		// TODO(jaylim-crl): Check for transfer state here.
		return nil
	})
	f.clientConn = interceptor.NewBackendConn(clientConn)
	f.serverConn = interceptor.NewFrontendConn(serverConn)

	// Start request (client to server) and response (server to client)
	// processors. We will copy all pgwire messages/ from client to server
	// (and vice-versa) until we encounter an error or a shutdown signal
	// (i.e. context cancellation).
	go func() {
		defer f.Close()

		err := wrapClientToServerError(f.handleClientToServer())
		select {
		case f.errChan <- err: /* error reported */
		default: /* the channel already contains an error */
		}
	}()
	go func() {
		defer f.Close()

		err := wrapServerToClientError(f.handleServerToClient())
		select {
		case f.errChan <- err: /* error reported */
		default: /* the channel already contains an error */
		}
	}()

	return f
}

// Close closes the forwarder, and stops the forwarding process. This is
// idempotent.
func (f *forwarder) Close() {
	f.ctxCancel()

	// Since Close is idempotent, we'll ignore the error from Close in case it
	// has already been closed.
	f.serverConn.Close()
}

// handleClientToServer handles the communication from the client to the server.
// This returns a context cancellation error whenever the forwarder's context
// is cancelled, or whenever forwarding fails. When ForwardMsg gets blocked on
// Read, we will unblock that through our custom readTimeoutConn wrapper, which
// gets triggered when context is cancelled.
func (f *forwarder) handleClientToServer() error {
	for f.ctx.Err() == nil {
		if _, err := f.clientConn.ForwardMsg(f.serverConn); err != nil {
			return err
		}
	}
	return f.ctx.Err()
}

// handleServerToClient handles the communication from the server to the client.
// This returns a context cancellation error whenever the forwarder's context
// is cancelled, or whenever forwarding fails. When ForwardMsg gets blocked on
// Read, we will unblock that by closing serverConn through f.Close().
func (f *forwarder) handleServerToClient() error {
	for f.ctx.Err() == nil {
		if _, err := f.serverConn.ForwardMsg(f.clientConn); err != nil {
			return err
		}
	}
	return f.ctx.Err()
}

// wrapClientToServerError overrides client to server errors for external
// consumption.
//
// TODO(jaylim-crl): We don't send any of these to the client today,
// unfortunately. At the moment, this is only used for metrics. See TODO in
// proxy_handler about sending safely to avoid corrupted packets. Handle these
// errors in a friendly manner.
func wrapClientToServerError(err error) error {
	if err == nil ||
		errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return nil
	}
	return newErrorf(codeClientDisconnected, "copying from client to target server: %v", err)
}

// wrapServerToClientError overrides server to client errors for external
// consumption.
//
// TODO(jaylim-crl): We don't send any of these to the client today,
// unfortunately. At the moment, this is only used for metrics. See TODO in
// proxy_handler about sending safely to avoid corrupted packets. Handle these
// errors in a friendly manner.
func wrapServerToClientError(err error) error {
	if err == nil ||
		errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return nil
	}
	return newErrorf(codeBackendDisconnected, "copying from target server to client: %s", err)
}