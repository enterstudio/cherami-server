// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package outputhost

import "github.com/uber/cherami-server/common"

// Committer is an interface that wraps the internals of how offsets/acklevels are committed for a given queueing
// system (e.g. Kafka or Cherami)
type (
	CommitterLevel struct {
		seqNo   common.SequenceNumber // Logical sequence number of the ack manager
		address storeHostAddress      // storage system address in the message
	}

	Committer interface {
		// Commit indicates that work up to and including the message specified by the sequence number and address
		// has been acknowledged. Not guaranteed to be persisted until a successful call to Flush()
		Commit(l CommitterLevel)

		// Read indicates that a particular message has been read. Metadata not guaranteed to be communicated/persisted
		// until a successful call to Flush()
		Read(l CommitterLevel)

		// Final indicates that a particular level is the last that can possibly be read. Metadata not guaranteed
		// to be communicated/persisted until a successful call to Flush()
		Final(l CommitterLevel)

		// Flush pushes accumulated commit/read state to durable storage, e.g. Kafka offset storage or Cherami-Cassandra
		// AckLevel storage
		Flush() error

		// GetCommit receives the last value given to Commit()
		GetCommit() (l CommitterLevel)

		// GetRead receives the last value given to Read()
		GetRead() (l CommitterLevel)
	}
)
