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

import (
	"sync"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/tchannel-go/thrift"
)

const metaContextTimeout = 10 * time.Second

// cheramiCommitter is commits ackLevels to Cassandra through the TChanMetadataClient interface
type cheramiCommitter struct {
	outputHostUUID     string
	cgUUID             string
	extUUID            string
	connectedStoreUUID *string
	commitLevel        CommitterLevel
	readLevel          CommitterLevel
	finalLevel         CommitterLevel
	metaclient         metadata.TChanMetadataService
	m                  sync.Mutex
}

/*
 * Committer interface
 */

// Commit just updates the next Cherami ack level that will be flushed
func (c *cheramiCommitter) Commit(l CommitterLevel) {
	c.m.Lock()
	c.commitLevel = l
	c.m.Unlock()
}

// Read just updates the next Cherami read level that will be flushed
func (c *cheramiCommitter) Read(l CommitterLevel) {
	c.m.Lock()
	c.readLevel = l
	c.m.Unlock()
}

// Final just updates the last possible read level
func (c *cheramiCommitter) Final(l CommitterLevel) {
	c.m.Lock()
	c.finalLevel = l
	c.m.Unlock()
}

// Flush pushes our commit and read levels to Cherami metadata, using SetAckOffset
func (c *cheramiCommitter) Flush() error {
	c.m.Lock()
	defer c.m.Unlock()
	ctx, cancel := thrift.NewContext(metaContextTimeout)
	defer cancel()

	oReq := &metadata.SetAckOffsetRequest{
		Status:             common.CheramiConsumerGroupExtentStatusPtr(metadata.ConsumerGroupExtentStatus_OPEN),
		OutputHostUUID:     common.StringPtr(c.outputHostUUID),
		ConsumerGroupUUID:  common.StringPtr(c.cgUUID),
		ExtentUUID:         common.StringPtr(c.extUUID),
		ConnectedStoreUUID: common.StringPtr(*c.connectedStoreUUID),
		AckLevelAddress:    common.Int64Ptr(int64(c.commitLevel.address)),
		AckLevelSeqNo:      common.Int64Ptr(int64(c.commitLevel.seqNo)),
		ReadLevelAddress:   common.Int64Ptr(int64(c.readLevel.address)),
		ReadLevelSeqNo:     common.Int64Ptr(int64(c.readLevel.seqNo)),
	}

	if c.finalLevel != CommitterLevel(CommitterLevel{}) { // If the final level has been set
		if c.finalLevel.address == c.readLevel.address && c.readLevel.address == c.commitLevel.address { // And final==read==commit
			oReq.Status = common.CheramiConsumerGroupExtentStatusPtr(metadata.ConsumerGroupExtentStatus_CONSUMED)
		}
	}

	return c.metaclient.SetAckOffset(ctx, oReq)
}

/*
 * Setup & Utility
 */

// NewCheramiCommitter instantiates a cheramiCommitter
func NewCheramiCommitter(metaclient metadata.TChanMetadataService,
	outputHostUUID string,
	cgUUID string,
	extUUID string,
	connectedStoreUUID *string) *cheramiCommitter {
	return &cheramiCommitter{
		metaclient:         metaclient,
		outputHostUUID:     outputHostUUID,
		cgUUID:             cgUUID,
		extUUID:            extUUID,
		connectedStoreUUID: connectedStoreUUID,
	}
}

// GetRead returns the next readlevel that will be flushed
func (c *cheramiCommitter) GetRead() (l CommitterLevel) {
	c.m.Lock()
	l = c.readLevel
	c.m.Unlock()
	return
}

// GetCommit returns the next commit level that will be flushed
func (c *cheramiCommitter) GetCommit() (l CommitterLevel) {
	c.m.Lock()
	l = c.commitLevel
	c.m.Unlock()
	return
}
