// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datanode

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	milvus_storage "github.com/milvus-io/milvus-storage/go/storage"
	"github.com/milvus-io/milvus-storage/go/storage/options"
	"github.com/milvus-io/milvus-storage/go/storage/schema"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

func TestFlushTaskRunner(t *testing.T) {
	task := newFlushTaskRunner(1, nil)
	signal := make(chan struct{})

	saveFlag := false
	nextFlag := false
	processed := make(chan struct{})

	task.init(func(*segmentFlushPack) {
		saveFlag = true
	}, func(pack *segmentFlushPack, i postInjectionFunc) {}, signal)

	go func() {
		<-task.finishSignal
		nextFlag = true
		processed <- struct{}{}
	}()

	assert.False(t, saveFlag)
	assert.False(t, nextFlag)

	task.runFlushInsert(&emptyFlushTask{}, nil, nil, false, false, nil)
	task.runFlushDel(&emptyFlushTask{}, &DelDataBuf{})

	assert.False(t, saveFlag)
	assert.False(t, nextFlag)

	close(signal)
	<-processed

	assert.True(t, saveFlag)
	assert.True(t, nextFlag)
}

func TestFlushTaskRunner_FailError(t *testing.T) {
	task := newFlushTaskRunner(1, nil)
	signal := make(chan struct{})

	errFlag := false
	nextFlag := false
	processed := make(chan struct{})

	task.init(func(pack *segmentFlushPack) {
		if pack.err != nil {
			errFlag = true
		}
	}, func(pack *segmentFlushPack, i postInjectionFunc) {}, signal)

	go func() {
		<-task.finishSignal
		nextFlag = true
		processed <- struct{}{}
	}()

	assert.False(t, errFlag)
	assert.False(t, nextFlag)

	task.runFlushInsert(&errFlushTask{}, nil, nil, false, false, nil, retry.Attempts(1))
	task.runFlushDel(&errFlushTask{}, &DelDataBuf{}, retry.Attempts(1))

	assert.False(t, errFlag)
	assert.False(t, nextFlag)

	close(signal)
	<-processed

	assert.True(t, errFlag)
	assert.True(t, nextFlag)
}

func TestFlushTaskRunner_Injection(t *testing.T) {
	injectCh := make(chan *taskInjection, 1)
	task := newFlushTaskRunner(1, injectCh)
	signal := make(chan struct{})

	saveFlag := false
	nextFlag := false
	processed := make(chan struct{})

	ti := newTaskInjection(1, func(pack *segmentFlushPack) {
		t.Log("task injection executed")
		pack.segmentID = 2
	})
	go ti.waitForInjected()
	injectCh <- ti

	go func() {
		<-ti.injected
		ti.injectDone(true)
	}()

	task.init(func(pack *segmentFlushPack) {
		assert.EqualValues(t, 2, pack.segmentID)
		saveFlag = true
	}, func(pack *segmentFlushPack, i postInjectionFunc) {
		if i != nil {
			i(pack)
		}
	}, signal)

	go func() {
		<-task.finishSignal
		nextFlag = true
		processed <- struct{}{}
	}()

	assert.False(t, saveFlag)
	assert.False(t, nextFlag)

	task.runFlushInsert(&emptyFlushTask{}, nil, nil, false, false, nil)
	task.runFlushDel(&emptyFlushTask{}, &DelDataBuf{})

	assert.False(t, saveFlag)
	assert.False(t, nextFlag)

	close(signal)
	<-processed

	assert.True(t, saveFlag)
	assert.True(t, nextFlag)
}

func TestFlushTaskRunnerV2(t *testing.T) {
	tmpDir := t.TempDir()
	injectCh := make(chan *taskInjection)
	runner := newFlushTaskRunnerV2(1, injectCh)
	startSig := make(chan struct{})

	flushed := false
	runner.init(func(*segmentFlushPack) {
		flushed = true
	}, func(pack *segmentFlushPack, i postInjectionFunc) {}, startSig)

	arrowSchema, err := convertToArrowSchema([]*schemapb.FieldSchema{
		{Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
		{Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{Name: "ts", DataType: schemapb.DataType_Int64},
	})
	assert.NoError(t, err)

	space, err := milvus_storage.Open(fmt.Sprintf("file:///%s", tmpDir), options.NewSpaceOptionBuilder().SetSchema(schema.NewSchema(arrowSchema, &schema.SchemaOptions{PrimaryColumn: "pk", VectorColumn: "vec", VersionColumn: "ts"})).Build())
	assert.NoError(t, err)

	itr, err := array.NewRecordReader(arrowSchema, []arrow.Record{})
	assert.NoError(t, err)
	task := &flushBufferInsertTask2{
		space:     space,
		reader:    itr,
		statsBlob: nil,
		flush:     true,
	}
	runner.runFlushInsert(task, false, true, nil)
	runner.runFlushDel(&flushBufferDeleteTask2{})

	startSig <- struct{}{}
	<-runner.finishSignal
	assert.True(t, flushed)
}
