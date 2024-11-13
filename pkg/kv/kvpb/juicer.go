package kvpb

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "github.com/mit-pdos/gokv/grove_ffi"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	cmap "github.com/orcaman/concurrent-map"
	"google.golang.org/grpc"
)

var TotalTransactionAborts atomic.Uint64
var TotalTransactionCommits atomic.Uint64

type TransactionId string

// const TotalTransactionSites uint64 = 64
const KEY_QUEUE_DURATION_MULTIPLIER float64 = 50
const DEFAULT_FLUSH_DURATION = 10 // 5 ms
const FLUSH_TIME_STEPS = 1        // 1ms

var reconfigEpoch int64 = 10
var reconfigTime int64 = 0
var prevRTT uint64 = 0
var curRTT uint64 = 0
var nodeId uint64 = 0

type juicerLog struct {
	log        *string
	isBatchLog bool
}

type rttValue struct {
	prevRTT uint32
	curRTT  uint32
}

type TransactionMetrics struct {
	info                        map[string]*TransactionInfo
	loggingCh                   chan *juicerLog
	batchId                     atomic.Uint64
	prevRTT                     uint64
	coordRTTMap                 cmap.ConcurrentMap
	serverRTTMap                map[uint64]*metric
	serverRTTMapMutex           sync.Mutex
	txnIdTimestampConcurrentMap cmap.ConcurrentMap
	keyQueues                   cmap.ConcurrentMap
}

var transactionStats *TransactionMetrics

type BatchSingleRequest interface {
	StartKey() string
	EndKey() string
	MonitoringEnabled() bool
	Type() string
}

func (ba *BatchRequest) IsTransaction() bool {
	return ba.Txn != nil
}

func (ba *BatchRequest) TransactionId() string {
	return ba.Txn.ID.String()
}

func (ba *BatchRequest) TransactionType() string {
	return ""
}

func (ba *BatchRequest) Isolationlevel() string {
	return ""
}

func (br *BatchRequest) GetTxnRequests() []BatchSingleRequest {
	var txnReq []BatchSingleRequest
	for _, req := range br.Requests {
		r := req
		// fmt.Printf("%s\n", req)
		if r.MonitoringEnabled() {
			txnReq = append(txnReq, r)
		}
	}
	return txnReq
}

func (br *BatchRequest) GetReadConsistency() string {
	return br.ReadConsistency.String()
}

func (br *BatchResponse) Status() string {
	return ""
}

func (br *BatchResponse) GetTxnResponses() interface{} {
	return br.Responses
}

func (r RequestUnion) StartKey() string {
	if r.GetPut() != nil {
		req := r.GetPut()
		return req.Key.String()
	} else if r.GetGet() != nil {
		req := r.GetGet()
		return req.Key.String()
	} else if r.GetConditionalPut() != nil {
		req := r.GetConditionalPut()
		return req.Key.String()
	}
	return ""
}

func (r RequestUnion) EndKey() string {
	if r.GetPut() != nil {
		req := r.GetPut()
		return req.EndKey.String()
	} else if r.GetGet() != nil {
		req := r.GetGet()
		return req.EndKey.String()
	} else if r.GetConditionalPut() != nil {
		req := r.GetConditionalPut()
		return req.EndKey.String()
	}
	return ""
}

func (r RequestUnion) Type() string {
	if r.GetPut() != nil {
		return "put"
	} else if r.GetGet() != nil {
		return "get"
	} else if r.GetConditionalPut() != nil {
		return "conditional put"
	} else if r.GetEndTxn() != nil {
		if r.GetEndTxn().Commit {
			return "commit"
		}
		return "abort"
	}
	return ""
}

func (r RequestUnion) MonitoringEnabled() bool {
	if r.GetGet() != nil || r.GetPut() != nil || r.GetConditionalPut() != nil || r.GetEndTxn() != nil {
		return true
	}
	return false
}

func (br *BatchRequest) SetTimestamp(time uint64) {
	br.TxnTimestamp = time
}

func (br *BatchRequest) GetTimestamp() uint64 {
	return br.TxnTimestamp
}

func (br *BatchRequest) SetRequestTimestamp(ts uint64) {
	br.RequestTimestamp = ts
}

func (br *BatchRequest) GetRequestTimestamp() uint64 {
	return br.RequestTimestamp
}

func (br *BatchRequest) IsTransactionEnd() bool {
	for _, req := range br.Requests {
		r := req
		if r.GetEndTxn() != nil {
			return true
		}
	}
	return false
}

func init() {
	nodeId = uint64(time.Now().UnixNano())
	fmt.Println("Node Id: " + fmt.Sprint(nodeId))
	transactionStats = &TransactionMetrics{
		info:                        make(map[string]*TransactionInfo),
		loggingCh:                   make(chan *juicerLog, 10000),
		batchId:                     atomic.Uint64{},
		coordRTTMap:                 cmap.New(),               // coordinator - s1 -> most recent rtt
		serverRTTMap:                make(map[uint64]*metric), // server - c1 -> rtts {count = rtt1 + rtt2 + rtt3, total = n} -> sum(rtts) / n
		serverRTTMapMutex:           sync.Mutex{},
		txnIdTimestampConcurrentMap: cmap.New(), // txnId -> ts
		keyQueues:                   cmap.New(), // key -> KeyQueue
	}
}

var randSrc = rand.NewSource(time.Now().UnixNano())

type TransactionInfo struct {
	id                      string
	status                  string
	txnType                 string
	isComplete              bool
	processingTime          time.Duration
	isolationLevel          string
	requestArrivalTime      time.Time
	requestCompleteTime     time.Time
	requestDelayStartTime   time.Time
	requestProcessStartTime time.Time
	numRequests             int
	batchId                 uint64
	readConsistency         string
	txnTimestamp            uint64

	abortReason string
}

type TransactionMessage interface {
	TransactionId() string
	TransactionType() string
	Isolationlevel() string
	IsTransaction() bool
	GetTxnRequests() []BatchSingleRequest
	GetReadConsistency() string
	IsTransactionEnd() bool
}

type TimestampedTransactionMessage interface {
	SetTimestamp(uint64)
	GetTimestamp() uint64
}

type TransactionResponse interface {
	Status() string
	GetTxnResponses() interface{}
}

type TimestampDeltaEntry struct {
	delta        float64
	requestCount uint64
}

type logentry struct {
	fields  *string
	message *string
}

func (txn *TransactionInfo) logFields() *juicerLog {
	log := fmt.Sprintf("%s,%s,%d,%s,%s,%s,%s,%s,%d,%s,%d", txn.id,
		txn.status,
		txn.processingTime.Abs().Microseconds(),
		txn.isolationLevel,
		txn.requestArrivalTime,
		txn.requestCompleteTime,
		txn.requestDelayStartTime,
		txn.requestProcessStartTime,
		txn.batchId,
		txn.readConsistency,
		txn.txnTimestamp)
	return &juicerLog{log: &log, isBatchLog: true}
}

func logFields(r BatchSingleRequest, batchId uint64) *juicerLog {
	log := fmt.Sprintf("%d,%s,%s,%s", batchId, r.StartKey(), r.EndKey(), r.Type())
	return &juicerLog{log: &log, isBatchLog: false}
}

func constructTransactionInfo(message TransactionMessage, batchId uint64, ctx context.Context, txnTimestamp uint64) *TransactionInfo {
	// peerInfo, _ := peer.FromContext(ctx)
	return &TransactionInfo{
		id:                 message.TransactionId(),
		txnType:            message.TransactionType(),
		isComplete:         false,
		isolationLevel:     message.Isolationlevel(),
		requestArrivalTime: time.Now(),
		numRequests:        0,
		batchId:            batchId,
		readConsistency:    message.GetReadConsistency(),
		txnTimestamp:       txnTimestamp,
	}
}

func TransactionServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// return handler(ctx, req)
	txnMessage, ok := req.(TransactionMessage)
	if !ok || !txnMessage.IsTransaction() {
		return handler(ctx, req)
	}

	serverTimestamp := uint64(time.Now().Nanosecond())
	var timestamp uint64
	timestampMessage, ok := req.(TimestampedTransactionMessage)
	if !ok {
		timestamp = 0
	} else {
		timestamp = timestampMessage.GetTimestamp()
	}
	batchId := transactionStats.batchId.Add(1)
	txnInfo := constructTransactionInfo(txnMessage, batchId, ctx, timestamp)

	txnInfo.requestDelayStartTime = time.Now()

	monitoredTxnRequests := txnMessage.GetTxnRequests()
	wg := sync.WaitGroup{}

	ba, _ := req.(*BatchRequest)

	client := ba.CoordNodeId

	for _, txnRequest := range monitoredTxnRequests {
		if txnRequest.StartKey() == "" {
			continue
		}
		wg.Add(1)
		var ts hlc.Timestamp
		if txnRequest.Type() == "get" {
			ts = ba.Txn.ReadTimestamp
		} else {
			ts = ba.Txn.WriteTimestamp
		}
		rtt := ba.Rtt

		transactionStats.serverRTTMapMutex.Lock()
		_, ok = transactionStats.serverRTTMap[client]
		if !ok {
			transactionStats.serverRTTMap[client] = &metric{}
		}
		transactionStats.serverRTTMap[client].count.Add(rtt)
		transactionStats.serverRTTMap[client].total.Add(1)
		transactionStats.serverRTTMapMutex.Unlock()

		request := &QueueValue{
			requestWg:    &wg,
			key:          txnRequest.StartKey(),
			TxnTimestamp: ts,
		}
		transactionStats.keyQueues.SetIfAbsent(txnRequest.StartKey(), newQueue(txnRequest.StartKey()))
		// log.Infof(context.TODO(), "JUICER - Monitored txn request %s %s", txnRequest.StartKey(), txnRequest.EndKey())
		qValue, _ := transactionStats.keyQueues.Get(txnRequest.StartKey())
		q := qValue.(*KeyQueue)
		q.mu.Lock()
		q.curFlush.abort.total.Add(1)
		q.Push(request)
		if !q.timerStarted {
			q.timerStarted = true
			go flushQueue(q)
		}
		q.mu.Unlock()
	}

	wg.Wait()

	txnInfo.requestProcessStartTime = time.Now()
	res, err := handler(ctx, req)

	txnReply := res.(TransactionResponse)
	txnInfo.status = txnReply.Status()
	txnInfo.isComplete = true
	txnInfo.requestCompleteTime = time.Now()
	txnInfo.processingTime = txnInfo.requestCompleteTime.Sub(txnInfo.requestProcessStartTime)

	br, ok := res.(*BatchResponse)
	if ok {
		br.TimestampS = serverTimestamp
		br.ServerNodeId = nodeId
		// Increases the abort counts
		if br.Error != nil && len(monitoredTxnRequests) > 0 && monitoredTxnRequests[0].StartKey() != "" {
			// log.Errorf(context.TODO(), "JUICER - Error response for batch response %v", *br)
			qValue, _ := transactionStats.keyQueues.Get(monitoredTxnRequests[0].StartKey())
			queue := qValue.(*KeyQueue)
			queue.mu.Lock()
			queue.curFlush.abort.count.Add(1)
			// log.Infof(context.TODO(), "JUICER - Abort rate for queue %s %d %d", monitoredTxnRequests[0].StartKey(), queue.abortRate.count.Load(), queue.abortRate.total.Load())
			queue.mu.Unlock()
		}
	}
	return res, err
}

func ClientUnaryInterceptor(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// return invoker(ctx, method, req, resp, cc, opts...)
	txnMessage, ok := req.(TransactionMessage)
	if !ok || !txnMessage.IsTransaction() {
		return invoker(ctx, method, req, resp, cc, opts...)
	}

	txnTimestampMessage, ok := req.(TimestampedTransactionMessage)
	if !ok {
		return invoker(ctx, method, req, resp, cc, opts...)
	}

	ba := req.(*BatchRequest)
	ba.CoordNodeId = nodeId
	txnId := txnMessage.TransactionId()
	var timestamp uint64
	ts, ok := transactionStats.txnIdTimestampConcurrentMap.Get(txnId)
	if !ok {
		timestamp = generateTid()
	} else {
		timestamp = ts.(uint64)
	}

	clientTimestamp := uint64(time.Now().Nanosecond())
	txnTimestampMessage.SetTimestamp(timestamp)

	if cc != nil {
		rtt, found := transactionStats.coordRTTMap.Get(cc.Target())
		if found {
			ba.Rtt = rtt.(uint64)
		}
	}

	err := invoker(ctx, method, req, resp, cc, opts...)
	if err != nil {
		return err
	}

	if cc != nil {
		br, ok := resp.(*BatchResponse)
		if ok {
			rtt := uint64(time.Now().Nanosecond()) - clientTimestamp
			transactionStats.coordRTTMap.SetIfAbsent(fmt.Sprint(br.ServerNodeId), rtt)
		}
	}

	if txnMessage.IsTransactionEnd() {
		transactionStats.txnIdTimestampConcurrentMap.Remove(txnId)
	}
	return nil
}

func generateTid() uint64 {
	return uint64(time.Now().Nanosecond())
}

func flushQueue(q *KeyQueue) {

	<-time.NewTimer(time.Millisecond * time.Duration(q.curFlush.flushTime)).C

	q.mu.Lock()
	defer q.mu.Unlock()

	for q.Len() > 0 {
		q.Pop().requestWg.Done()
	}

	q.timerStarted = false
	// not atomic - add mutex?
	transactionStats.serverRTTMapMutex.Lock()
	if time.Now().Unix()-reconfigTime > reconfigEpoch {
		// timer elapsed
		reconfigTime = time.Now().Unix()
		curRTT = calculateRTT()
		if q.heuristicsEquation(curRTT) {
			var newFlushTime uint32
			q.prevFlush = q.curFlush.flushTime
			if int(q.curFlush.flushTime)-FLUSH_TIME_STEPS < 0 {
				newFlushTime = 0
			} else {
				newFlushTime = q.curFlush.flushTime - FLUSH_TIME_STEPS
			}
			// fmt.Printf("Next flush time for queue %s = %d", q.key, newFlushTime)
			q.curFlush = flushStats{
				flushTime: newFlushTime,
				abort:     metric{},
			}
		} else {
			q.curFlush.abort = metric{}
		}
		prevRTT = curRTT
	}
	transactionStats.serverRTTMapMutex.Unlock()
}

func calculateRTT() uint64 {
	rtts := uint64(0)
	total := uint64(0)
	for k, v := range transactionStats.serverRTTMap {
		rtts += v.count.Load() / v.total.Load()
		total++
		delete(transactionStats.serverRTTMap, k)
	}
	return rtts / total
}
