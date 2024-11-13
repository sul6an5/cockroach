package kvpb

import (
	"container/heap"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type metric struct {
	count atomic.Uint64
	total atomic.Uint64
}

type QueueHeap []*QueueValue

type flushStats struct {
	flushTime uint32
	abort     metric
}

type KeyQueue struct {
	mu           sync.Mutex
	key          string
	timerStarted bool
	minHeap      *QueueHeap
	curFlush     flushStats
	prevFlush    uint32
}

type QueueValue struct {
	requestWg    *sync.WaitGroup
	key          string
	TxnTimestamp hlc.Timestamp
}

func newQueue(key string) *KeyQueue {
	q := new(KeyQueue)
	q.curFlush = flushStats{
		flushTime: DEFAULT_FLUSH_DURATION,
		abort:     metric{},
	}
	q.prevFlush = 0
	q.key = key
	q.minHeap = new(QueueHeap)
	heap.Init(q.minHeap)
	return q
}

func (q *KeyQueue) Len() int {
	return q.minHeap.Len()
}

func (q *KeyQueue) Push(x *QueueValue) {
	heap.Push(q.minHeap, x)
}

func (q *KeyQueue) Pop() *QueueValue {
	return heap.Pop(q.minHeap).(*QueueValue)
}

func (q *QueueHeap) Push(x interface{}) {
	*q = append(*q, x.(*QueueValue))
}

func (q *QueueHeap) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

func (q *QueueHeap) Len() int {
	return len(*q)
}

func (q *QueueHeap) Less(i, j int) bool {
	return (*q)[i].TxnTimestamp.Compare((*q)[j].TxnTimestamp) < 0
}

func (q *QueueHeap) Swap(i, j int) {
	(*q)[i], (*q)[j] = (*q)[j], (*q)[i]
}

func (q *KeyQueue) heuristicsEquation(rtt uint64) bool {
	return true
}
