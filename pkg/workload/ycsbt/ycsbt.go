// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package ycsb is the workload specified by the Yahoo! Cloud Serving Benchmark.
package ycsbt

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"strings"
	"sync/atomic"

	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/pflag"
	"golang.org/x/exp/rand"
)

const (
	numTableFields = 10  // 10 fields per row
	fieldLength    = 100 // In characters (value size: 512 B)
	zipfIMin       = 0
	selectedColumn = "field0" // for r-m-w
	// we now update all fields of a row

	usertableSchemaRelational = `(
		ycsb_key VARCHAR(255) PRIMARY KEY NOT NULL,
		FIELD0 TEXT NOT NULL,
		FIELD1 TEXT NOT NULL,
		FIELD2 TEXT NOT NULL,
		FIELD3 TEXT NOT NULL,
		FIELD4 TEXT NOT NULL,
		FIELD5 TEXT NOT NULL,
		FIELD6 TEXT NOT NULL,
		FIELD7 TEXT NOT NULL,
		FIELD8 TEXT NOT NULL,
		FIELD9 TEXT NOT NULL
	)`
	usertableSchemaRelationalWithFamilies = `(
		ycsb_key VARCHAR(255) PRIMARY KEY NOT NULL,
		FIELD0 TEXT NOT NULL,
		FIELD1 TEXT NOT NULL,
		FIELD2 TEXT NOT NULL,
		FIELD3 TEXT NOT NULL,
		FIELD4 TEXT NOT NULL,
		FIELD5 TEXT NOT NULL,
		FIELD6 TEXT NOT NULL,
		FIELD7 TEXT NOT NULL,
		FIELD8 TEXT NOT NULL,
		FIELD9 TEXT NOT NULL,
		FAMILY (ycsb_key),
		FAMILY (FIELD0),
		FAMILY (FIELD1),
		FAMILY (FIELD2),
		FAMILY (FIELD3),
		FAMILY (FIELD4),
		FAMILY (FIELD5),
		FAMILY (FIELD6),
		FAMILY (FIELD7),
		FAMILY (FIELD8),
		FAMILY (FIELD9)
	)`
	usertableSchemaJSON = `(
		ycsb_key VARCHAR(255) PRIMARY KEY NOT NULL,
		FIELD JSONB
	)`

	timeFormatTemplate = `2006-01-02 15:04:05.000000-07:00`
)

var RandomSeed = workload.NewUint64RandomSeed()

type Shots string

const (
	OneShot   Shots = "one_shot"
	TwoShot   Shots = "two_shot"
	MultiShot Shots = "multi_shot"
	BatchRW   Shots = "one_shot_read_write"
)

type ycsbt struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	timeString  bool
	insertHash  bool
	zeroPadding int
	insertStart int
	insertCount int
	recordCount int
	json        bool
	families    bool
	sfu         bool
	splits      int

	workload                     string
	requestDistribution          string
	scanLengthDistribution       string
	minScanLength, maxScanLength uint64

	// Juicer fields
	txSize                                int
	writeFraction, zipfConstant, rmwRatio float32
	parallel                              string
	shot                                  Shots
}

func init() {
	workload.Register(ycsbtMeta)
}

var ycsbtMeta = workload.Meta{
	Name:        `ycsbt`,
	Description: `YCSB+T is for Juicer benchmarking.`,
	Version:     `1.0.0`,
	RandomSeed:  RandomSeed,
	New: func() workload.Generator {
		g := &ycsbt{}
		g.flags.FlagSet = pflag.NewFlagSet(`ycsbt`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`workload`: {RuntimeOnly: true},
		}
		g.flags.BoolVar(&g.timeString, `time-string`, false, `Prepend field[0-9] data with current time in microsecond precision.`)
		g.flags.BoolVar(&g.insertHash, `insert-hash`, true, `Key to be hashed or ordered.`)
		g.flags.IntVar(&g.zeroPadding, `zero-padding`, 1, `Key using "insert-hash=false" has zeros padded to left to make this length of digits.`)
		g.flags.IntVar(&g.insertStart, `insert-start`, 0, `Key to start initial sequential insertions from. (default 0)`)
		g.flags.IntVar(&g.insertCount, `insert-count`, 10000, `Number of rows to sequentially insert before beginning workload.`)
		g.flags.IntVar(&g.recordCount, `record-count`, 0, `Key to start workload insertions from. Must be >= insert-start + insert-count. (Default: insert-start + insert-count)`)
		g.flags.BoolVar(&g.json, `json`, false, `Use JSONB rather than relational data.`)
		g.flags.BoolVar(&g.families, `families`, true, `Place each column in its own column family.`)
		g.flags.BoolVar(&g.sfu, `select-for-update`, true, `Use SELECT FOR UPDATE syntax in read-modify-write transactions.`)
		g.flags.IntVar(&g.splits, `splits`, 0, `Number of splits to perform before starting normal operations.`)
		g.flags.StringVar(&g.workload, `workload`, `J`, `Workload type does not matter to Juicer.`)
		g.flags.StringVar(&g.requestDistribution, `request-distribution`, `zipfian`, `Distribution for request key generation [zipfian, uniform, latest]. The default for workloads A, B, C, E, and F is zipfian, and the default for workload D is latest.`)
		g.flags.StringVar(&g.scanLengthDistribution, `scan-length-distribution`, `uniform`, `Distribution for scan length generation [zipfian, uniform]. Primarily used for workload E.`)
		g.flags.Uint64Var(&g.minScanLength, `min-scan-length`, 1, `The minimum length for scan operations. Primarily used for workload E.`)
		g.flags.Uint64Var(&g.maxScanLength, `max-scan-length`, 1000, `The maximum length for scan operations. Primarily used for workload E.`)
		// Juicer flags
		g.flags.IntVar(&g.txSize, `tx-size`, 10, `Transaction size: number of requests per tx.`)
		g.flags.Float32Var(&g.writeFraction, `write-fraction`, 0.1, `write fractions: 0.1 means 10% are write transactions, either update or rmw.`)
		g.flags.Float32Var(&g.zipfConstant, `zipf`, 0.99, `Zipfian constant: 1.2 is high skew [0.1 -- 1.2], avoid 1`)
		g.flags.Float32Var(&g.rmwRatio, `rmw`, 0.0, `fraction of read-modify-writes within write transactions`)
		g.flags.StringVar(&g.parallel, `parallel`, `onerw`, `whether requests are sent in [one], [two], [onerw], or [multi] shots. Default to [onerw].`)

		RandomSeed.AddFlag(&g.flags)

		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*ycsbt) Meta() workload.Meta { return ycsbtMeta }

// Flags implements the Flagser interface.
func (g *ycsbt) Flags() workload.Flags { return g.flags }

// Hooks implements the Hookser interface.
func (g *ycsbt) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			var distribution = strings.ToUpper(g.requestDistribution)
			if distribution != "UNIFORM" && distribution != "ZIPFIAN" {
				return errors.Errorf("invalid distribution: %s. Expect ZIPFIAN or UNIFORM.", distribution)
			}
			if g.zipfConstant < 0 || g.zipfConstant == 1 {
				return errors.Errorf("invalid zipfian constant: %f.", g.zipfConstant)
			}
			if g.recordCount == 0 {
				g.recordCount = g.insertStart + g.insertCount
			}
			if g.insertStart+g.insertCount > g.recordCount {
				return errors.Errorf("insertStart + insertCount (%d) must be <= recordCount (%d)", g.insertStart+g.insertCount, g.recordCount)
			}
			if g.txSize <= 0 {
				return errors.Errorf("invalid transaction size. txSize = %d, but must be at least 1.", g.txSize)
			}
			parallel := strings.ToUpper(g.parallel)
			switch parallel {
			case "ONE":
				g.shot = OneShot
			case "TWO":
				g.shot = TwoShot
				g.rmwRatio = 0.0 // 2 shot txs only include reads and updates
			case "MULTI":
				g.shot = MultiShot
			case "ONERW":
				g.shot = BatchRW
			default:
				return errors.Errorf("Invalid parameter for parallel: ONE, TWO, MULTI are expected.")
			}
			return nil
		},
	}
}

var usertableTypes = []*types.T{
	types.Bytes, types.Bytes, types.Bytes, types.Bytes, types.Bytes, types.Bytes,
	types.Bytes, types.Bytes, types.Bytes, types.Bytes, types.Bytes,
}

// Tables implements the Generator interface.
func (g *ycsbt) Tables() []workload.Table {
	usertable := workload.Table{
		Name: `usertable`,
		Splits: workload.Tuples(
			g.splits,
			func(splitIdx int) []interface{} {
				step := math.MaxUint64 / uint64(g.splits+1)
				return []interface{}{
					keyNameFromHash(step * uint64(splitIdx+1)),
				}
			},
		),
	}
	if g.json {
		usertable.Schema = usertableSchemaJSON
		usertable.InitialRows = workload.Tuples(
			g.insertCount,
			func(rowIdx int) []interface{} {
				w := ycsbtWorker{
					config:   g,
					hashFunc: fnv.New64(),
				}
				key := w.buildKeyName(uint64(g.insertStart + rowIdx))
				// TODO(peter): Need to fill in FIELD here, rather than an empty JSONB
				// value.
				return []interface{}{key, "{}"}
			})
	} else {
		if g.families {
			usertable.Schema = usertableSchemaRelationalWithFamilies
		} else {
			usertable.Schema = usertableSchemaRelational
		}

		const batchSize = 1000
		usertable.InitialRows = workload.BatchedTuples{
			NumBatches: (g.insertCount + batchSize - 1) / batchSize,
			FillBatch: func(batchIdx int, cb coldata.Batch, _ *bufalloc.ByteAllocator) {
				rowBegin, rowEnd := batchIdx*batchSize, (batchIdx+1)*batchSize
				if rowEnd > g.insertCount {
					rowEnd = g.insertCount
				}
				cb.Reset(usertableTypes, rowEnd-rowBegin, coldata.StandardColumnFactory)

				key := cb.ColVec(0).Bytes()
				// coldata.Bytes only allows appends so we have to reset it.
				key.Reset()

				var fields [numTableFields]*coldata.Bytes
				for i := range fields {
					fields[i] = cb.ColVec(i + 1).Bytes()
					// coldata.Bytes only allows appends so we have to reset it.
					fields[i].Reset()
				}

				w := ycsbtWorker{
					config:   g,
					hashFunc: fnv.New64(),
				}
				rng := rand.NewSource(RandomSeed.Seed() + uint64(batchIdx))

				var tmpbuf [fieldLength]byte
				for rowIdx := rowBegin; rowIdx < rowEnd; rowIdx++ {
					rowOffset := rowIdx - rowBegin

					key.Set(rowOffset, []byte(w.buildKeyName(uint64(rowIdx))))

					for i := range fields {
						randStringLetters(rng, tmpbuf[:])
						fields[i].Set(rowOffset, tmpbuf[:])
					}
				}
			},
		}
	}
	return []workload.Table{usertable}
}

// Ops implements the Opser interface.
func (g *ycsbt) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(g, g.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// read parallel statement
	var buf strings.Builder
	fmt.Fprintf(&buf, `SELECT * FROM usertable WHERE ycsb_key IN (`)
	for i := 0; i < g.txSize; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, `$%d`, i+1)
	}
	buf.WriteString(`)`)
	readParallelStmtStr := buf.String()

	// Read multishot statement
	readMultishotStmtStr := `SELECT * FROM usertable WHERE ycsb_key = $1`

	// reads in read-modify-writes
	var readFieldForUpdateStmtStr string
	if g.json {
		readFieldForUpdateStmtStr = fmt.Sprintf(`SELECT field->>'%s' FROM usertable WHERE ycsb_key = $1`, selectedColumn)
	} else {
		readFieldForUpdateStmtStr = fmt.Sprintf(`SELECT %s FROM usertable WHERE ycsb_key = $1`, selectedColumn)
	}
	if g.sfu { // TODO: check if select-for-update syntax is correct
		readFieldForUpdateStmtStr = fmt.Sprintf(`%s FOR UPDATE`, readFieldForUpdateStmtStr)
	}

	var insertStmtStr string
	if g.json {
		insertStmtStr = `INSERT INTO usertable VALUES ($1, json_build_object(
			'field0',  $2:::text,
			'field1',  $3:::text,
			'field2',  $4:::text,
			'field3',  $5:::text,
			'field4',  $6:::text,
			'field5',  $7:::text,
			'field6',  $8:::text,
			'field7',  $9:::text,
			'field8',  $10:::text,
			'field9',  $11:::text
		))`
	} else {
		insertStmtStr = `INSERT INTO usertable VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)`
	}

	// udpate parallel statement
	// YCSB+T $1 is the new value for field0, and the rest is keys starting with $2
	var updateParallelStmtStr string
	if g.json { //TODO: check json syntax, do we use this?
		updateParallelStmtStr = `UPDATE usertable SET field = field || $1 WHERE ycsb_key IN (`
	} else {
		// we update all fields in each row
		updateParallelStmtStr = `UPDATE usertable SET 
			field0 = $1, 
			field1 = $1,
			field2 = $1,
			field3 = $1,
			field4 = $1,
			field5 = $1,
			field6 = $1,
			field7 = $1,
			field8 = $1,
			field9 = $1
			WHERE ycsb_key IN (`
	}
	for i := 0; i < g.txSize; i++ {
		if i > 0 {
			updateParallelStmtStr = fmt.Sprintf(`%s, `, updateParallelStmtStr)
		}
		updateParallelStmtStr = fmt.Sprintf(`%s$%d`, updateParallelStmtStr, i+2)
	}
	updateParallelStmtStr = fmt.Sprintf(`%s)`, updateParallelStmtStr)

	// update multishot statement
	var updateMultishotStmtStr string
	if g.json { //TODO: check json syntax, do we use this?
		updateMultishotStmtStr = `UPDATE usertable SET field = field || $1 WHERE ycsb_key = $2`
	} else {
		updateMultishotStmtStr = `UPDATE usertable SET 
		field0 = $1, 
		field1 = $1,
		field2 = $1,
		field3 = $1,
		field4 = $1,
		field5 = $1,
		field6 = $1,
		field7 = $1,
		field8 = $1,
		field9 = $1 
		WHERE ycsb_key = $2`
	}

	rowIndexVal := uint64(g.recordCount)
	rowIndex := &rowIndexVal
	rowCounter := NewAcknowledgedCounter((uint64)(g.recordCount))

	var requestGen randGenerator
	requestGenRng := rand.New(rand.NewSource(RandomSeed.Seed()))
	switch strings.ToLower(g.requestDistribution) {
	case "zipfian":
		requestGen, err = NewRejectionInversionGenerator(uint64(g.recordCount), float64(g.zipfConstant)) // new zipf generator from TurboDB
	case "uniform":
		requestGen, err = NewUniformGenerator(requestGenRng, 0, uint64(g.recordCount)-1)
	default:
		return workload.QueryLoad{}, errors.Errorf("Unknown request distribution: %s", g.requestDistribution)
	}
	if err != nil {
		return workload.QueryLoad{}, err
	}

	cfg := workload.NewMultiConnPoolCfgFromFlags(g.connFlags)
	pool, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}

	const (
		readParallelStmt             stmtKey = "readParallel"
		readMultishotStmt            stmtKey = "readMultishot"
		scanStmt                     stmtKey = "scan"
		insertStmt                   stmtKey = "insert"
		readFieldForUpdateStmtFormat         = "readFieldForUpdate%d"
		updateParallelStmtFormat             = "updateParallel%d"
		updateMultishotStmtFormat            = "updateMultishot%d"
	)
	pool.AddPreparedStatement(readParallelStmt, readParallelStmtStr)
	pool.AddPreparedStatement(readMultishotStmt, readMultishotStmtStr)

	readFieldForUpdateStmts := make([]stmtKey, 1)
	key := fmt.Sprintf(readFieldForUpdateStmtFormat, 0)
	pool.AddPreparedStatement(key, readFieldForUpdateStmtStr)
	readFieldForUpdateStmts[0] = key

	pool.AddPreparedStatement(insertStmt, insertStmtStr)

	updateParallelStmts := make([]stmtKey, 1)
	key = fmt.Sprintf(updateParallelStmtFormat, 0)
	pool.AddPreparedStatement(key, updateParallelStmtStr)
	updateParallelStmts[0] = key

	updateMultishotStmts := make([]stmtKey, 1)
	key = fmt.Sprintf(updateMultishotStmtFormat, 0)
	pool.AddPreparedStatement(key, updateMultishotStmtStr)
	updateMultishotStmts[0] = key

	for i := 0; i < g.connFlags.Concurrency; i++ {
		// We want to have 1 connection per worker, however the
		// multi-connection pool round robins access to different pools so it
		// is always possible that we hit a pool that is full, unless we create
		// more connections than necessary. To avoid this, first check if the
		// pool we are attempting to acquire a connection from is full. If
		// full, skip this pool and continue to the next one, otherwise grab
		// the connection and move on.
		var pl *pgxpool.Pool
		var try int
		for try = 0; try < g.connFlags.Concurrency; try++ {
			pl = pool.Get()
			plStat := pl.Stat()
			if plStat.MaxConns()-plStat.AcquiredConns() > 0 {
				break
			}
		}
		if try == g.connFlags.Concurrency {
			return workload.QueryLoad{},
				errors.AssertionFailedf("Unable to acquire connection for worker %d", i)
		}

		conn, err := pl.Acquire(ctx)
		if err != nil {
			return workload.QueryLoad{}, err
		}

		rng := rand.New(rand.NewSource(RandomSeed.Seed() + uint64(i)))
		w := &ycsbtWorker{
			config:                  g,
			hists:                   reg.GetHandle(),
			conn:                    conn,
			readParallelStmt:        readParallelStmt,
			readMultishotStmt:       readMultishotStmt,
			readFieldForUpdateStmts: readFieldForUpdateStmts,
			scanStmt:                scanStmt,
			insertStmt:              insertStmt,
			updateParallelStmts:     updateParallelStmts,
			updateMultishotStmts:    updateMultishotStmts,
			rowIndex:                rowIndex,
			rowCounter:              rowCounter,
			nextInsertIndex:         nil,
			requestGen:              requestGen,
			rng:                     rng,
			hashFunc:                fnv.New64(),
		}
		ql.WorkerFns = append(ql.WorkerFns, w.run)
	}
	return ql, nil
}

type randGenerator interface {
	Uint64() uint64
	IncrementIMax(count uint64) error
}

type stmtKey = string

type ycsbtWorker struct {
	config *ycsbt
	hists  *histogram.Histograms
	conn   *pgxpool.Conn
	// Statement to read all the fields of a row. Used for read requests.
	readParallelStmt  stmtKey
	readMultishotStmt stmtKey
	// Statements to read a specific field of a row in preparation for
	// updating it. Used for read-modify-write requests.
	readFieldForUpdateStmts []stmtKey
	scanStmt, insertStmt    stmtKey
	// In normal mode this is one statement per field, since the field name
	// cannot be parametrized. In JSON mode it's a single statement.
	updateParallelStmts  []stmtKey
	updateMultishotStmts []stmtKey

	// The next row index to insert.
	rowIndex *uint64
	// Counter to keep track of which rows have been inserted.
	rowCounter *AcknowledgedCounter
	// Next insert index to use if non-nil.
	nextInsertIndex *uint64

	requestGen randGenerator // used to generate random keys for requests
	rng        *rand.Rand    // used to generate random strings for the values
	hashFunc   hash.Hash64
	hashBuf    [8]byte
}

func (yw *ycsbtWorker) run(ctx context.Context) error {
	start := timeutil.Now()
	var err error
	var op operation
	if yw.config.shot == TwoShot {
		op = twoShotTx
		err = yw.readWriteTx(ctx)
	} else if yw.config.shot == BatchRW {
		op = oneShotBatchTx
		err = yw.readWriteTx(ctx)
	} else {
		op = yw.chooseOp()
		switch op {
		case writeTx:
			switch yw.config.shot {
			case OneShot:
				err = yw.updateRowsTxParallel(ctx)
			case MultiShot:
				err = yw.updateRowsTxMultiShot(ctx)
			default:
				return errors.Errorf(`invalid parallel config: %s`, yw.config.shot)
			}
		case readTx:
			switch yw.config.shot {
			case OneShot:
				err = yw.readRowsTxParallel(ctx)
			case MultiShot:
				err = yw.readRowsTxMultishot(ctx)
			default:
				return errors.Errorf(`invalid parallel config: %s`, yw.config.shot)
			}
		case readModifyWriteTx:
			switch yw.config.shot {
			case OneShot:
			case MultiShot:
				err = yw.readModifyWriteRowsTx(ctx)
			default:
				return errors.Errorf(`invalid parallel config: %s`, yw.config.shot)
			}
		default:
			return errors.Errorf(`unknown operation: %s`, op)
		}
	}
	if err != nil {
		return err
	}
	elapsed := timeutil.Since(start)
	yw.hists.Get(string(op)).Record(elapsed)
	return nil
}

type operation string

const (
	writeTx           operation = `update`
	insertOp          operation = `insert`
	readTx            operation = `read`
	readModifyWriteTx operation = `readModifyWrite`
	twoShotTx         operation = `twoShotTx`
	oneShotBatchTx    operation = `oneShotBatchTx`
)

func (yw *ycsbtWorker) hashKey(key uint64) uint64 {
	yw.hashBuf = [8]byte{} // clear hashBuf
	binary.PutUvarint(yw.hashBuf[:], key)
	yw.hashFunc.Reset()
	if _, err := yw.hashFunc.Write(yw.hashBuf[:]); err != nil {
		panic(err)
	}
	return yw.hashFunc.Sum64()
}

func (yw *ycsbtWorker) buildKeyName(keynum uint64) string {
	if yw.config.insertHash {
		return keyNameFromHash(yw.hashKey(keynum))
	}
	return keyNameFromOrder(keynum, yw.config.zeroPadding)
}

func keyNameFromHash(hashedKey uint64) string {
	return fmt.Sprintf("user%d", hashedKey)
}

func keyNameFromOrder(keynum uint64, zeroPadding int) string {
	return fmt.Sprintf("user%0*d", zeroPadding, keynum)
}

// Keys are chosen by first drawing from a Zipf distribution, hashing the drawn
// value, and modding by the total number of rows, so that not all hot keys are
// close together.
// See YCSB paper section 5.3 for a complete description of how keys are chosen.
func (yw *ycsbtWorker) nextReadKey() string {
	rowCount := yw.rowCounter.Last()
	// TODO(jeffreyxiao): The official YCSB implementation creates a very large
	// key space for the zipfian distribution, hashes, mods it by the number of
	// expected number of keys at the end of the workload to obtain the key index
	// to read. The key index is then hashed again to obtain the key. If the
	// generated key index is greater than the number of acknowledged rows, then
	// the key is regenerated. Unfortunately, we cannot match this implementation
	// since we run YCSB for a set amount of time rather than number of
	// operations, so we don't know the expected number of keys at the end of the
	// workload. Instead we directly use the zipfian generator to generate our
	// key indexes by modding it by the actual number of rows. We don't hash so
	// the hotspot is consistent and randomly distributed in the key space like
	// with the official implementation. However, unlike the official
	// implementation, this causes the hotspot to not be random w.r.t the
	// insertion order of the keys (the hottest key is always the first inserted
	// key). The distribution is also slightly different than the official YCSB's
	// distribution, so it might be worthwhile to exactly emulate what they're
	// doing.
	rowIndex := yw.requestGen.Uint64() % rowCount
	return yw.buildKeyName(rowIndex)
}

func (yw *ycsbtWorker) nextInsertKeyIndex() uint64 {
	if yw.nextInsertIndex != nil {
		result := *yw.nextInsertIndex
		yw.nextInsertIndex = nil
		return result
	}
	return atomic.AddUint64(yw.rowIndex, 1) - 1
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// Generate a random string of alphabetic characters.
func (yw *ycsbtWorker) randString(length int) string {
	str := make([]byte, length)
	// prepend current timestamp matching the default CRDB UTC time format
	strStart := 0
	if yw.config.timeString {
		currentTime := timeutil.Now().UTC()
		str = currentTime.AppendFormat(str[:0], timeFormatTemplate)
		strStart = len(str)
		str = str[:length]
	}
	// the rest of data is random str
	for i := strStart; i < length; i++ {
		str[i] = letters[yw.rng.Intn(len(letters))]
	}
	return string(str)
}

// NOTE: The following is intentionally duplicated with the ones in
// workload/tpcc/generate.go. They're a very hot path in restoring a fixture and
// hardcoding the consts seems to trigger some compiler optimizations that don't
// happen if those things are params. Don't modify these without consulting
// BenchmarkRandStringFast.

func randStringLetters(rng rand.Source, buf []byte) {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const lettersLen = uint64(len(letters))
	const lettersCharsPerRand = uint64(11) // floor(log(math.MaxUint64)/log(lettersLen))

	var r, charsLeft uint64
	for i := 0; i < len(buf); i++ {
		if charsLeft == 0 {
			r = rng.Uint64()
			charsLeft = lettersCharsPerRand
		}
		buf[i] = letters[r%lettersLen]
		r = r / lettersLen
		charsLeft--
	}
}

func (yw *ycsbtWorker) insertRow(ctx context.Context) error {
	var args [numTableFields + 1]interface{}
	keyIndex := yw.nextInsertKeyIndex()
	args[0] = yw.buildKeyName(keyIndex)
	for i := 1; i <= numTableFields; i++ {
		args[i] = yw.randString(fieldLength)
	}
	if _, err := yw.conn.Exec(ctx, yw.insertStmt, args[:]...); err != nil {
		yw.nextInsertIndex = new(uint64)
		*yw.nextInsertIndex = keyIndex
		return err
	}

	count, err := yw.rowCounter.Acknowledge(keyIndex)
	if err != nil {
		return err
	}
	return yw.requestGen.IncrementIMax(count)
}

func (yw *ycsbtWorker) getDistinctKeysForTx(tx_size int) []string {
	key_map := make(map[string]bool)
	for len(key_map) < tx_size {
		key := yw.nextReadKey()
		if _, ok := key_map[key]; !ok {
			key_map[key] = true
		}
	}
	keys := make([]string, 0, tx_size)
	for key := range key_map {
		keys = append(keys, key)
	}
	return keys
}

func (yw *ycsbtWorker) determinReadWriteSizes(txSize int) (readSize int, writeSize int) {
	readCount := 0
	writeCount := 0
	for i := 0; i < txSize; i++ {
		p := yw.rng.Float32()
		if p <= yw.config.writeFraction {
			writeCount++
		} else {
			readCount++
		}
	}
	if readCount+writeCount != txSize {
		fmt.Printf("Fatal. readCount (%d) + writeCount (%d) != txSize (%d).\n", readCount, writeCount, txSize)
	}
	return readCount, writeCount
}

func (yw *ycsbtWorker) prepareReadStmt(readSize int) string {
	statement := `SELECT * FROM usertable WHERE ycsb_key IN (`
	for i := 0; i < readSize; i++ {
		if i > 0 {
			statement = fmt.Sprintf(`%s, `, statement)
		}
		statement = fmt.Sprintf(`%s$%d`, statement, i+1)
	}
	statement = fmt.Sprintf(`%s)`, statement)
	return statement
}

func (yw *ycsbtWorker) prepareWriteStmt(writeSize int) string {
	statement := `UPDATE usertable SET 
		field0 = $1, 
		field1 = $1,
		field2 = $1,
		field3 = $1,
		field4 = $1,
		field5 = $1,
		field6 = $1,
		field7 = $1,
		field8 = $1,
		field9 = $1
		WHERE ycsb_key IN (`
	for i := 0; i < writeSize; i++ {
		if i > 0 {
			statement = fmt.Sprintf(`%s, `, statement)
		}
		statement = fmt.Sprintf(`%s$%d`, statement, i+2)
	}
	statement = fmt.Sprintf(`%s)`, statement)
	return statement
}

func (yw *ycsbtWorker) oneShotBatchReadWriteTx(ctx context.Context, read_stmt string, readArgs []interface{}, write_stmt string, writeArgs []interface{}) error {
	txOptions := pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	}
	err := crdbpgx.ExecuteTx(ctx, yw.conn, txOptions, func(tx pgx.Tx) error {
		batch := &pgx.Batch{}
		batch.Queue(read_stmt, readArgs[:]...)
		batch.Queue(write_stmt, writeArgs[:]...)

		br := tx.SendBatch(ctx, batch) // send the batch of all statements in one round

		defer func() {
			if e := br.Close(); e != nil {
				fmt.Printf("Batch close error: %s.\n", e)
			}
		}()

		// extract read results
		rows, e := br.Query()
		if e != nil {
			fmt.Printf("oneShotBatchTx read part failed. error = %s. stmt = %s. args = %s.\n", e, read_stmt, readArgs)
			return e
		}
		defer rows.Close()
		rowCount := 0
		for rows.Next() {
			rowCount++
		}
		if rowCount != len(readArgs) && rows.Err() == nil {
			fmt.Printf("read tx 2 shot failed. Some rows are empty. res.Err = %s. stmt = %s. args = %s.\n", rows.Err(), read_stmt, readArgs)
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		// extract write results
		if _, er := br.Exec(); er != nil {
			return er
		}
		return nil
	})
	return err
}

func (yw *ycsbtWorker) twoShotReadWriteTx(ctx context.Context, read_stmt string, readArgs []interface{}, write_stmt string, writeArgs []interface{}) error {
	txOptions := pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	}
	err := crdbpgx.ExecuteTx(ctx, yw.conn, txOptions, func(tx pgx.Tx) error {
		// do reads first
		// res, er := yw.conn.Query(ctx, read_stmt, readArgs[:]...)
		res, er := tx.Query(ctx, read_stmt, readArgs[:]...)
		if er != nil {
			fmt.Printf("read tx 2shot failed. error = %s. stmt = %s. args = %s.\n", er, read_stmt, readArgs)
			return er
		}
		defer res.Close()
		rowCount := 0
		for res.Next() {
			rowCount++
		}
		if rowCount != len(readArgs) && res.Err() == nil {
			fmt.Printf("read tx 2 shot failed. Some rows are empty. res.Err = %s. stmt = %s. args = %s.\n", res.Err(), read_stmt, readArgs)
		}
		if res.Err() != nil {
			return res.Err()
		}
		// do writes
		if _, e := tx.Exec(ctx, write_stmt, writeArgs[:]...); e != nil {
			fmt.Printf("write tx 2 shot failed. error = %s. stmt = %s. args = %s.\n", e, write_stmt, writeArgs)
			return e
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) && ctx.Err() != nil {
		// Sometimes a context cancellation during a transaction can result in
		// sql.ErrNoRows instead of the appropriate context.DeadlineExceeded. In
		// this case, we just return ctx.Err(). See
		// https://github.com/lib/pq/issues/874.
		return ctx.Err()
	}
	return err
}

func (yw *ycsbtWorker) readWriteTx(ctx context.Context) error {
	readSize, writeSize := yw.determinReadWriteSizes(yw.config.txSize)
	if readSize == 0 {
		return yw.updateRowsTxParallel(ctx)
	}
	if writeSize == 0 {
		return yw.readRowsTxParallel(ctx)
	}
	// prepare read and write statements and arguments
	read_stmt := yw.prepareReadStmt(readSize)
	write_stmt := yw.prepareWriteStmt(writeSize)
	keys := yw.getDistinctKeysForTx(yw.config.txSize)
	readKeys := keys[:readSize]
	writeKeys := keys[readSize:]
	value := yw.randString(fieldLength)
	readArgs := make([]interface{}, 0, readSize)
	for _, readKey := range readKeys {
		readArgs = append(readArgs, interface{}(readKey))
	}
	writeArgs := make([]interface{}, 0, writeSize+1) // will have txSize + 1 (for value) elements
	if yw.config.json {
		return errors.Errorf(`json is not supported.`)
	} else {
		writeArgs = append(writeArgs, value)
	}
	for _, writeKey := range writeKeys {
		writeArgs = append(writeArgs, interface{}(writeKey))
	}
	switch yw.config.shot {
	case BatchRW:
		return yw.oneShotBatchReadWriteTx(ctx, read_stmt, readArgs, write_stmt, writeArgs)
	case TwoShot:
		return yw.twoShotReadWriteTx(ctx, read_stmt, readArgs, write_stmt, writeArgs)
	default:
		return errors.Errorf("Invalide shot config. Should be either BatchRW or TwoShot")
	}
}

func (yw *ycsbtWorker) updateRowsTxParallel(ctx context.Context) error {
	keys := yw.getDistinctKeysForTx(yw.config.txSize)
	value := yw.randString(fieldLength)
	stmt := yw.updateParallelStmts[0]
	num_args := yw.config.txSize + 1         // n keys + a value
	args := make([]interface{}, 0, num_args) // will have txSize + 1 (for value) elements
	if yw.config.json {
		return errors.Errorf(`json is not supported.`)
	} else {
		args = append(args, value)
	}
	for _, key := range keys {
		args = append(args, interface{}(key))
	}
	txOptions := pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	}
	err := crdbpgx.ExecuteTx(ctx, yw.conn, txOptions, func(tx pgx.Tx) error {
		// TODO: Do we really need this ExecuteTx wrapper?
		if _, e := tx.Exec(ctx, stmt, args[:]...); e != nil {
			fmt.Printf("write tx parallel failed. error = %s. stmt = %s. args = %s.\n", e, stmt, args)
			return e
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) && ctx.Err() != nil {
		// TODO: Do we need this?
		// Sometimes a context cancellation during a transaction can result in
		// sql.ErrNoRows instead of the appropriate context.DeadlineExceeded. In
		// this case, we just return ctx.Err(). See
		// https://github.com/lib/pq/issues/874.
		return ctx.Err()
	}
	return err
}

func (yw *ycsbtWorker) updateRowsTxMultiShot(ctx context.Context) error {
	keys := yw.getDistinctKeysForTx(yw.config.txSize)
	value := yw.randString(fieldLength)
	write_stmt := yw.updateMultishotStmts[0]
	write_args_base := make([]interface{}, 0, 2)
	if yw.config.json {
		return errors.Errorf(`json is not supported.`)
	} else {
		write_args_base = append(write_args_base, value)
	}
	txOptions := pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	}
	err := crdbpgx.ExecuteTx(ctx, yw.conn, txOptions, func(tx pgx.Tx) error {
		// TODO: Do we really need this ExecuteTx wrapper?
		for _, key := range keys {
			write_args := append(write_args_base, interface{}(key))
			if _, er := tx.Exec(ctx, write_stmt, write_args[:]...); er != nil {
				fmt.Printf("write tx multishot failed. error = %s. stmt = %s. args = %s.\n", er, write_stmt, write_args)
				return er
			}
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) && ctx.Err() != nil {
		// TODO: Do we need this?
		// Sometimes a context cancellation during a transaction can result in
		// sql.ErrNoRows instead of the appropriate context.DeadlineExceeded. In
		// this case, we just return ctx.Err(). See
		// https://github.com/lib/pq/issues/874.
		return ctx.Err()
	}
	return err
}

func (yw *ycsbtWorker) readRowsTxParallel(ctx context.Context) error {
	keys := yw.getDistinctKeysForTx(yw.config.txSize)
	args := make([]interface{}, 0, yw.config.txSize)
	for _, key := range keys {
		args = append(args, interface{}(key))
	}
	txOptions := pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	}
	err := crdbpgx.ExecuteTx(ctx, yw.conn, txOptions, func(tx pgx.Tx) error {
		// TODO: Do we really need this ExecuteTx wrapper?
		// res, er := yw.conn.Query(ctx, yw.readParallelStmt, args[:]...)
		res, er := tx.Query(ctx, yw.readParallelStmt, args[:]...)
		if er != nil {
			fmt.Printf("read tx parallel failed. error = %s. stmt = %s. args = %s.\n", er, yw.readParallelStmt, args)
			return er
		}
		defer res.Close()
		rowCount := 0
		for res.Next() {
			rowCount++
		}
		if rowCount != len(keys) && res.Err() == nil {
			fmt.Printf("read tx parallel failed. Some rows are empty. res.Err = %s. stmt = %s. args = %s.\n", res.Err(), yw.readParallelStmt, args)
		}
		return res.Err()
	})
	if errors.Is(err, pgx.ErrNoRows) && ctx.Err() != nil {
		// TODO: Do we need this?
		// Sometimes a context cancellation during a transaction can result in
		// sql.ErrNoRows instead of the appropriate context.DeadlineExceeded. In
		// this case, we just return ctx.Err(). See
		// https://github.com/lib/pq/issues/874.
		return ctx.Err()
	}
	return err
}

func (yw *ycsbtWorker) readRowsTxMultishot(ctx context.Context) error {
	keys := yw.getDistinctKeysForTx(yw.config.txSize)
	txOptions := pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	}
	err := crdbpgx.ExecuteTx(ctx, yw.conn, txOptions, func(tx pgx.Tx) error {
		// TODO: Do we really need this ExecuteTx wrapper?
		for _, key := range keys {
			row, e := tx.Query(ctx, yw.readMultishotStmt, interface{}(key))
			if e != nil {
				fmt.Printf("read tx multishot failed. error = %s. stmt = %s. args = %s.\n", e, yw.readMultishotStmt, key)
				return e
			}
			defer row.Close()
			for row.Next() {
			}
			if row.Err() != nil {
				return row.Err()
			}
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) && ctx.Err() != nil {
		// TODO: Do we need this?
		// Sometimes a context cancellation during a transaction can result in
		// sql.ErrNoRows instead of the appropriate context.DeadlineExceeded. In
		// this case, we just return ctx.Err(). See
		// https://github.com/lib/pq/issues/874.
		return ctx.Err()
	}
	return err
}

// execute is like crdb.Execute from cockroach-go, but for pgx. This function
// should ultimately be moved to crdbpgx.
//
// TODO(ajwerner): Move this function to crdbpgx and adopt that.
func execute(fn func() error) error {
	for {
		err := fn()
		if err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) &&
			pgcode.MakeCode(pgErr.Code) == pgcode.SerializationFailure {
			continue
		}
		return err
	}
}

func (yw *ycsbtWorker) readModifyWriteRowsTx(ctx context.Context) error {
	keys := yw.getDistinctKeysForTx(yw.config.txSize)
	newValue := yw.randString(fieldLength)
	read_stmt := yw.readFieldForUpdateStmts[0]
	write_stmt := yw.updateMultishotStmts[0]
	write_args_base := make([]interface{}, 0, 2) // will have txSize + 1 (for value) elements
	if yw.config.json {
		return errors.Errorf(`json is not supported.`)
	} else {
		write_args_base = append(write_args_base, newValue)
	}
	txOptions := pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	}
	err := crdbpgx.ExecuteTx(ctx, yw.conn, txOptions, func(tx pgx.Tx) error {
		// r-m-w reads one field from a row and then updates all fields of this row
		var oldValue []byte
		for _, key := range keys { // same as txSize
			if e := tx.QueryRow(ctx, read_stmt, interface{}(key)).Scan(&oldValue); e != nil {
				fmt.Printf("read part failed. error = %s. read_statment: %s; read_args: %s\n", e, read_stmt, key)
				return e
			}
			// TODO: We should make new values based on returned rows as for r-m-w logic,
			// but we don't for simplicity
			write_args := append(write_args_base, interface{}(key))
			if _, er := tx.Exec(ctx, write_stmt, write_args[:]...); er != nil {
				fmt.Printf("write part failed. error = %s. write_statment: %s; write_args: %s\n", er, write_stmt, write_args)
				return er
			}
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) && ctx.Err() != nil {
		// Sometimes a context cancellation during a transaction can result in
		// sql.ErrNoRows instead of the appropriate context.DeadlineExceeded. In
		// this case, we just return ctx.Err(). See
		// https://github.com/lib/pq/issues/874.
		return ctx.Err()
	}
	return err
}

// Choose an operation in proportion to the frequencies.
func (yw *ycsbtWorker) chooseOp() operation {
	p := yw.rng.Float32()
	if p <= yw.config.writeFraction {
		// do write transaction, could be updates or rmws
		q := yw.rng.Float32()
		if q <= yw.config.rmwRatio {
			// do rmws
			return readModifyWriteTx
		} else {
			// do updates
			return writeTx
		}
	} else {
		// this should be a read tx
		return readTx
	}
}
