package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/profile"
	"github.com/scylladb/scylla-go-driver/gocql"
)

const insertStmt = "INSERT INTO benchks.benchtab (pk, v1, v2) VALUES(?, ?, ?)"
const selectStmt = "SELECT v1, v2 FROM benchks.benchtab WHERE pk = ?"
const samples = 20_000

func main() {
	config := readConfig()
	log.Printf("Benchmark configuration: %#v\n", config)

	if config.profileCPU && config.profileMem {
		log.Fatal("select one profile type")
	}
	if config.profileCPU {
		log.Println("Running with CPU profiling")
		defer profile.Start(profile.CPUProfile).Stop()
	}
	if config.profileMem {
		log.Println("Running with memory profiling")
		defer profile.Start(profile.MemProfile).Stop()
	}
	cluster := gocql.NewCluster(config.nodeAddresses[:]...)
	cluster.Timeout = 30 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	if !config.dontPrepare {
		prepareKeyspaceAndTable(session)
	}

	if config.workload == Selects && !config.dontPrepare {
		prepareSelectsBenchmark(session, config)
	}

	var wg sync.WaitGroup
	nextBatchStart := int64(0)

	log.Println("Starting the benchmark")

	startTime := time.Now()

	selectCh := make(chan time.Duration, 2*samples)
	insertCh := make(chan time.Duration, 2*samples)
	for i := int64(0); i < config.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			insertQ := session.Query(insertStmt)
			selectQ := session.Query(selectStmt)

			for {
				curBatchStart := atomic.AddInt64(&nextBatchStart, config.batchSize)
				if curBatchStart >= config.tasks {
					// no more work to do
					break
				}

				curBatchEnd := min(curBatchStart+config.batchSize, config.tasks)

				for pk := curBatchStart; pk < curBatchEnd; pk++ {
					sample := false
					var startTime time.Time
					if rand.Int63n(config.tasks) < samples {
						sample = true
					}
					if config.workload == Inserts || config.workload == Mixed {
						if sample {
							startTime = time.Now()
						}
						err := insertQ.Bind(pk, 2*pk, 3*pk).Exec()
						if err != nil {
							panic(err)
						}
						if sample {
							insertCh <- time.Now().Sub(startTime)
						}
					}

					if config.workload == Selects || config.workload == Mixed {
						var v1, v2 int64
						if sample {
							startTime = time.Now()
						}
						err := selectQ.Bind(pk).Scan(&v1, &v2)
						if err != nil {
							panic(err)
						}

						if v1 != 2*pk || v2 != 3*pk {
							panic("bad data")
						}

						if sample {
							selectCh <- time.Now().Sub(startTime)
						}
					}
				}
			}
		}()
	}

	wg.Wait()
	benchTime := time.Now().Sub(startTime)

	fmt.Printf("time %d\n", benchTime.Milliseconds())
	printLatencyInfo("select", selectCh)
	printLatencyInfo("insert", insertCh)
	log.Printf("Finished\nBenchmark time: %d ms\n", benchTime.Milliseconds())
}

func printLatencyInfo(name string, ch chan time.Duration) {
	cnt := len(ch)
	for i := 0; i < cnt; i++ {
		fmt.Printf("%s %d\n", name, (<-ch).Nanoseconds())
	}
}

func awaitSchemaAgreement(session *gocql.Session) {
	// err := session.AwaitSchemaAgreement(context.Background())
	// if err != nil {
	// 	panic(err)
	// }
	time.Sleep(time.Second)
}

func prepareKeyspaceAndTable(session *gocql.Session) {
	err := session.Query("DROP KEYSPACE IF EXISTS benchks").Exec()
	if err != nil {
		panic(err)
	}
	awaitSchemaAgreement(session)

	err = session.Query("CREATE KEYSPACE IF NOT EXISTS benchks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}").Exec()
	if err != nil {
		panic(err)
	}
	awaitSchemaAgreement(session)

	err = session.Query("CREATE TABLE IF NOT EXISTS benchks.benchtab (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)").Exec()
	if err != nil {
		panic(err)
	}
	awaitSchemaAgreement(session)
}

func prepareSelectsBenchmark(session *gocql.Session, config Config) {
	log.Println("Preparing a selects benchmark (inserting values)...")

	var wg sync.WaitGroup
	nextBatchStart := int64(0)

	for i := int64(0); i < max(1024, config.concurrency); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			insertQ := session.Query(insertStmt)

			for {
				curBatchStart := atomic.AddInt64(&nextBatchStart, config.batchSize)
				if curBatchStart >= config.tasks {
					// no more work to do
					break
				}

				curBatchEnd := min(curBatchStart+config.batchSize, config.tasks)

				for pk := curBatchStart; pk < curBatchEnd; pk++ {
					err := insertQ.Bind(pk, 2*pk, 3*pk).Exec()
					if err != nil {
						panic(err)
					}
				}
			}
		}()
	}

	wg.Wait()
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}
