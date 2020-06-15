package full_check

import (
	"strconv"

	"full_check/client"
	"full_check/common"

	"github.com/jinzhu/copier"
	"sync"
)

func (p *FullCheck) ScanFromSourceRedis(allKeys chan<- []*common.Key) {
	var wg sync.WaitGroup

	wg.Add(len(p.sourcePhysicalDBList))
	for idx := 0; idx < len(p.sourcePhysicalDBList); idx++ {
		// use goroutine to run db concurrently
		go func(index int) {
			defer wg.Done()
			cursor := 0
			var sourceClient client.RedisClient
			var err error

			// build client
			if p.SourceHost.IsCluster() {
				var singleHost client.RedisHost
				copier.Copy(&singleHost, &p.SourceHost)
				// set single host address
				singleHost.Addr = []string{singleHost.Addr[index]}
				singleHost.DBType = common.TypeDB
				// build client by single db
				if sourceClient, err = client.NewRedisClient(singleHost, p.currentDB); err != nil {
					panic(common.Logger.Critical(err))
				}
			} else {
				sourceClient, err = client.NewRedisClient(p.SourceHost, p.currentDB)
				if err != nil {
					panic(common.Logger.Errorf("create redis client with host[%v] db[%v] error[%v]",
						p.SourceHost, p.currentDB, err))
				}
			}
			defer sourceClient.Close()

			common.Logger.Infof("build connection[%v]", sourceClient.String())

			for {
				var reply interface{}
				var err error

				switch p.SourceHost.DBType {
				case common.TypeDB:
					fallthrough
				case common.TypeCluster:
					reply, err = sourceClient.Do("scan", cursor, "count", p.BatchCount)
				case common.TypeAliyunProxy:
					reply, err = sourceClient.Do("iscan", index, cursor, "count", p.BatchCount)
				case common.TypeTencentProxy:
					reply, err = sourceClient.Do("scan", cursor, "count", p.BatchCount, p.sourcePhysicalDBList[index])
				}
				if err != nil {
					panic(common.Logger.Critical(err))
				}

				replyList, ok := reply.([]interface{})
				if ok == false || len(replyList) != 2 {
					panic(common.Logger.Criticalf("scan %d count %d failed, result: %+v", cursor, p.BatchCount, reply))
				}

				bytes, ok := replyList[0].([]byte)
				if ok == false {
					panic(common.Logger.Criticalf("scan %d count %d failed, result: %+v", cursor, p.BatchCount, reply))
				}

				cursor, err = strconv.Atoi(string(bytes))
				if err != nil {
					panic(common.Logger.Critical(err))
				}

				keylist, ok := replyList[1].([]interface{})
				if ok == false {
					panic(common.Logger.Criticalf("scan failed, result: %+v", reply))
				}
				keysInfo := make([]*common.Key, 0, len(keylist))
				for _, value := range keylist {
					bytes, ok = value.([]byte)
					if ok == false {
						panic(common.Logger.Criticalf("scan failed, result: %+v", reply))
					}

					// check filter list
					if common.CheckFilter(p.FilterTree, bytes) == false {
						continue
					}

					keysInfo = append(keysInfo, &common.Key{
						Key:          bytes,
						Tp:           common.EndKeyType,
						ConflictType: common.EndConflict,
					})
					// common.Logger.Debugf("read key: %v", string(bytes))
				}
				p.IncrScanStat(len(keysInfo))
				allKeys <- keysInfo

				if cursor == 0 {
					break
				}
			} // end for{}
		}(idx)
	} // end fo for idx := 0; idx < p.sourcePhysicalDBList; idx++

	wg.Wait()
	close(allKeys)
}