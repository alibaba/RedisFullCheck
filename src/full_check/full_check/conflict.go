package full_check

import (
	"encoding/json"
	"errors"
	"fmt"
	"full_check/client"
	"full_check/common"
	conf "full_check/configure"
	redigoredis "github.com/garyburd/redigo/redis"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

var (
	random int64 = -1
)

type keyInfo struct {
	Key          string `json:"k"`
	Type         string `json:"t"`
	ConflictType string `json:"ct"`
	Db           int32  `json:"db"`
	SourceLen    int64  `json:"sl"`
	TargetLen    int64  `json:"tl"`
}

type fieldInfo struct {
	Key          string `json:"k"`
	Field        string `json:"f"`
	ConflictType string `json:"ct"`
}

func (p *FullCheck) WriteConflictKey(conflictKey <-chan *common.Key) {
	if random == -1 {
		rand.Seed(time.Now().UnixNano())
		random = rand.Int63()
	}
	rc, err := client.NewRedisClient(p.TargetHost, 0)
	if err != nil {
		log.Fatal("unable to store conflict keys, because the target cluster is unreacheable")
		return
	}

	var resultfile *os.File
	if len(conf.Opts.ResultFile) > 0 {
		resultfile, _ = os.OpenFile(conf.Opts.ResultFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		defer resultfile.Close()
	}

	conflictKeyTableName, conflictFieldTableName := p.GetCurrentResultTable()

	keyList := fmt.Sprintf("fullcheck:%d:%s:key", random, conflictKeyTableName)
	c := 0
	for oneKeyInfo := range conflictKey {
		info := &keyInfo{
			Key:          string(oneKeyInfo.Key),
			Type:         oneKeyInfo.Tp.Name,
			ConflictType: oneKeyInfo.ConflictType.String(),
			Db:           p.currentDB,
			SourceLen:    oneKeyInfo.SourceAttr.ItemCount,
			TargetLen:    oneKeyInfo.TargetAttr.ItemCount,
		}
		infoJson, _ := json.Marshal(info)
		total := atomic.AddUint64(&(p.conflictBytesUsed), uint64(len(infoJson)))
		if total > uint64(conf.Opts.ResultBytesLimit) {
			panic(common.Logger.Errorf("too many conflicts!"))
		}
		_, err := rc.Do("RPUSH", keyList, string(infoJson))
		if err != nil {
			panic(common.Logger.Errorf("failed to exec rpush command: ", err))
		}
		if c == 0 {
			rc.Do("EXPIRE", keyList, 3600*4)
			c++
		}
		if len(oneKeyInfo.Field) != 0 {
			keyFields := []*fieldInfo{}
			for i := 0; i < len(oneKeyInfo.Field); i++ {
				keyFields = append(keyFields, &fieldInfo{
					Key:          info.Key,
					Field:        string(oneKeyInfo.Field[i].Field),
					ConflictType: oneKeyInfo.Field[i].ConflictType.String(),
				})
				if p.times == p.CompareCount {
					if len(conf.Opts.ResultFile) != 0 {
						resultfile.WriteString(fmt.Sprintf("%d\t%s\t%s\t%s\n", int(p.currentDB), oneKeyInfo.Field[i].ConflictType.String(), string(oneKeyInfo.Key), string(oneKeyInfo.Field[i].Field)))
					}
				}
			}
			fieldsList := fmt.Sprintf("fullcheck:%d:%s:key:%s:fields", random, conflictFieldTableName, info.Key)
			fieldsInfo, _ := json.Marshal(keyFields)
			atomic.AddUint64(&(p.conflictBytesUsed), uint64(len(fieldsInfo)))
			if total > uint64(conf.Opts.ResultBytesLimit) {
				panic(common.Logger.Errorf("too many conflicts!"))
			}
			_, err = rc.Do("SET", fieldsList, string(fieldsInfo), "EX", 3600*4)
		} else {
			if p.times == p.CompareCount {
				if len(conf.Opts.ResultFile) != 0 {
					resultfile.WriteString(fmt.Sprintf("%d\t%s\t%s\t%s\n", int(p.currentDB), oneKeyInfo.ConflictType.String(), string(oneKeyInfo.Key), ""))
				}
			}
		}
	}
}

func byteSlices(reply interface{}, err error) ([][]byte, error) {
	if err != nil {
		return nil, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		result := make([][]byte, len(reply))
		for i := range reply {
			if reply[i] == nil {
				continue
			}
			p, ok := reply[i].([]byte)
			if !ok {
				return nil, fmt.Errorf("redigo: unexpected element type for ByteSlices, got type %T", reply[i])
			}
			result[i] = p
		}
		return result, nil
	case []byte:
		return [][]byte{reply}, nil
	case nil:
		return nil, errors.New("ErrNil")
	case error:
		return nil, reply
	}
	return nil, fmt.Errorf("redigo: unexpected type for ByteSlices, got type %T", reply)
}

func (p *FullCheck) ScanFromDB(allkeys chan<- []*common.Key) {
	conflictKeyTableName, conflictFieldTableName := p.GetLastResultTable()
	keyList := fmt.Sprintf("fullcheck:%d:%s:key", random, conflictKeyTableName)
	rc, err := client.NewRedisClient(p.TargetHost, 0)
	if err != nil {
		log.Fatal("unable to scan conflict keys, because the target redis is unreacheable")
		return
	}

	keyInfoBatch := []*common.Key{}
	for {
		result, err := byteSlices(rc.Do("BLPOP", keyList, 1))
		if err == redigoredis.ErrNil || len(result) == 0 || len(result[0]) == 0 {
			if len(keyInfoBatch) > 0 {
				p.IncrScanStat(len(keyInfoBatch))
				allkeys <- keyInfoBatch
			}
			close(allkeys)
			rc.Do("DEL", keyList)
			break
		}
		if err != nil {
			panic(common.Logger.Errorf("failed to exec blpop command: ", err.Error()))
		}

		keyInfo := &keyInfo{}
		err = json.Unmarshal(result[1], keyInfo)
		if err != nil {
			panic(common.Logger.Errorf("failed to unmarshal data: ", err))
		}
		atomic.AddUint64(&(p.conflictBytesUsed), -uint64(len(result[1])))
		oneKeyInfo := &common.Key{
			Key:          []byte(keyInfo.Key),
			Tp:           common.NewKeyType(keyInfo.Type),
			ConflictType: common.NewConflictType(keyInfo.ConflictType),
			SourceAttr:   common.Attribute{ItemCount: keyInfo.SourceLen},
			TargetAttr:   common.Attribute{ItemCount: keyInfo.TargetLen},
		}
		if oneKeyInfo.Tp == common.EndKeyType {
			panic(common.Logger.Errorf("invalid type from redis %s: key=%s type=%s ", conflictKeyTableName, keyInfo.Key, keyInfo.Type))
		}
		if oneKeyInfo.ConflictType == common.EndConflict {
			panic(common.Logger.Errorf("invalid conflict_type from redis %s: key=%s conflict_type=%s ", conflictKeyTableName, keyInfo.Key, keyInfo.Type))
		}

		if oneKeyInfo.Tp != common.StringKeyType {
			fieldsListKey := fmt.Sprintf("fullcheck:%d:%s:key:%s:fields", random, conflictFieldTableName, oneKeyInfo.Key)
			fieldsBytes, err := redigoredis.Bytes(rc.Do("GET", fieldsListKey))
			if err != nil && err != redigoredis.ErrNil {
				panic(common.Logger.Errorf("failed to exec get command: ", err))
			}
			if err == nil {
				keyFields := []*fieldInfo{}
				err = json.Unmarshal(fieldsBytes, &keyFields)
				if err != nil {
					panic(common.Logger.Errorf("failed to unmarshal data: ", err))
				}
				for _, field := range keyFields {
					oneField := common.Field{
						Field:        []byte(field.Field),
						ConflictType: common.NewConflictType(field.ConflictType),
					}
					if oneField.ConflictType == common.EndConflict {
						panic(common.Logger.Errorf("invalid conflict_type from redis %s: field=%s type=%s ", conflictFieldTableName, field.Field, field.ConflictType))
					}
					oneKeyInfo.Field = append(oneKeyInfo.Field, oneField)
				}
				rc.Do("DEL", fieldsListKey)
				atomic.AddUint64(&(p.conflictBytesUsed), -uint64(len(fieldsBytes)))
			}
		}
		keyInfoBatch = append(keyInfoBatch, oneKeyInfo)
		if len(keyInfoBatch) == p.BatchCount {
			p.IncrScanStat(len(keyInfoBatch))
			allkeys <- keyInfoBatch
			keyInfoBatch = []*common.Key{}
		}
	}
}
