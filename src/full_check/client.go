package main

import (
	"bytes"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	MaxRetryCount     = 20
	StatRollFrequency = 2
)

type RedisHost struct {
	addr      string
	password  string
	timeoutMs uint64
	role      string // "source" or "target"
	authtype  string // "auth" or "adminauth"
}

func (p RedisHost) String() string {
	return fmt.Sprintf("%s redis addr: %s", p.role, p.addr)
}

type RedisClient struct {
	redisHost RedisHost
	db        int32
	conn      redis.Conn
}

func (p RedisClient) String() string {
	return p.redisHost.String()
}

func NewRedisClient(redisHost RedisHost, db int32) (RedisClient, error) {
	rc := RedisClient{
		redisHost: redisHost,
		db:        db,
	}

	// send ping command first
	ret, err := rc.Do("ping")
	if err == nil && ret.(string) != "PONG" {
		return RedisClient{}, fmt.Errorf("ping return invaild[%v]", string(ret.([]byte)))
	}
	return rc, err
}

func (p *RedisClient) CheckHandleNetError(err error) bool {
	if err == io.EOF { // 对方断开网络
		if p.conn != nil {
			p.conn.Close()
			p.conn = nil
			// 网络相关错误1秒后重试
			time.Sleep(time.Second)
		}
		return true
	} else if _, ok := err.(net.Error); ok {
		if p.conn != nil {
			p.conn.Close()
			p.conn = nil
			// 网络相关错误1秒后重试
			time.Sleep(time.Second)
		}
		return true
	}
	return false
}

func (p *RedisClient) Connect() error {
	var err error
	if p.conn == nil {
		if p.redisHost.timeoutMs == 0 {
			p.conn, err = redis.Dial("tcp", p.redisHost.addr)
		} else {
			p.conn, err = redis.DialTimeout("tcp", p.redisHost.addr, time.Millisecond*time.Duration(p.redisHost.timeoutMs),
				time.Millisecond*time.Duration(p.redisHost.timeoutMs), time.Millisecond*time.Duration(p.redisHost.timeoutMs))
		}
		if err != nil {
			return err
		}
		if len(p.redisHost.password) != 0 {
			_, err = p.conn.Do(p.redisHost.authtype, p.redisHost.password)
			if err != nil {
				return err
			}
		}
		_, err = p.conn.Do("select", p.db)
		if err != nil {
			return err
		}
	} // p.conn == nil
	return nil
}

func (p *RedisClient) Do(commandName string, args ...interface{}) (interface{}, error) {
	var err error
	var result interface{}
	tryCount := 0
	for {
		if tryCount > MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					continue
				}
				return nil, err
			}
		}

		result, err = p.conn.Do(commandName, args...)
		if err != nil {
			if p.CheckHandleNetError(err) {
				continue
			}
			return nil, err
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) Close() {
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}

func (p *RedisClient) PipeTypeCommand(keyInfo []*Key) ([]string, error) {
	var err error
	result := make([]string, len(keyInfo))
	tryCount := 0
begin:
	for {
		if tryCount > MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, key := range keyInfo {
			err = p.conn.Send("type", key.key)
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(keyInfo); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
			result[i] = reply.(string)
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeExistsCommand(keyInfo []*Key) ([]int64, error) {
	var err error
	result := make([]int64, len(keyInfo))
	tryCount := 0
begin:
	for {
		if tryCount > MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, key := range keyInfo {
			err = p.conn.Send("exists", key.key)
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(keyInfo); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
			result[i] = reply.(int64)
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeLenCommand(keys []*Key) ([]int64, error) {
	var err error
	result := make([]int64, len(keys))
	tryCount := 0
begin:
	for {
		if tryCount > MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, key := range keys {
			err = p.conn.Send(key.tp.fetchLenCommand, key.key)
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(keys); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				if strings.HasPrefix(err.Error(), "WRONGTYPE") {
					result[i] = -1
				}
			} else {
				result[i] = reply.(int64)
			}
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeValueCommand(fetchValueKeyInfo []*Key) ([]interface{}, error) {
	var err error
	result := make([]interface{}, len(fetchValueKeyInfo))
	tryCount := 0
begin:
	for {
		if tryCount > MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, item := range fetchValueKeyInfo {
			switch item.tp {
			case StringType:
				err = p.conn.Send("get", item.key)
			case HashType:
				err = p.conn.Send("hgetall", item.key)
			case ListType:
				err = p.conn.Send("lrange", item.key, 0, -1)
			case SetType:
				err = p.conn.Send("smembers", item.key)
			case ZsetType:
				err = p.conn.Send("zrange", item.key, 0, -1, "WITHSCORES")
			default:
				err = p.conn.Send("get", item.key)
			}

			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(fetchValueKeyInfo); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
			result[i] = reply
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeSismemberCommand(key []byte, field [][]byte) ([]interface{}, error) {
	var err error
	result := make([]interface{}, len(field))
	tryCount := 0
begin:
	for {
		if tryCount > MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, item := range field {
			err = p.conn.Send("SISMEMBER", key, item)
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(field); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
			result[i] = reply
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeZscoreCommand(key []byte, field [][]byte) ([]interface{}, error) {
	var err error
	result := make([]interface{}, len(field))
	tryCount := 0
begin:
	for {
		if tryCount > MaxRetryCount {
			return nil, err
		}
		tryCount++

		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}

		for _, item := range field {
			err = p.conn.Send("ZSCORE", key, item)
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				break begin
			}
			return nil, err
		}

		for i := 0; i < len(field); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					break begin
				}
				return nil, err
			}
			result[i] = reply
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) FetchValueUseScan_Hash_Set_SortedSet(oneKeyInfo *Key, onceScanCount int) (map[string][]byte, error) {
	var scanCmd string
	switch oneKeyInfo.tp {
	case HashType:
		scanCmd = "hscan"
	case SetType:
		scanCmd = "sscan"
	case ZsetType:
		scanCmd = "zscan"
	default:
		return nil, fmt.Errorf("key type %s is not hash/set/zset", oneKeyInfo.tp)
	}
	cursor := 0
	value := make(map[string][]byte)
	for {
		reply, err := p.Do(scanCmd, oneKeyInfo.key, cursor, "count", onceScanCount)
		if err != nil {
			return nil, err
		}

		replyList, ok := reply.([]interface{})
		if ok == false || len(replyList) != 2 {
			return nil, fmt.Errorf("%s %s %d count %d failed, result: %+v", scanCmd, string(oneKeyInfo.key), cursor, onceScanCount, reply)
		}

		cursorBytes, ok := replyList[0].([]byte)
		if ok == false {
			return nil, fmt.Errorf("%s %s %d count %d failed, result: %+v", scanCmd, string(oneKeyInfo.key), cursor, onceScanCount, reply)
		}

		cursor, err = strconv.Atoi(string(cursorBytes))
		if err != nil {
			return nil, err
		}

		keylist, ok := replyList[1].([]interface{})
		if ok == false {
			panic(logger.Criticalf("%s %s failed, result: %+v", scanCmd, string(oneKeyInfo.key), reply))
		}
		switch oneKeyInfo.tp {
		case HashType:
			fallthrough
		case ZsetType:
			for i := 0; i < len(keylist); i += 2 {
				value[string(keylist[i].([]byte))] = keylist[i+1].([]byte)
			}
		case SetType:
			for i := 0; i < len(keylist); i++ {
				value[string(keylist[i].([]byte))] = nil
			}
		default:
			return nil, fmt.Errorf("key type %s is not hash/set/zset", oneKeyInfo.tp)
		}

		if cursor == 0 {
			break
		}
	} // end for{}
	return value, nil
}

func ParseKeyspace(content []byte) (map[int32]int64, error) {
	if bytes.HasPrefix(content, []byte("# Keyspace")) == false {
		return nil, fmt.Errorf("invalid info Keyspace: %s", string(content))
	}

	lines := bytes.Split(content, []byte("\n"))
	reply := make(map[int32]int64)
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("db")) == true {
			// line "db0:keys=18,expires=0,avg_ttl=0"
			items := bytes.Split(line, []byte(":"))
			db, err := strconv.Atoi(string(items[0][2:]))
			if err != nil {
				return nil, err
			}
			nums := bytes.Split(items[1], []byte(","))
			if bytes.HasPrefix(nums[0], []byte("keys=")) == false {
				return nil, fmt.Errorf("invalid info Keyspace: %s", string(content))
			}
			keysNum, err := strconv.ParseInt(string(nums[0][5:]), 10, 0)
			if err != nil {
				return nil, err
			}
			reply[int32(db)] = int64(keysNum)
		} // end true
	} // end for
	return reply, nil
}

// ParseInfo convert result of info command to map[string]string.
// For example, "opapply_source_count:1\r\nopapply_source_0:server_id=3171317,applied_opid=1\r\n" is converted to map[string]string{"opapply_source_count": "1", "opapply_source_0": "server_id=3171317,applied_opid=1"}.
func ParseInfo(content []byte) map[string]string {
	result := make(map[string]string, 10)
	lines := bytes.Split(content, []byte("\r\n"))
	for i := 0; i < len(lines); i++ {
		items := bytes.SplitN(lines[i], []byte(":"), 2)
		if len(items) != 2 {
			continue
		}
		result[string(items[0])] = string(items[1])
	}
	return result
}

func ValueHelper_Hash_SortedSet(reply interface{}) map[string][]byte {
	if reply == nil {
		return nil
	}

	tmpValue := reply.([]interface{})
	if len(tmpValue) == 0 {
		return nil
	}
	value := make(map[string][]byte)
	for i := 0; i < len(tmpValue); i += 2 {
		value[string(tmpValue[i].([]byte))] = tmpValue[i+1].([]byte)
	}
	return value
}

func ValueHelper_Set(reply interface{}) map[string][]byte {
	tmpValue := reply.([]interface{})
	if len(tmpValue) == 0 {
		return nil
	}
	value := make(map[string][]byte)
	for i := 0; i < len(tmpValue); i++ {
		value[string(tmpValue[i].([]byte))] = nil
	}
	return value
}

func ValueHelper_List(reply interface{}) [][]byte {
	tmpValue := reply.([]interface{})
	if len(tmpValue) == 0 {
		return nil
	}
	value := make([][]byte, len(tmpValue))
	for i := 0; i < len(tmpValue); i++ {
		value[i] = tmpValue[i].([]byte)
	}
	return value
}
