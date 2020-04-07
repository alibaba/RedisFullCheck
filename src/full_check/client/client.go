package client

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
	"errors"

	"full_check/common"

	"github.com/garyburd/redigo/redis"
	redigoCluster "github.com/vinllen/redis-go-cluster"
	"reflect"
)

var (
	emptyError = errors.New("empty")
)

type RedisHost struct {
	Addr         []string
	Password     string
	TimeoutMs    uint64
	Role         string // "source" or "target"
	Authtype     string // "auth" or "adminauth"
	DBType       int
	DBFilterList map[int]struct{} // whitelist
}

func (p RedisHost) String() string {
	return fmt.Sprintf("%s redis addr: %s", p.Role, p.Addr)
}

func (p RedisHost) IsCluster() bool {
	return p.DBType == common.TypeCluster
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
		return RedisClient{}, fmt.Errorf("ping return invaild[%v]", ret)
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
	if p.conn != nil {
		return nil
	}

	var err error
	if p.redisHost.IsCluster() == false {
		// single db or proxy
		if p.redisHost.TimeoutMs == 0 {
			p.conn, err = redis.Dial("tcp", p.redisHost.Addr[0])
		} else {
			p.conn, err = redis.DialTimeout("tcp", p.redisHost.Addr[0], time.Millisecond*time.Duration(p.redisHost.TimeoutMs),
				time.Millisecond*time.Duration(p.redisHost.TimeoutMs), time.Millisecond*time.Duration(p.redisHost.TimeoutMs))
		}
	} else {
		// cluster
		cluster, err := redigoCluster.NewCluster(
			&redigoCluster.Options{
				StartNodes:   p.redisHost.Addr,
				ConnTimeout:  time.Duration(p.redisHost.TimeoutMs) * time.Millisecond,
				ReadTimeout:  0,
				WriteTimeout: 0,
				KeepAlive:    16,
				AliveTime:    60 * time.Second,
				Password:     p.redisHost.Password,
			})
		if err == nil {
			p.conn = common.NewClusterConn(cluster, 0)
		}
	}
	if err != nil {
		return err
	}

	if len(p.redisHost.Password) != 0 {
		_, err = p.conn.Do(p.redisHost.Authtype, p.redisHost.Password)
		if err != nil {
			return err
		}
	}

	if p.redisHost.DBType != common.TypeCluster {
		_, err = p.conn.Do("select", p.db)
		if err != nil {
			return err
		}
	}

	if p.conn == nil {
		return fmt.Errorf("connect host[%v] failed: unknown", p.redisHost.Addr)
	}
	return nil
}

func (p *RedisClient) Do(commandName string, args ...interface{}) (interface{}, error) {
	var err error
	var result interface{}
	for tryCount := 0; tryCount < common.MaxRetryCount; tryCount++ {
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
	return result, err
}

func (p *RedisClient) Close() {
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}

type combine struct {
	command string
	params  []interface{}
}

func (c combine) String() string {
	all := make([]string, 0, len(c.params) + 1)
	all = append(all, c.command)
	for _, ele := range c.params {
		all = append(all, string(ele.([]byte)))
	}
	return strings.Join(all, " ")
}

func (p *RedisClient) PipeRawCommand(commands []combine, specialErrorPrefix string) ([]interface{}, error) {
	if len(commands) == 0 {
		common.Logger.Warnf("input commands length is 0")
		return nil, emptyError
	}

	result := make([]interface{}, len(commands))
	var err error
begin:
	for tryCount := 0; tryCount < common.MaxRetryCount; tryCount++ {
		if p.conn == nil {
			err = p.Connect()
			if err != nil {
				if p.CheckHandleNetError(err) {
					continue
				}
				common.Logger.Errorf("connect failed[%v]", err)
				return nil, err
			}
		}

		for _, ele := range commands {
			err = p.conn.Send(ele.command, ele.params...)
			if err != nil {
				if p.CheckHandleNetError(err) {
					continue begin
				}
				common.Logger.Errorf("send command[%v] failed[%v]", ele.command, err)
				return nil, err
			}
		}
		err = p.conn.Flush()
		if err != nil {
			if p.CheckHandleNetError(err) {
				continue
			}
			common.Logger.Errorf("flush failed[%v]", err)
			return nil, err
		}

		for i := 0; i < len(commands); i++ {
			reply, err := p.conn.Receive()
			if err != nil {
				if p.CheckHandleNetError(err) {
					continue begin
				}
				// 此处处理不太好，但是别人代码写死了，我只能这么改了
				if strings.HasPrefix(err.Error(), specialErrorPrefix) {
					// this error means the type between initial 'scan' and the following round comparison
					// is different. we should marks this.
					result[i] = common.TypeChanged
					continue
				}
				common.Logger.Errorf("receive command[%v] failed[%v]", commands[i], err)
				return nil, err
			}
			result[i] = reply
		}
		break
	} // end for {}
	return result, nil
}

func (p *RedisClient) PipeTypeCommand(keyInfo []*common.Key) ([]string, error) {
	commands := make([]combine, len(keyInfo))
	for i, key := range keyInfo {
		commands[i] = combine{
			command: "type",
			params:  []interface{}{key.Key},
		}
	}

	result := make([]string, len(keyInfo))
	if ret, err := p.PipeRawCommand(commands, ""); err != nil {
		if err != emptyError {
			common.Logger.Errorf("run PipeRawCommand with commands[%v] failed[%v]", commands, err)
			return nil, err
		}
	} else {
		for i, ele := range ret {
			if v, ok := ele.(string); ok {
				result[i] = v
			} else {
				err := fmt.Errorf("run PipeRawCommand with commands[%s] return element[%v] isn't type string[%v]",
					printCombinList(commands), ele, reflect.TypeOf(ele))
				common.Logger.Error(err)
				return nil, err
			}
		}
	}
	return result, nil
}

func (p *RedisClient) PipeExistsCommand(keyInfo []*common.Key) ([]int64, error) {
	commands := make([]combine, len(keyInfo))
	for i, key := range keyInfo {
		commands[i] = combine{
			command: "exists",
			params:  []interface{}{key.Key},
		}
	}

	result := make([]int64, len(keyInfo))
	if ret, err := p.PipeRawCommand(commands, ""); err != nil {
		if err != emptyError {
			return nil, err
		}
	} else {
		for i, ele := range ret {
			if v, ok := ele.(int64); ok {
				result[i] = v
			} else {
				err := fmt.Errorf("run PipeRawCommand with commands[%s] return element[%v] isn't type int64[%v]",
					printCombinList(commands), ele, reflect.TypeOf(ele))
				common.Logger.Error(err)
				return nil, err
			}
		}
	}
	return result, nil
}

func (p *RedisClient) PipeLenCommand(keyInfo []*common.Key) ([]int64, error) {
	commands := make([]combine, len(keyInfo))
	for i, key := range keyInfo {
		commands[i] = combine{
			command: key.Tp.FetchLenCommand,
			params:  []interface{}{key.Key},
		}
	}

	result := make([]int64, len(keyInfo))
	if ret, err := p.PipeRawCommand(commands, "WRONGTYPE"); err != nil {
		if err != emptyError {
			return nil, err
		}
	} else {
		for i, ele := range ret {
			if v, ok := ele.(int64); ok {
				result[i] = v
			} else {
				err := fmt.Errorf("run PipeRawCommand with commands[%s] return element[%v] isn't type int64[%v]",
					printCombinList(commands), ele, reflect.TypeOf(ele))
				common.Logger.Error(err)
				return nil, err
			}
		}
	}
	return result, nil
}

func (p *RedisClient) PipeTTLCommand(keyInfo []*common.Key) ([]bool, error) {
	commands := make([]combine, len(keyInfo))
	for i, key := range keyInfo {
		commands[i] = combine{
			command: "ttl",
			params:  []interface{}{key.Key},
		}
	}

	result := make([]bool, len(keyInfo))
	if ret, err := p.PipeRawCommand(commands, ""); err != nil {
		if err != emptyError {
			return nil, err
		}
	} else {
		for i, ele := range ret {
			if v, ok := ele.(int64); ok {
				result[i] = (v == 0 || v == -2)
			} else {
				err := fmt.Errorf("run PipeRawCommand with commands[%s] return element[%v] isn't type int64[%v]",
					printCombinList(commands), ele, reflect.TypeOf(ele))
				common.Logger.Error(err)
				return nil, err
			}
		}
	}
	return result, nil
}

func (p *RedisClient) PipeValueCommand(keyInfo []*common.Key) ([]interface{}, error) {
	commands := make([]combine, len(keyInfo))
	for i, key := range keyInfo {
		switch key.Tp {
		case common.StringKeyType:
			commands[i] = combine{
				command: "get",
				params:  []interface{}{key.Key},
			}
		case common.HashKeyType:
			commands[i] = combine{
				command: "hgetall",
				params:  []interface{}{key.Key},
			}
		case common.ListKeyType:
			commands[i] = combine{
				command: "lrange",
				params:  []interface{}{key.Key, "0", "-1"},
			}
		case common.SetKeyType:
			commands[i] = combine{
				command: "smembers",
				params:  []interface{}{key.Key},
			}
		case common.ZsetKeyType:
			commands[i] = combine{
				command: "zrange",
				params:  []interface{}{key.Key, "0", "-1", "WITHSCORES"},
			}
		default:
			commands[i] = combine{
				command: "get",
				params:  []interface{}{key.Key},
			}
		}
	}

	if ret, err := p.PipeRawCommand(commands, ""); err != nil && err != emptyError {
		return nil, err
	} else {
		return ret, nil
	}
}

func (p *RedisClient) PipeSismemberCommand(key []byte, field [][]byte) ([]interface{}, error) {
	commands := make([]combine, len(field))
	for i, ele := range field {
		commands[i] = combine{
			command: "SISMEMBER",
			params:  []interface{}{key, ele},
		}
	}

	if ret, err := p.PipeRawCommand(commands, ""); err != nil && err != emptyError {
		return nil, err
	} else {
		return ret, nil
	}
}

func (p *RedisClient) PipeZscoreCommand(key []byte, field [][]byte) ([]interface{}, error) {
	commands := make([]combine, len(field))
	for i, ele := range field {
		commands[i] = combine{
			command: "ZSCORE",
			params:  []interface{}{key, ele},
		}
	}

	if ret, err := p.PipeRawCommand(commands, ""); err != nil && err != emptyError {
		return nil, err
	} else {
		return ret, nil
	}
}

func (p *RedisClient) FetchValueUseScan_Hash_Set_SortedSet(oneKeyInfo *common.Key, onceScanCount int) (map[string][]byte, error) {
	var scanCmd string
	switch oneKeyInfo.Tp {
	case common.HashKeyType:
		scanCmd = "hscan"
	case common.SetKeyType:
		scanCmd = "sscan"
	case common.ZsetKeyType:
		scanCmd = "zscan"
	default:
		return nil, fmt.Errorf("key type %s is not hash/set/zset", oneKeyInfo.Tp)
	}
	cursor := 0
	value := make(map[string][]byte)
	for {
		reply, err := p.Do(scanCmd, oneKeyInfo.Key, cursor, "count", onceScanCount)
		if err != nil {
			return nil, err
		}

		replyList, ok := reply.([]interface{})
		if ok == false || len(replyList) != 2 {
			return nil, fmt.Errorf("%s %s %d count %d failed, result: %+v", scanCmd, string(oneKeyInfo.Key),
				cursor, onceScanCount, reply)
		}

		cursorBytes, ok := replyList[0].([]byte)
		if ok == false {
			return nil, fmt.Errorf("%s %s %d count %d failed, result: %+v", scanCmd, string(oneKeyInfo.Key),
				cursor, onceScanCount, reply)
		}

		cursor, err = strconv.Atoi(string(cursorBytes))
		if err != nil {
			return nil, err
		}

		keylist, ok := replyList[1].([]interface{})
		if ok == false {
			panic(common.Logger.Criticalf("%s %s failed, result: %+v", scanCmd, string(oneKeyInfo.Key), reply))
		}
		switch oneKeyInfo.Tp {
		case common.HashKeyType:
			fallthrough
		case common.ZsetKeyType:
			for i := 0; i < len(keylist); i += 2 {
				value[string(keylist[i].([]byte))] = keylist[i+1].([]byte)
			}
		case common.SetKeyType:
			for i := 0; i < len(keylist); i++ {
				value[string(keylist[i].([]byte))] = nil
			}
		default:
			return nil, fmt.Errorf("key type %s is not hash/set/zset", oneKeyInfo.Tp)
		}

		if cursor == 0 {
			break
		}
	} // end for{}
	return value, nil
}

func printCombinList(input []combine) string {
	ret := make([]string, 0, len(input))
	for _, ele := range input {
		ret = append(ret, ele.String())
	}
	return strings.Join(ret, "; ")
}
