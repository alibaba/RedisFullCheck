package conf

var Opts struct {
	SourceAddr         string `short:"s" long:"source" value-name:"SOURCE"  description:"Set host:port of source redis. If db type is cluster, split by semicolon(;'), e.g., 10.1.1.1:1000;10.2.2.2:2000;10.3.3.3:3000. We also support auto-detection, so \"master@10.1.1.1:1000\" or \"slave@10.1.1.1:1000\" means choose master or slave. Only need to give a role in the master or slave."`
	SourcePassword     string `short:"p" long:"sourcepassword" value-name:"Password" description:"Set source redis password"`
	SourceAuthType     string `long:"sourceauthtype" value-name:"AUTH-TYPE" default:"auth" description:"useless for opensource redis, valid value:auth/adminauth" `
	SourceDBType       int    `long:"sourcedbtype" default:"0" description:"0: db, 1: cluster 2: aliyun proxy, 3: tencent proxy"`
	SourceDBFilterList string `long:"sourcedbfilterlist" default:"-1" description:"db white list that need to be compared, -1 means fetch all, \"0;5;15\" means fetch db 0, 5, and 15"`
	TargetAddr         string `short:"t" long:"target" value-name:"TARGET"  description:"Set host:port of target redis. If db type is cluster, split by semicolon(;'), e.g., 10.1.1.1:1000;10.2.2.2:2000;10.3.3.3:3000. We also support auto-detection, so \"master@10.1.1.1:1000\" or \"slave@10.1.1.1:1000\" means choose master or slave. Only need to give a role in the master or slave."`
	TargetPassword     string `short:"a" long:"targetpassword" value-name:"Password" description:"Set target redis password"`
	TargetAuthType     string `long:"targetauthtype" value-name:"AUTH-TYPE" default:"auth" description:"useless for opensource redis, valid value:auth/adminauth" `
	TargetDBType       int    `long:"targetdbtype" default:"0" description:"0: db, 1: cluster 2: aliyun proxy 3: tencent proxy"`
	TargetDBFilterList string `long:"targetdbfilterlist" default:"-1" description:"db white list that need to be compared, -1 means fetch all, \"0;5;15\" means fetch db 0, 5, and 15"`
	ResultDBFile       string `short:"d" long:"db" value-name:"Sqlite3-DB-FILE" default:"result.db" description:"sqlite3 db file for store result. If exist, it will be removed and a new file is created."`
	ResultFile         string `long:"result" value-name:"FILE" description:"store all diff result into the file, format is 'db\tdiff-type\tkey\tfield'"`
	ResultBytesLimit   int    `long:"resultbyteslimit" value-NAME:"RESULT-BYTES-LIMIT" default:"10485760" description:"total bytes allowed to store the temp data in target redis"`
	CompareTimes       string `long:"comparetimes" value-name:"COUNT" default:"3" description:"Total compare count, at least 1. In the first round, all keys will be compared. The subsequent rounds of the comparison will be done on the previous results."`
	CompareMode        int    `short:"m" long:"comparemode" default:"2" description:"compare mode, 1: compare full value, 2: only compare value length, 3: only compare keys outline, 4: compare full value, but only compare value length when meets big key"`
	Id                 string `long:"id" default:"unknown" description:"used in metric, run id, useless for open source"`
	JobId              string `long:"jobid" default:"unknown" description:"used in metric, job id, useless for open source"`
	TaskId             string `long:"taskid" default:"unknown" description:"used in metric, task id, useless for open source"`
	Qps                int    `short:"q" long:"qps" default:"15000" description:"max batch qps limit: e.g., if qps is 10, full-check fetches 10 * $batch keys every second"`
	Interval           int    `long:"interval" value-name:"Second" default:"5" description:"The time interval for each round of comparison(Second)"`
	BatchCount         string `long:"batchcount" value-name:"COUNT" default:"256" description:"the count of key/field per batch compare, valid value [1, 10000]"`
	Parallel           int    `long:"parallel" value-name:"COUNT" default:"5" description:"concurrent goroutine number for comparison, valid value [1, 100]"`
	LogFile            string `long:"log" value-name:"FILE" description:"log file, if not specified, log is put to console"`
	LogLevel           string `long:"loglevel" value-name:"LEVEL" description:"log level: 'debug', 'info', 'warn', 'error', default is 'info'"`
	MetricPrint        bool   `long:"metric" value-name:"BOOL" description:"print metric in log"`
	BigKeyThreshold    int64  `long:"bigkeythreshold" value-name:"COUNT" default:"16384"`
	FilterList         string `short:"f" long:"filterlist" value-name:"FILTER" default:"" description:"if the filter list isn't empty, all elements in list will be synced. The input should be split by '|'. The end of the string is followed by a * to indicate a prefix match, otherwise it is a full match. e.g.: 'abc*|efg|m*' matches 'abc', 'abc1', 'efg', 'm', 'mxyz', but 'efgh', 'p' aren't'"`
	SystemProfile      uint   `long:"systemprofile" value-name:"SYSTEM-PROFILE" default:"20445" description:"port that used to print golang inner head and stack message"`
	Version            bool   `short:"v" long:"version"`
}
