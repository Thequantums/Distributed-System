package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Id int64
	Op string

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64

}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
// This data struct is needed for when a new server become a backup
type Fwddatatobackupargs struct {
	Data_arg map[string]string
	Request_records_arg map[int64]req_rec
}

type Fwdreply struct {
	Err Err
}
