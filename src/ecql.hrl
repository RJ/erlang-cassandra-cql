-define(CQL_VERSION, <<"3.0.0">>).

-record(frame, {
        type = 0,
        version = 1,
        flags = 0,
        stream = 1,
        opcode,
        length,
        body = <<>>
    }).

-record(metadata, {
    flags = 0,
    numcols = 0,
    columns = [],
    global_keyspace,
    global_table
    }).

-define(short, 1/big-unsigned-unit:16).
-define(int,   1/big-signed-unit:32).
-define(int8,  1/big-signed-unit:8).

-define(CONSISTENCY_ANY,            0).
-define(CONSISTENCY_ONE,            1).
-define(CONSISTENCY_TWO,            2).
-define(CONSISTENCY_THREE,          3).
-define(CONSISTENCY_QUORUM,         4).
-define(CONSISTENCY_ALL,            5).
-define(CONSISTENCY_LOCAL_QUORUM,   6).
-define(CONSISTENCY_EACH_QUORUM,    7).

-define(OP_ERROR,       0).
-define(OP_STARTUP,     1).
-define(OP_READY,       2).
-define(OP_AUTHENTICATE,3).
-define(OP_CREDENTIALS, 4).
-define(OP_OPTIONS,     5).
-define(OP_SUPPORTED,   6).
-define(OP_QUERY,       7).
-define(OP_RESULT,      8).
-define(OP_PREPARE,     9).
-define(OP_EXECUTE,     10).
-define(OP_REGISTER,    11).
-define(OP_EVENT,       12).


-define(FLAG_COMPRESSION,   1).
-define(FLAG_TRACING,       2).


