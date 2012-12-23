-module(ecql_connection).
-behaviour(gen_fsm).
-include("ecql.hrl").

%% API
-export([start_link/0,
        q/2, q/3
    ]).

%% gen_fsm callbacks
-export([init/1,
         state_name/2,
         state_name/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-export([
    ready/3
    ]).

-define(SERVER, ?MODULE).
-define(DEFAULT_PORT, 9042).

-record(state, {
            host,
            port,
            sock,
            caller,
            buffer = <<>>
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
        gen_fsm:start_link({local, ?SERVER}, ?MODULE, ["localhost",?DEFAULT_PORT], []).

q(Pid, Query) -> q(Pid, Query, any).

q(Pid, Query, ConsistencyLevel) when is_pid(Pid) ->
    gen_fsm:sync_send_event(Pid, {q, Query, ConsistencyLevel}).


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([Host, Port]) ->
    {ok, Sock} = connect_sock(Host, Port),
    State = #state{ host=Host, port=Port, sock=Sock },
    %% send STARTUP frame:
    StartupFrame = #frame{
        opcode = ?OP_STARTUP,
        body = ecql_parser:encode_string_map([{<<"CQL_VERSION">>,?CQL_VERSION}])
    },
    sock_send(Sock, StartupFrame),
    {ok, connecting, State}.


connect_sock(Host,Port) ->
    Opts = [
        {active, true},
        {packet, raw},
        binary,
        {nodelay, true}
    ],
    gen_tcp:connect(Host, Port, Opts).

sock_send(Sock, F=#frame{}) ->
    io:format("sock_send: ~p\n",[F]),
    Enc = ecql_parser:encode(F),
    io:format("SEND: ~p\n",[iolist_to_binary(Enc)]),
    gen_tcp:send(Sock, Enc).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
state_name(_Event, State) ->
        {next_state, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------

ready({q, Query, Consistency}, From, State = #state{sock=Sock}) ->
    F = ecql_parser:make_query_frame(Query,Consistency),
    sock_send(Sock, F),
    NewState = State#state{caller=From},
    {next_state, awaiting_reply, NewState}.


state_name(_Event, _From, State) ->
        Reply = ok,
        {reply, Reply, state_name, State}.




%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
        {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
        Reply = ok,
        {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------

handle_info({tcp, Sock, Data}, St, State = #state{sock=Sock, buffer=Buffer}) ->
    case ecql_parser:read_frame(<< Buffer/binary, Data/binary >>) of
        {continue, NewBuf} ->
            %inet:set_opts(Sock, [{active, once}]),
            {next_state, St, State#state{buffer=NewBuf}};
        {F=#frame{}, NewBuf} ->
            io:format("got frame: ~p\n",[F]),
            %inet:set_opts(Sock, [{active, once}]),
            handle_frame(St, F, State#state{buffer=NewBuf})
    end;

handle_info({tcp_closed, Sock}, _St, State = #state{sock=Sock}) ->
    {stop, tcp_closed, State};

handle_info({tcp_error, Sock, Reason}, _St, State = #state{sock=Sock}) ->
    {stop, {tcp_error, Reason}, State};

handle_info(Info, StateName, State) ->
    io:format("Unhandled info when ~p ~p\n",[StateName, Info]),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
        ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
        {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

format_error(Code, Msg, Rest) ->
    {Code, Msg, Rest}.

handle_frame(connecting, #frame{opcode=?OP_READY}, State = #state{}) ->
    {next_state, ready, State};
handle_frame(connecting, #frame{opcode=?OP_CREDENTIALS}, State = #state{}) ->
    throw({todo, send_credentials}),
    {next_state, ready, State};

handle_frame(awaiting_reply, #frame{opcode=?OP_ERROR, body = <<Code:?int,Body/binary>> }, State = #state{}) ->
    {Msg, Rest} = ecql_parser:consume_string(Body),
    Reply = {error, format_error(Code,Msg,Rest)},
    {next_state, ready, send_reply(Reply, State)};

handle_frame(awaiting_reply, #frame{opcode=?OP_RESULT, body = <<Kind:?int,Body/binary>> }, State = #state{}) ->
    case Kind of
        1 -> %%   Void: for results carrying no information.
            {next_state, ready, send_reply(ok, State)};
        2 -> %%   Rows: for results to select queries, returning a set of rows.
            {Metadata, Rest1} = ecql_parser:consume_metadata(Body),
            {NumRows, Rest2}  = ecql_parser:consume_int(Rest1),
            {Rows, _}         = ecql_parser:consume_num_rows(NumRows, Metadata, Rest2),
            Reply = {rows, Metadata, Rows},
            {next_state, ready, send_reply(Reply, State)};
        3 -> %%   Set_keyspace: the result to a `use` query.
            {KS,_} = ecql_parser:consume_string(Body),
            Reply = {ok, KS},
            {next_state, ready, send_reply(Reply, State)};
        4 -> %%   Prepared: result to a PREPARE message.
            {PrepID, Rest} = ecql_parser:consume_short_bytes(Body),
            {Metadata, _} = ecql_parser:consume_metadata(Rest),
            Reply = {PrepID, Metadata},
            {next_state, ready, send_reply(Reply, State)};
        5 -> %%   Schema_change: the result to a schema altering query.
          % - <change> describe the type of change that has occured. It can be one of
          %   "CREATED", "UPDATED" or "DROPPED".
          % - <keyspace> is the name of the affected keyspace or the keyspace of the
          %   affected table.
          % - <table> is the name of the affected table. <table> will be empty (i.e.
          %   the empty string "") if the change was affecting a keyspace and not a
          %   table.
            {Change, Rest1}   = ecql_parser:consume_string(Body),
            {KeySpace, Rest2} = ecql_parser:consume_string(Rest1),
            {Table, _Rest3}   = ecql_parser:consume_string(Rest2),
            Reply = {Change, KeySpace, Table},
            {next_state, ready, send_reply(Reply, State)}
    end;


handle_frame(St, F, State) ->
    {stop, {unhandled_frame, St, F}, State}.


send_reply(Reply, State = #state{caller=From}) when From =/= undefined ->
    gen_fsm:reply(From, Reply),
    State#state{caller=undefined}.
