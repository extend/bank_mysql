%% Copyright (c) 2012-2013, Loïc Hoguin <essen@ninenines.eu>
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

%% @doc MySQL driver for Bank.
-module(bank_mysql).

%% API.
-export([connect/5]).
-export([close/1]).
-export([execute/3]).
-export([fetch/1]).
-export([fetch_all/1]).
-export([ping/1]).
-export([prepare/3]).
-export([unprepare/2]).
-export([sql_query/2]).

%% Flags for client capabilities.
-define(CLIENT_LONG_PASSWORD, 1). %% New more secure passwords
-define(CLIENT_FOUND_ROWS, 2). %% Found instead of affected rows
-define(CLIENT_LONG_FLAG, 4). %% Get all column flags
-define(CLIENT_CONNECT_WITH_DB, 8). %% One can specify db on connect
-define(CLIENT_NO_SCHEMA, 16). %% Don't allow database.table.column
-define(CLIENT_COMPRESS, 32). %% Can use compression protocol
-define(CLIENT_ODBC, 64). %% Odbc client
-define(CLIENT_LOCAL_FILES, 128). %% Can use LOAD DATA LOCAL
-define(CLIENT_IGNORE_SPACE, 256). %% Ignore spaces before '('
-define(CLIENT_PROTOCOL_41, 512). %% New 4.1 protocol
-define(CLIENT_INTERACTIVE, 1024). %% This is an interactive client
-define(CLIENT_SSL, 2048). %% Switch to SSL after handshake
-define(CLIENT_IGNORE_SIGPIPE, 4096). %% IGNORE sigpipes
-define(CLIENT_TRANSACTIONS, 8192). %% Client knows about transactions
-define(CLIENT_RESERVED, 16384). %% Old flag for 4.1 protocol
-define(CLIENT_SECURE_CONNECTION, 32768). %% New 4.1 authentication
-define(CLIENT_MULTI_STATEMENTS, 65536). %% Enable/disable multi-stmt support
-define(CLIENT_MULTI_RESULTS, 131072). %% Enable/disable multi-results

%% Commands.
-define(COM_SLEEP, 16#00). %% (none, this is an internal thread state)
-define(COM_QUIT, 16#01). %% mysql_close
-define(COM_INIT_DB, 16#02). %% mysql_select_db 
-define(COM_QUERY, 16#03). %% mysql_real_query
-define(COM_FIELD_LIST, 16#04). %% mysql_list_fields
-define(COM_CREATE_DB, 16#05). %% mysql_create_db (deprecated)
-define(COM_DROP_DB, 16#06). %% mysql_drop_db (deprecated)
-define(COM_REFRESH, 16#07). %% mysql_refresh
-define(COM_SHUTDOWN, 16#08). %% mysql_shutdown
-define(COM_STATISTICS, 16#09). %% mysql_stat
-define(COM_PROCESS_INFO, 16#0a). %% mysql_list_processes
-define(COM_CONNECT, 16#0b). %% (none, this is an internal thread state)
-define(COM_PROCESS_KILL, 16#0c). %% mysql_kill
-define(COM_DEBUG, 16#0d). %% mysql_dump_debug_info
-define(COM_PING, 16#0e). %% mysql_ping
-define(COM_TIME, 16#0f). %% (none, this is an internal thread state)
-define(COM_DELAYED_INSERT, 16#10). %% (none, this is an internal thread state)
-define(COM_CHANGE_USER, 16#11). %% mysql_change_user
-define(COM_BINLOG_DUMP, 16#12). %% sent by the slave IO thread to request a binlog
-define(COM_TABLE_DUMP, 16#13). %% LOAD TABLE ... FROM MASTER (deprecated)
-define(COM_CONNECT_OUT, 16#14). %% (none, this is an internal thread state)
-define(COM_REGISTER_SLAVE, 16#15). %% sent by the slave to register with the master (optional)
-define(COM_STMT_PREPARE, 16#16). %% mysql_stmt_prepare
-define(COM_STMT_EXECUTE, 16#17). %% mysql_stmt_execute
-define(COM_STMT_SEND_LONG_DATA, 16#18). %% mysql_stmt_send_long_data
-define(COM_STMT_CLOSE, 16#19). %% mysql_stmt_close
-define(COM_STMT_RESET, 16#1a). %% mysql_stmt_reset
-define(COM_SET_OPTION, 16#1b). %% mysql_set_server_option
-define(COM_STMT_FETCH, 16#1c). %% mysql_stmt_fetch

%% Data types.
-define(MYSQL_TYPE_DECIMAL, 16#00).
-define(MYSQL_TYPE_TINY, 16#01).
-define(MYSQL_TYPE_SHORT, 16#02).
-define(MYSQL_TYPE_LONG, 16#03).
-define(MYSQL_TYPE_FLOAT, 16#04).
-define(MYSQL_TYPE_DOUBLE, 16#05).
-define(MYSQL_TYPE_NULL, 16#06).
-define(MYSQL_TYPE_TIMESTAMP, 16#07).
-define(MYSQL_TYPE_LONGLONG, 16#08).
-define(MYSQL_TYPE_INT24, 16#09).
-define(MYSQL_TYPE_DATE, 16#0a).
-define(MYSQL_TYPE_TIME, 16#0b).
-define(MYSQL_TYPE_DATETIME, 16#0c).
-define(MYSQL_TYPE_YEAR, 16#0d).
-define(MYSQL_TYPE_NEWDATE, 16#0e).
-define(MYSQL_TYPE_VARCHAR, 16#0f).
-define(MYSQL_TYPE_BIT, 16#10).
-define(MYSQL_TYPE_NEWDECIMAL, 16#f6).
-define(MYSQL_TYPE_ENUM, 16#f7).
-define(MYSQL_TYPE_SET, 16#f8).
-define(MYSQL_TYPE_TINY_BLOB, 16#f9).
-define(MYSQL_TYPE_MEDIUM_BLOB, 16#fa).
-define(MYSQL_TYPE_LONG_BLOB, 16#fb).
-define(MYSQL_TYPE_BLOB, 16#fc).
-define(MYSQL_TYPE_VAR_STRING, 16#fd).
-define(MYSQL_TYPE_STRING, 16#fe).
-define(MYSQL_TYPE_GEOMETRY, 16#ff).

%% @todo bitstring, time_tz, timestamp_tz
-type erlang_type() :: null | integer | float
	| date | time | timestamp | binary.

%% @todo Support all other types.
-type mysql_type() :: ?MYSQL_TYPE_TINY
	| ?MYSQL_TYPE_SHORT
	| ?MYSQL_TYPE_LONG
	| ?MYSQL_TYPE_FLOAT
	| ?MYSQL_TYPE_DOUBLE
	| ?MYSQL_TYPE_NULL
	| ?MYSQL_TYPE_TIMESTAMP
	| ?MYSQL_TYPE_LONGLONG
	| ?MYSQL_TYPE_DATE
	| ?MYSQL_TYPE_TIME
	| ?MYSQL_TYPE_DATETIME
	| ?MYSQL_TYPE_BLOB
	| ?MYSQL_TYPE_VAR_STRING
	| ?MYSQL_TYPE_STRING.

%% Cursor types.
-define(CURSOR_TYPE_NO_CURSOR, 0).
-define(CURSOR_TYPE_READ_ONLY, 1).
-define(CURSOR_TYPE_FOR_UPDATE, 2).
-define(CURSOR_TYPE_SCROLLABLE, 4).

-type field() :: {field, binary(), erlang_type(), mysql_type()}.
-type remote_error() :: {remote_error, non_neg_integer(), binary(), binary()}.
-type value() :: null | integer() | float()
	| calendar:date() | calendar:time() | calendar:datetime() | binary().

-record(mysql_client, {
	socket = undefined,
	buffer = <<>> :: binary(),
	packetno = 0 :: -1 | non_neg_integer(),
	state = ready :: ready | {fetch, lcs | bin, [field()]},
	stmts = [] :: [{any(), non_neg_integer()}],

	recv_timeout = 5000 :: timeout(),
	max_packet_size = 100000 :: non_neg_integer()
}).
-opaque state() :: #mysql_client{}.
-export_type([state/0]).

%% API.

%% @doc Connect to the given MySQL database.
-spec connect(string(), inet:port_number(), string(), string(), string())
	-> {ok, state()} | remote_error().
connect(Host, Port, User, Password, Database) ->
	{ok, Socket} = gen_tcp:connect(Host, Port,
		[binary, {active, false}, {packet, raw}]),
	State = #mysql_client{socket=Socket},
	{ok, Packet, State2} = recv(State),
	{ok, _ProtoVersion, _ServerVersion, _ThreadID, ScrambleBuffer,
		_Caps, Language, _Status, _AuthPlugin} = parse_handshake_init(Packet),
	{ok, State3} = send_client_auth(User, Password, Database,
		ScrambleBuffer, Language, State2),
	{ok, ResPacket, State4} = recv(State3),
	case type(ResPacket) of
		ok ->
			{ok, 0, 0, _ResStatus, 0, <<>>} = parse_ok(ResPacket),
			{ok, State4};
		error ->
			parse_error(ResPacket)
	end.

%% @doc Close the MySQL database connection.
-spec close(state()) -> ok.
close(State) ->
	{ok, State2} = send_quit(State),
	ok = gen_tcp:close(State2#mysql_client.socket).

%% @doc Execute a prepared statement.
-spec execute(any(), [value()], State)
	-> {ok, non_neg_integer(), non_neg_integer(), State}
	| {result_set, [field()], State} | remote_error() when State::state().
execute(Stmt, Params, State=#mysql_client{state=ready, stmts=StmtsList}) ->
	{Stmt, StmtHandler} = lists:keyfind(Stmt, 1, StmtsList),
	{ok, State2} = send_execute(StmtHandler, Params, new_query(State)),
	{ok, ResPacket, State3} = recv(State2),
	case handle_result(ResPacket, State3) of
		{result_set, FieldCount, _Extra} ->
			{fields, Fields, State4} = handle_fields(FieldCount, State3),
			{result_set, Fields, State4#mysql_client{
				state={fetch, bin, Fields}}};
		Res ->
			Res
	end.

%% @doc Fetch a single row from a query that returned a result set.
-spec fetch(State)
	-> {row, [value()], State} | {done, State} when State::state().
fetch(State=#mysql_client{state={fetch, Format, Fields}}) ->
	{ok, Packet, State2} = recv(State),
	case type(Packet) of
		eof ->
			{done, State2#mysql_client{state=ready}};
		_ ->
			{row, Values} = fetch_row(Packet, Fields, Format),
			{row, Values, State2}
	end.

%% @doc Fetch all rows from a query that returned a result set.
-spec fetch_all(State) -> {rows, [[value()]], State} when State::state().
fetch_all(State) ->
	fetch_all(State, []).
fetch_all(State, Acc) ->
	case fetch(State) of
		{done, State2} ->
			{rows, lists:reverse(Acc), State2};
		{row, Values, State2} ->
			fetch_all(State2, [Values|Acc])
	end.

%% @doc Send a ping to the database.
-spec ping(State) -> {ok, State} when State::state().
ping(State) ->
	{ok, State2} = send_ping(new_query(State)),
	{ok, ResPacket, State3} = recv(State2),
	{ok, 0, 0, State4} = handle_result(ResPacket, State3),
	{ok, State4}.

%% @doc Create a prepared statement.
-spec prepare(any(), string(), State)
	-> {ok, State} | remote_error() when State::state().
prepare(Stmt, Query, State=#mysql_client{state=ready, stmts=StmtsList}) ->
	{ok, State2} = send_prepare(Query, new_query(State)),
	{ok, ResPacket, State3} = recv(State2),
	case handle_prepare_result(ResPacket) of
		{ok, StmtHandler, FieldCount, ParamCount, _Warnings} ->
			%% We don't need this.
			{fields, _, State4} = handle_fields(ParamCount, State3),
			{fields, _, State5} = handle_fields(FieldCount, State4),
			{ok, State5#mysql_client{
				stmts=[{Stmt, StmtHandler}|StmtsList]}};
		Res ->
			Res
	end.

%% @doc Delete a prepared statement.
-spec unprepare(any(), State)
	-> {ok, State} when State::state().
unprepare(Stmt, State=#mysql_client{state=ready, stmts=StmtsList}) ->
	case lists:keytake(Stmt, 1, StmtsList) of
		{value, {Stmt, StmtHandler}, StmtsList0} ->
			{ok, State2} = send_close(StmtHandler, new_query(State)),
			{ok, State2#mysql_client{stmts=StmtsList0}};
		false ->
			{ok, State}
	end.

%% @doc Execute the given SQL query.
-spec sql_query(string(), State)
	-> {ok, non_neg_integer(), non_neg_integer(), State}
	| {result_set, [field()], State} | remote_error() when State::state().
sql_query(Query, State=#mysql_client{state=ready}) ->
	{ok, State2} = send_query(Query, new_query(State)),
	{ok, ResPacket, State3} = recv(State2),
	case handle_result(ResPacket, State3) of
		{result_set, FieldCount, _Extra} ->
			{fields, Fields, State4} = handle_fields(FieldCount, State3),
			{result_set, Fields, State4#mysql_client{
				state={fetch, lcs, Fields}}};
		Res ->
			Res
	end.

%% State.

new_query(State) ->
	State#mysql_client{packetno=-1}.

%% Parsing.

type(Packet) ->
	case binary:at(Packet, 0) of
		0 -> ok;
		254 when byte_size(Packet) < 9 -> eof;
		255 -> error;
		_ -> data
	end.

parse_handshake_init(Packet) ->
	<< ProtoVersion:8, Rest/binary >> = Packet,
	[ServerVersion, Rest2] = binary:split(Rest, << 0:8 >>),
	<<	ThreadID:32/little, ScrambleBuff1:64/bits,
		0:8, CapsLow:16/little, Language:8,
		Status:16/little, CapsHigh:16/little,
		_ScrambleLength:8, 0:80, Rest3/bits >> = Rest2,
	Caps = CapsLow bor (CapsHigh bsl 16),
	[ScrambleBuff2, AuthPlugin]
		= case binary:split(Rest3, << 0:8 >>, [global, trim]) of
      [S, A] -> [S, A];
      %% MySQL before version 5.5.7 didn't have auth_plugin_name
      [S] -> [S, <<>>]
    end,
	{ok, ProtoVersion, ServerVersion, ThreadID,
		<< ScrambleBuff1/binary, ScrambleBuff2/binary >>,
		Caps, Language, Status, AuthPlugin}.

parse_ok(<< 0:8, Rest/bits >>) ->
	{AffectedRows, Rest2} = parse_lcb(Rest),
	{InsertID, Rest3} = parse_lcb(Rest2),
	<< Status:16/little, Warnings:16/little, Message/bits >> = Rest3,
	{ok, AffectedRows, InsertID, Status, Warnings, Message}.

parse_prepare_ok(Packet) ->
	<< 0:8, StmtHandler:32/little, FieldCount:16/little,
		ParamCount:16/little, 0:8, Warnings:16/little >> = Packet,
	{ok, StmtHandler, FieldCount, ParamCount, Warnings}.

parse_result_set(Packet) ->
	case parse_lcb(Packet) of
		{FieldCount, <<>>} ->
			{result_set, FieldCount, undefined};
		{FieldCount, Rest} ->
			{Extra, <<>>} = parse_lcb(Rest),
			{result_set, FieldCount, Extra}
	end.

parse_field(Packet) ->
	{_Catalog, Rest} = parse_lcs(Packet),
	{_DB, Rest2} = parse_lcs(Rest),
	{_Table, Rest3} = parse_lcs(Rest2),
	{_OrigTable, Rest4} = parse_lcs(Rest3),
	{Name, Rest5} = parse_lcs(Rest4),
	{_OrigName, Rest6} = parse_lcs(Rest5),
	<<	_:8, _Charset:16/little, _Length:32/little, Type:8,
		_Flags:16/little, _Decimals:8, 0:16, Rest7/bits >> = Rest6,
	{_Default, <<>>} = case Rest7 of
		<<>> -> {undefined, <<>>};
		_ -> parse_lcb(Rest7)
	end,
	%% @todo Return length or precision+scale where applicable.
	%% @todo Return charset converted to atoms like 'utf8' or 'latin1'.
	{field, Name, mysql_to_erlang_type(Type), Type}.

%mysql_to_erlang_type(?MYSQL_TYPE_DECIMAL) -> float;
mysql_to_erlang_type(?MYSQL_TYPE_TINY) -> integer;
mysql_to_erlang_type(?MYSQL_TYPE_SHORT) -> integer;
mysql_to_erlang_type(?MYSQL_TYPE_LONG) -> integer;
mysql_to_erlang_type(?MYSQL_TYPE_FLOAT) -> float;
mysql_to_erlang_type(?MYSQL_TYPE_DOUBLE) -> float;
mysql_to_erlang_type(?MYSQL_TYPE_NULL) -> null;
mysql_to_erlang_type(?MYSQL_TYPE_TIMESTAMP) -> timestamp;
mysql_to_erlang_type(?MYSQL_TYPE_LONGLONG) -> integer;
%mysql_to_erlang_type(?MYSQL_TYPE_INT24) -> integer;
mysql_to_erlang_type(?MYSQL_TYPE_DATE) -> date;
mysql_to_erlang_type(?MYSQL_TYPE_TIME) -> time;
mysql_to_erlang_type(?MYSQL_TYPE_DATETIME) -> timestamp;
%mysql_to_erlang_type(?MYSQL_TYPE_YEAR) -> integer;
%mysql_to_erlang_type(?MYSQL_TYPE_NEWDATE) -> date;
%mysql_to_erlang_type(?MYSQL_TYPE_VARCHAR) -> binary;
%mysql_to_erlang_type(?MYSQL_TYPE_BIT) -> bitstring;
%mysql_to_erlang_type(?MYSQL_TYPE_NEWDECIMAL) -> float;
%mysql_to_erlang_type(?MYSQL_TYPE_ENUM) -> integer;
%mysql_to_erlang_type(?MYSQL_TYPE_SET) -> integer;
%mysql_to_erlang_type(?MYSQL_TYPE_TINY_BLOB) -> binary;
%mysql_to_erlang_type(?MYSQL_TYPE_MEDIUM_BLOB) -> binary;
%mysql_to_erlang_type(?MYSQL_TYPE_LONG_BLOB) -> binary;
mysql_to_erlang_type(?MYSQL_TYPE_BLOB) -> binary;
mysql_to_erlang_type(?MYSQL_TYPE_VAR_STRING) -> binary;
mysql_to_erlang_type(?MYSQL_TYPE_STRING) -> binary.
%mysql_to_erlang_type(?MYSQL_TYPE_GEOMETRY) -> geometry.

fetch_row(Packet, Fields, lcs) ->
	parse_row(Packet, Fields);
fetch_row(Packet, Fields, bin) ->
	parse_bin_row(Packet, Fields).

parse_row(Packet, Fields) ->
	parse_row(Packet, Fields, []).
parse_row(<<>>, [], Acc) ->
	{row, lists:reverse(Acc)};
parse_row(Packet, [Field|Tail], Acc) ->
	{Value, Rest} = parse_lcs(Packet),
	%% @todo Depending on the type we may need to convert encoding to UTF-8.
	Value2 = convert_type(element(3, Field), Value),
	parse_row(Rest, Tail, [Value2|Acc]).

parse_bin_row(Packet, Fields) ->
	NullBinSize = (length(Fields) + 7 + 2) div 8,
	<< 0:8, NullBin:NullBinSize/binary, Rest/binary >> = Packet,
	NullBin2 = null_map_from_mysql(NullBin),
	parse_bin_row(Rest, Fields, NullBin2, []).
parse_bin_row(<<>>, [], _, Acc) ->
	{row, lists:reverse(Acc)};
parse_bin_row(Packet, [_|Fields], << 1:1, NullRest/bits >>, Acc) ->
	parse_bin_row(Packet, Fields, NullRest, [null|Acc]);
parse_bin_row(Packet, [{field, _, binary, _}|Fields],
		<< 0:1, NullRest/bits >>, Acc) ->
	{Value, Rest} = parse_lcs(Packet),
	%% We don't expect any null value here.
	false = Value =:= null,
	parse_bin_row(Rest, Fields, NullRest, [Value|Acc]);
parse_bin_row(Packet, [{field, _, integer, ?MYSQL_TYPE_TINY}|Fields],
		<< 0:1, NullRest/bits >>, Acc) ->
	<< Value:8/little, Rest/binary >> = Packet,
	parse_bin_row(Rest, Fields, NullRest, [Value|Acc]);
parse_bin_row(Packet, [{field, _, integer, ?MYSQL_TYPE_LONG}|Fields],
		<< 0:1, NullRest/bits >>, Acc) ->
	<< Value:32/little, Rest/binary >> = Packet,
	parse_bin_row(Rest, Fields, NullRest, [Value|Acc]);
parse_bin_row(Packet, [{field, _, integer, ?MYSQL_TYPE_LONGLONG}|Fields],
		<< 0:1, NullRest/bits >>, Acc) ->
	<< Value:64/little, Rest/binary >> = Packet,
	parse_bin_row(Rest, Fields, NullRest, [Value|Acc]);
parse_bin_row(Packet, [{field, _, date, _}|Fields],
		<< 0:1, NullRest/bits >>, Acc) ->
	<< 4:8, Y:16/little, Mo:8, D:8, Rest/binary >> = Packet,
	parse_bin_row(Rest, Fields, NullRest, [{Y, Mo, D}|Acc]);
parse_bin_row(Packet, [{field, _, time, _}|Fields],
		<< 0:1, NullRest/bits >>, Acc) ->
	%% @todo Second byte is 1 if time is negative.
	<< 8:8, 0:8, 0:32, H:8, Mi:8, S:8, Rest/bits >> = Packet,
	parse_bin_row(Rest, Fields, NullRest, [{H, Mi, S}|Acc]);
parse_bin_row(Packet, [{field, _, timestamp, _}|Fields],
		<< 0:1, NullRest/bits >>, Acc) ->
	<< Length:8, Rest/bits >> = Packet,
	case Length of
		0 ->
			parse_bin_row(Rest, Fields, NullRest,
				[{{0, 0, 0}, {0, 0, 0}}|Acc]);
		4 ->
			<< Y:16/little, Mo:8, D:8, Rest2/bits >> = Rest,
			parse_bin_row(Rest2, Fields, NullRest,
				[{{Y, Mo, D}, {0, 0, 0}}|Acc]);
		7 ->
			<< Y:16/little, Mo:8, D:8, H:8, Mi:8, S:8, Rest2/bits >> = Rest,
			parse_bin_row(Rest2, Fields, NullRest,
				[{{Y, Mo, D}, {H, Mi, S}}|Acc])
	end.

%% @todo decimal, int24, year, newdate, varchar, bit, newdecimal,
%% enum, set, tiny_blob, medium_blob, long_blob, geometry.
convert_type(_, null) ->
	null;
convert_type(integer, Value) ->
	list_to_integer(binary_to_list(Value));
convert_type(float, Value) ->
	binary_to_float(Value);
convert_type(date, Value) ->
	<< Y:4/binary, $-, Mo:2/binary, $-, D:2/binary >> = Value,
	{binary_to_integer(Y), binary_to_integer(Mo), binary_to_integer(D)};
convert_type(time, Value) ->
	<< H:2/binary, $:, Mi:2/binary, $:, S:2/binary >> = Value,
	{binary_to_integer(H), binary_to_integer(Mi), binary_to_integer(S)};
convert_type(timestamp, Value) ->
	<< Date:10/binary, " ", Time:8/binary >> = Value,
	{convert_type(date, Date), convert_type(time, Time)};
convert_type(binary, Value) ->
	Value.

parse_eof(Packet) ->
	<< 16#fe:8, Warnings:16/little, Status:16/little >> = Packet,
	{eof, Warnings, Status}.

parse_error(Packet) ->
	<< 16#ff:8, ErrNo:16/little, SqlState:48/bits, Message/bits >> = Packet,
	{remote_error, ErrNo, SqlState, Message}.

parse_lcb(<< Value:8, Rest/bits >>) when Value =< 250 ->
	{Value, Rest};
parse_lcb(<< 252:8, Value:16/little, Rest/bits >>) ->
	{Value, Rest};
parse_lcb(<< 253:8, Value:24/little, Rest/bits >>) ->
	{Value, Rest};
parse_lcb(<< 254:8, Value:64/little, Rest/bits >>) ->
	{Value, Rest}.

parse_lcs(<< 251:8, Rest/bits >>) ->
	{null, Rest};
parse_lcs(Bin) ->
	{Length, Rest} = parse_lcb(Bin),
	<< String:Length/binary, Rest2/bits >> = Rest,
	{String, Rest2}.

%% Sending.

send_client_auth(User, Password, Database, ScrambleBuffer, Language,
		State=#mysql_client{max_packet_size=MaxPacketSize}) ->
	UserBin = list_to_binary(User),
	PassBin = case Password of
		"" -> << 0:8 >>;
		_ ->
			ScrambledPassword = scramble(Password, ScrambleBuffer),
			ScrambledSize = byte_size(ScrambledPassword),
			<< ScrambledSize:8, ScrambledPassword/binary >>
	end,
	DatabaseBin = list_to_binary(Database),
	Caps = ?CLIENT_LONG_PASSWORD bor ?CLIENT_LONG_FLAG bor ?CLIENT_TRANSACTIONS bor ?CLIENT_PROTOCOL_41 bor ?CLIENT_SECURE_CONNECTION bor ?CLIENT_CONNECT_WITH_DB,
	send(<< Caps:32/little, MaxPacketSize:32/little, Language:8, 0:184,
		UserBin/binary, 0:8, PassBin/binary, DatabaseBin/binary, 0:8 >>, State).

scramble(Password, Scramble) ->
	Stage1Hash = crypto:sha(Password),
	DoubleHash = crypto:sha(Stage1Hash),
	ScrambledHash = crypto:sha(<< Scramble/binary, DoubleHash/binary >>),
	binary_xor(ScrambledHash, Stage1Hash).

binary_xor(BinA, BinB) ->
	binary_xor(BinA, BinB, <<>>).
binary_xor(<<>>, <<>>, Acc) ->
	Acc;
binary_xor(<< A, RestA/binary >>, << B, RestB/binary >>, Acc) ->
	binary_xor(RestA, RestB, << Acc/binary, (A bxor B):8 >>).

send_execute(StmtHandler, Params, State) ->
	ParamsBin = case length(Params) of
		0 -> <<>>;
		_ -> params_to_bin(Params)
	end,
	Bin = << StmtHandler:32/little, ?CURSOR_TYPE_NO_CURSOR:8,
		1:32/little, ParamsBin/binary >>,
	send_command(?COM_STMT_EXECUTE, Bin, State).

params_to_bin(Params) ->
	params_to_bin(Params, <<>>, <<>>, <<>>).
params_to_bin([], NullBin, TypesBin, ValuesBin) ->
	NullBin2 = null_map_to_mysql(NullBin),
	<< NullBin2/binary, 1:8, TypesBin/binary, ValuesBin/binary >>;
params_to_bin([null|Tail], NullBin, TypesBin, ValuesBin) ->
	params_to_bin(Tail,
		<< NullBin/bitstring, 1:1 >>,
		<< TypesBin/binary, ?MYSQL_TYPE_NULL:16/little >>,
		ValuesBin);
%% There is no true or false in MySQL, use 1 or 0 instead.
params_to_bin([true|Tail], NullBin, TypesBin, ValuesBin) ->
	params_to_bin([1|Tail], NullBin, TypesBin, ValuesBin);
params_to_bin([false|Tail], NullBin, TypesBin, ValuesBin) ->
	params_to_bin([0|Tail], NullBin, TypesBin, ValuesBin);
%% Lists and other atoms are converted to binary for convenience.
%% Note that atoms are expected to be latin1.
params_to_bin([Value|Tail], NullBin, TypesBin, ValuesBin)
		when is_atom(Value) ->
	params_to_bin([atom_to_binary(Value, latin1)|Tail],
		NullBin, TypesBin, ValuesBin);
params_to_bin([Value|Tail], NullBin, TypesBin, ValuesBin)
		when is_list(Value) ->
	params_to_bin([list_to_binary(Value)|Tail], NullBin, TypesBin, ValuesBin);
params_to_bin([Value|Tail], NullBin, TypesBin, ValuesBin)
		when is_binary(Value) ->
	SizeBin = case byte_size(Value) of
		S when S =< 250 -> << S:8 >>;
		S when S =< 65535 -> << 252:8, S:16/little >>;
		S when S =< 16777215 -> << 253:8, S:24/little >>;
		S -> << 254:8, S:64/little >>
	end,
	params_to_bin(Tail,
		<< NullBin/bitstring, 0:1 >>,
		<< TypesBin/binary, ?MYSQL_TYPE_BLOB:16/little >>,
		<< ValuesBin/binary, SizeBin/binary, Value/binary >>);
%% @todo Handle unsigned if the value is too high, limit values otherwise.
%% signed: -9223372036854775808	9223372036854775807
%% unsigned: 0	18446744073709551615
params_to_bin([Value|Tail], NullBin, TypesBin, ValuesBin)
		when is_integer(Value) ->
	params_to_bin(Tail,
		<< NullBin/bitstring, 0:1 >>,
		<< TypesBin/binary, ?MYSQL_TYPE_LONGLONG:16/little >>,
		<< ValuesBin/binary, Value:64/little >>);
params_to_bin([Value|Tail], NullBin, TypesBin, ValuesBin)
		when is_float(Value) ->
	params_to_bin(Tail,
		<< NullBin/bitstring, 0:1 >>,
		<< TypesBin/binary, ?MYSQL_TYPE_DOUBLE:16/little >>,
		<< ValuesBin/binary, Value:64/float-little >>);
params_to_bin([{Y, Mo, D}|Tail], NullBin, TypesBin, ValuesBin)
		when Y > 23, Mo > 0, Mo =< 12, D > 0, D =< 31 ->
	params_to_bin(Tail,
		<< NullBin/bitstring, 0:1 >>,
		<< TypesBin/binary, ?MYSQL_TYPE_DATE:16/little >>,
		<< ValuesBin/binary, 4:8, Y:16/little, Mo:8, D:8 >>);
params_to_bin([{H, Mi, S}|Tail], NullBin, TypesBin, ValuesBin)
		when H >= 0, H < 24, Mi >= 0, Mi < 60, S >= 0, S < 60 ->
	params_to_bin(Tail,
		<< NullBin/bitstring, 0:1 >>,
		<< TypesBin/binary, ?MYSQL_TYPE_TIME:16/little >>,
		%% @todo Second byte is for negative times.
		<< ValuesBin/binary, 8:8, 0:8, 0:32, H:8, Mi:8, S:8 >>);
params_to_bin([{{Y, Mo, D}, {H, Mi, S}}|Tail], NullBin, TypesBin, ValuesBin)
		when Y > 23, Mo > 0, Mo =< 12, D > 0, D =< 31,
			H >= 0, H < 24, Mi >= 0, Mi < 60, S >= 0, S < 60 ->
	params_to_bin(Tail,
		<< NullBin/bitstring, 0:1 >>,
		<< TypesBin/binary, ?MYSQL_TYPE_DATETIME:16/little >>,
		<< ValuesBin/binary, 7:8, Y:16/little, Mo:8, D:8, H:8, Mi:8, S:8 >>).

null_map_from_mysql(NullBin) ->
	%% First 2 bits are always 0 in MySQL 5.x.
	<< F:1, E:1, D:1, C:1, B:1, A:1, 0:2, Rest/binary >> = NullBin,
	ReverseBin = << << (begin
		<< I:1, J:1, K:1, L:1, M:1, N:1, O:1, P:1 >> = X,
		<< Y:8 >> = << P:1, O:1, N:1, M:1, L:1, K:1, J:1, I:1 >>,
		Y
	end):8 >> || << X:8/bits >> <= Rest >>,
	<< A:1, B:1, C:1, D:1, E:1, F:1, ReverseBin/binary >>.

null_map_to_mysql(NullBin) ->
	null_map_to_mysql(NullBin, <<>>).
null_map_to_mysql(<< Byte:8/bits, Rest/bits >>, Acc) ->
	<< A:1, B:1, C:1, D:1, E:1, F:1, G:1, H:1 >> = Byte,
	null_map_to_mysql(Rest,
		<< Acc/binary, H:1, G:1, F:1, E:1, D:1, C:1, B:1, A:1 >>);
null_map_to_mysql(Bits, Acc) ->
	Padding = 8 - bit_size(Bits),
	BitsList = [X || << X:1 >> <= Bits],
	ReverseBits = << << Y:1 >> || Y <- lists:reverse(BitsList) >>,
	<< Acc/binary, 0:Padding, ReverseBits/bits >>.

send_ping(State) ->
	send_command(?COM_PING, <<>>, State).

send_prepare(Query, State) ->
	QueryBin = iolist_to_binary(Query),
	send_command(?COM_STMT_PREPARE, QueryBin, State).

send_close(StmtHandler, State) ->
	Bin = <<StmtHandler:32/little>>,
	send_command(?COM_STMT_CLOSE, Bin, State).

send_query(Query, State) ->
	QueryBin = iolist_to_binary(Query),
	send_command(?COM_QUERY, QueryBin, State).

send_quit(State) ->
	send_command(?COM_QUIT, <<>>, State).

send_command(Command, Arg, State) ->
	send(<< Command:8, Arg/binary >>, State).

send(Packet, State=#mysql_client{socket=Socket, packetno=PacketNo}) ->
	Length = byte_size(Packet),
	PacketNo2 = PacketNo + 1,
	ok = gen_tcp:send(Socket,
		<< Length:24/little, PacketNo2:8, Packet/binary >>),
	{ok, State#mysql_client{packetno=PacketNo2}}.

%% Receiving.

handle_result(ResPacket, State) ->
	case type(ResPacket) of
		ok ->
			{ok, AffectedRows, InsertID, _ResStatus, _Warnings, _Message}
				= parse_ok(ResPacket),
			{ok, AffectedRows, InsertID, State};
		data ->
			parse_result_set(ResPacket);
		error ->
			parse_error(ResPacket)
	end.

handle_prepare_result(ResPacket) ->
	case type(ResPacket) of
		ok ->
			parse_prepare_ok(ResPacket);
		error ->
			parse_error(ResPacket)
	end.

handle_fields(0, State) ->
	{fields, [], State};
handle_fields(Count, State) ->
	handle_fields(Count, State, []).

handle_fields(0, State, Acc) ->
	{ok, Packet, State2} = recv(State),
	{eof, _Warnings, _Status} = parse_eof(Packet),
	{fields, lists:reverse(Acc), State2};
handle_fields(Count, State, Acc) ->
	{ok, Packet, State2} = recv(State),
	Field = {field, _, _, _} = parse_field(Packet),
	handle_fields(Count - 1, State2, [Field|Acc]).

recv(State=#mysql_client{socket=Socket, buffer=Buffer, recv_timeout=Timeout}) ->
	case split(Buffer) of
		more ->
			{ok, Data} = gen_tcp:recv(Socket, 0, Timeout),
			recv(State#mysql_client{buffer= << Buffer/binary, Data/binary >>});
		{ok, PacketNo, Packet, Rest} ->
			{ok, Packet, State#mysql_client{buffer=Rest, packetno=PacketNo}}
	end.

split(Data) ->
	case Data of
		<< PacketSize:24/little, PacketNo:8, Rest/bits >> ->
			case Rest of
				<< Packet:PacketSize/binary, Rest2/binary >> ->
					{ok, PacketNo, Packet, Rest2};
				_ ->
					more
			end;
		_ ->
			more
	end.
