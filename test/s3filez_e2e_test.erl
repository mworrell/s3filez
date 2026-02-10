%% @author Marc Worrell
%% @copyright 2013-2026 Marc Worrell
%% @doc End-to-end tests using a minimal dummy S3 server.
%% @end
%%
%% Copyright 2013-2026 Marc Worrell
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%

-module(s3filez_e2e_test).

-include_lib("eunit/include/eunit.hrl").

-define(READ_TIMEOUT, 5000).

e2e_put_get_delete_test() ->
    case start_server() of
        {skip, Reason} ->
            {skip, Reason};
        {Pid, Port} ->
    try
        [ok = application:ensure_started(X) || X <- [crypto, public_key, ssl, ssl_verify_fun, inets, jobs, tls_certificate_check, s3filez]],
        Cfg = #{
            username => <<"test-access">>,
            password => <<"test-secret">>
        },
        Url = list_to_binary(io_lib:format("http://127.0.0.1:~B/test-bucket/test-key", [Port])),
        ok = s3filez:put(Cfg, Url, {data, <<"hello">>}),
        {ok, <<"binary/octet-stream">>, <<"hello">>} = s3filez:get(Cfg, Url),
        ok = s3filez:delete(Cfg, Url),
        {error, enoent} = s3filez:get(Cfg, Url)
    after
        stop_server(Pid)
    end
    end.

e2e_put_get_delete_v4_test() ->
    case start_server() of
        {skip, Reason} ->
            {skip, Reason};
        {Pid, Port} ->
    try
        [ok = application:ensure_started(X) || X <- [crypto, public_key, ssl, ssl_verify_fun, inets, jobs, tls_certificate_check, s3filez]],
        Cfg = #{
            username => <<"test-access">>,
            password => <<"test-secret">>,
            signature_version => v4,
            region => <<"us-east-1">>
        },
        Url = list_to_binary(io_lib:format("http://127.0.0.1:~B/test-bucket/test-key-v4", [Port])),
        ok = s3filez:put(Cfg, Url, {data, <<"hello-v4">>}),
        {ok, <<"binary/octet-stream">>, <<"hello-v4">>} = s3filez:get(Cfg, Url),
        ok = s3filez:delete(Cfg, Url),
        {error, enoent} = s3filez:get(Cfg, Url)
    after
        stop_server(Pid)
    end
    end.

e2e_get_missing_region_v4_test() ->
    case start_server() of
        {skip, Reason} ->
            {skip, Reason};
        {Pid, Port} ->
            try
                [ok = application:ensure_started(X) || X <- [crypto, public_key, ssl, ssl_verify_fun, inets, jobs, tls_certificate_check, s3filez]],
                Cfg = #{
                    username => <<"test-access">>,
                    password => <<"test-secret">>,
                    signature_version => v4
                },
                Url = list_to_binary(io_lib:format("http://127.0.0.1:~B/test-bucket/missing-region", [Port])),
                {error, region_needed} = s3filez:get(Cfg, Url)
            after
                stop_server(Pid)
            end
    end.

e2e_get_invalid_credentials_v4_test() ->
    case start_server() of
        {skip, Reason} ->
            {skip, Reason};
        {Pid, Port} ->
            try
                [ok = application:ensure_started(X) || X <- [crypto, public_key, ssl, ssl_verify_fun, inets, jobs, tls_certificate_check, s3filez]],
                Cfg = #{
                    username => <<"wrong-access">>,
                    password => <<"wrong-secret">>,
                    signature_version => v4,
                    region => <<"us-east-1">>
                },
                Url = list_to_binary(io_lib:format("http://127.0.0.1:~B/test-bucket/invalid-credentials-v4", [Port])),
                {error, forbidden} = s3filez:get(Cfg, Url)
            after
                stop_server(Pid)
            end
    end.

e2e_get_invalid_credentials_v2_test() ->
    case start_server() of
        {skip, Reason} ->
            {skip, Reason};
        {Pid, Port} ->
            try
                [ok = application:ensure_started(X) || X <- [crypto, public_key, ssl, ssl_verify_fun, inets, jobs, tls_certificate_check, s3filez]],
                Cfg = #{
                    username => <<"wrong-access">>,
                    password => <<"wrong-secret">>
                },
                Url = list_to_binary(io_lib:format("http://127.0.0.1:~B/test-bucket/invalid-credentials-v2", [Port])),
                {error, forbidden} = s3filez:get(Cfg, Url)
            after
                stop_server(Pid)
            end
    end.

start_server() ->
    case gen_tcp:listen(0, [binary, {packet, raw}, {active, false}, {reuseaddr, true}]) of
        {ok, LSock} ->
            {ok, {_, Port}} = inet:sockname(LSock),
            Table = ets:new(?MODULE, [set, private]),
            Pid = spawn_link(fun() -> accept_loop(LSock, Table) end),
            {Pid, Port};
        {error, Reason} ->
            {skip, {cannot_start_server, Reason}}
    end.

stop_server(Pid) ->
    Pid ! stop,
    ok.

accept_loop(LSock, Table) ->
    receive
        stop ->
            gen_tcp:close(LSock),
            ets:delete(Table),
            ok
    after 0 ->
        case gen_tcp:accept(LSock) of
            {ok, Sock} ->
                spawn(fun() -> handle_conn(Sock, Table) end),
                accept_loop(LSock, Table);
            {error, closed} ->
                ok
        end
    end.

handle_conn(Sock, Table) ->
    case recv_request(Sock) of
        {ok, Method, Path, Query, Headers, Body} ->
            handle_request(Sock, Table, Method, Path, Query, Headers, Body);
        _ ->
            ok
    end,
    gen_tcp:close(Sock).

recv_request(Sock) ->
    case recv_until_headers(Sock, <<>>) of
        {ok, HeaderBin, Rest} ->
            [ReqLine | HeaderLines] = binary:split(HeaderBin, <<"\r\n">>, [global]),
            {Method, Path, Query} = parse_request_line(ReqLine),
            Headers = parse_headers(HeaderLines, []),
            ContentLen = header_content_length(Headers),
            Body = recv_body(Sock, Rest, ContentLen),
            {ok, Method, Path, Query, Headers, Body};
        Error ->
            Error
    end.

recv_until_headers(Sock, Acc) ->
    case binary:match(Acc, <<"\r\n\r\n">>) of
        {Pos, _} ->
            HeaderBin = binary:part(Acc, 0, Pos),
            Rest = binary:part(Acc, Pos + 4, byte_size(Acc) - Pos - 4),
            {ok, HeaderBin, Rest};
        nomatch ->
            case gen_tcp:recv(Sock, 0, ?READ_TIMEOUT) of
                {ok, Data} ->
                    recv_until_headers(Sock, <<Acc/binary, Data/binary>>);
                Error ->
                    Error
            end
    end.

parse_request_line(Line) ->
    [MethodBin, PathBin | _] = binary:split(Line, <<" ">>, [global]),
    {Path, Query} =
        case binary:split(PathBin, <<"?">>) of
            [P, Q] -> {P, Q};
            [P] -> {P, <<>>}
        end,
    {binary_to_list(MethodBin), Path, Query}.

parse_headers([], Acc) ->
    Acc;
parse_headers([<<>> | _], Acc) ->
    Acc;
parse_headers([Line | Rest], Acc) ->
    case split_header(Line) of
        {Key, Val} ->
            parse_headers(Rest, [{string:lowercase(binary_to_list(Key)), binary:trim(Val, leading, <<" ">>)} | Acc]);
        error ->
            parse_headers(Rest, Acc)
    end.

split_header(Line) ->
    case binary:match(Line, <<":">>) of
        {Pos, 1} ->
            Key = binary:part(Line, 0, Pos),
            Val = binary:part(Line, Pos + 1, byte_size(Line) - Pos - 1),
            {Key, Val};
        nomatch ->
            error
    end.

header_content_length(Headers) ->
    case lists:keyfind("content-length", 1, Headers) of
        {_, V} -> list_to_integer(binary_to_list(V));
        false -> 0
    end.

recv_body(_Sock, Rest, 0) ->
    Rest;
recv_body(Sock, Rest, Len) ->
    Have = byte_size(Rest),
    case Have >= Len of
        true ->
            binary:part(Rest, 0, Len);
        false ->
            case gen_tcp:recv(Sock, 0, ?READ_TIMEOUT) of
                {ok, Data} ->
                    recv_body(Sock, <<Rest/binary, Data/binary>>, Len);
                _ ->
                    Rest
            end
    end.

handle_request(Sock, Table, Method, Path, Query, Headers, Body) ->
    case validate_signature(Method, Path, Query, Headers, Body) of
        ok ->
            handle_request_ok(Sock, Table, Method, Path, Body);
        {error, forbidden} ->
            send_response(Sock, 403, <<"Forbidden">>, <<>>);
        {error, bad_request} ->
            send_response(Sock, 400, <<"Bad Request">>, <<>>)
    end.

handle_request_ok(Sock, Table, "PUT", Path, Body) ->
    ets:insert(Table, {Path, Body}),
    send_response(Sock, 200, <<"OK">>, <<>>);
handle_request_ok(Sock, Table, "GET", Path, _Body) ->
    case ets:lookup(Table, Path) of
        [{_, Data}] ->
            send_response(Sock, 200, <<"OK">>, Data);
        [] ->
            send_response(Sock, 404, <<"Not Found">>, <<>>)
    end;
handle_request_ok(Sock, Table, "DELETE", Path, _Body) ->
    ets:delete(Table, Path),
    send_response(Sock, 204, <<"No Content">>, <<>>);
handle_request_ok(Sock, _Table, _Method, _Path, _Body) ->
    send_response(Sock, 400, <<"Bad Request">>, <<>>).

validate_signature(Method, Path, Query, Headers, Body) ->
    case header_value("authorization", Headers) of
        {ok, Auth} ->
            case Auth of
                <<"AWS4-HMAC-SHA256 ", _/binary>> ->
                    validate_sigv4(Method, Path, Query, Headers, Body, Auth);
                <<"AWS ", _/binary>> ->
                    validate_sigv2(Method, Path, Headers, Body, Auth);
                _ ->
                    {error, bad_request}
            end;
        error ->
            {error, bad_request}
    end.

validate_sigv2(Method, Path, Headers, _Body, Auth) ->
    case parse_sigv2_auth(Auth) of
        {ok, Access, Signature} when Access =:= <<"test-access">> ->
            Date = header_value_or("date", Headers, <<>>),
            ContentType = header_value_or("content-type", Headers, <<"">>),
            ContentMD5 = header_value_or("content-md5", Headers, <<"">>),
            Sig = s3filez_sigv2:sign(
                #{password => <<"test-secret">>},
                list_to_atom(string:lowercase(Method)),
                ContentMD5,
                ContentType,
                Date,
                normalize_headers(Headers),
                <<"127.0.0.1">>,
                Path
            ),
            case Signature =:= Sig of
                true -> ok;
                false -> {error, forbidden}
            end;
        _ ->
            {error, forbidden}
    end.

validate_sigv4(Method, Path, Query, Headers, _Body, Auth) ->
    case parse_sigv4_auth(Auth) of
        {ok, Access, SignedHeaders, Signature} when Access =:= <<"test-access">> ->
            case header_value("x-amz-date", Headers) of
                {ok, AmzDate} ->
                    PayloadHash = header_value_or("x-amz-content-sha256", Headers, <<"">>),
                    CanonicalHeaders = signed_header_list(SignedHeaders, Headers),
                    CanonicalRequest = iolist_to_binary([
                        string:uppercase(Method), $\n,
                        s3filez_sigv4:canonical_uri(Path), $\n,
                        s3filez_sigv4:canonical_query_string(Query), $\n,
                        element(1, s3filez_sigv4:canonical_headers(CanonicalHeaders)), $\n,
                        SignedHeaders, $\n,
                        PayloadHash
                    ]),
                    DateStamp = binary:part(AmzDate, 0, 8),
                    StringToSign = s3filez_sigv4:string_to_sign(
                        #{region => "us-east-1", password => <<"test-secret">>},
                        binary_to_list(AmzDate),
                        binary_to_list(DateStamp),
                        CanonicalRequest
                    ),
                    Sig = s3filez_sigv4:signature(
                        #{region => "us-east-1", password => <<"test-secret">>},
                        binary_to_list(DateStamp),
                        StringToSign
                    ),
                    case list_to_binary(Sig) =:= Signature of
                        true -> ok;
                        false -> {error, forbidden}
                    end;
                error ->
                    {error, bad_request}
            end;
        _ ->
            {error, forbidden}
    end.

parse_sigv2_auth(<<"AWS ", Rest/binary>>) ->
    case binary:split(Rest, <<":">>) of
        [Access, Sig] -> {ok, Access, Sig};
        _ -> error
    end;
parse_sigv2_auth(_) ->
    error.

parse_sigv4_auth(<<"AWS4-HMAC-SHA256 ", Rest/binary>>) ->
    Parts = binary:split(Rest, <<",">>, [global]),
    case {find_param(<<"Credential">>, Parts), find_param(<<"SignedHeaders">>, Parts), find_param(<<"Signature">>, Parts)} of
        {{ok, Cred}, {ok, SignedHeaders}, {ok, Sig}} ->
            case binary:split(Cred, <<"/">>) of
                [Access | _] -> {ok, Access, SignedHeaders, Sig};
                _ -> error
            end;
        _ ->
            error
    end;
parse_sigv4_auth(_) ->
    error.

find_param(Key, Parts) ->
    Prefix = <<Key/binary, "=">>,
    case lists:dropwhile(fun(P) -> not has_prefix(P, Prefix) end, Parts) of
        [P | _] ->
            {ok, binary:part(P, byte_size(Prefix), byte_size(P) - byte_size(Prefix))};
        [] ->
            error
    end.

has_prefix(Bin, Prefix) ->
    case binary:match(Bin, Prefix) of
        {0, _} -> true;
        _ -> false
    end.

signed_header_list(SignedHeaders, Headers) ->
    Names = binary:split(SignedHeaders, <<";">>, [global]),
    lists:foldl(
        fun(Name, Acc) ->
            case header_value(binary_to_list(Name), Headers) of
                {ok, V} -> [{binary_to_list(Name), V} | Acc];
                error -> Acc
            end
        end,
        [],
        Names
    ).

header_value_or(Name, Headers, Default) ->
    case header_value(Name, Headers) of
        {ok, V} -> V;
        error -> Default
    end.

header_value(Name, Headers) ->
    case lists:keyfind(Name, 1, Headers) of
        {_, V} -> {ok, V};
        false -> error
    end.

normalize_headers(Headers) ->
    [ {K, V} || {K, V} <- Headers ].

send_response(Sock, Code, Reason, Body) ->
    Len = byte_size(Body),
    Resp = [
        "HTTP/1.1 ", integer_to_list(Code), " ", binary_to_list(Reason), "\r\n",
        "Content-Length: ", integer_to_list(Len), "\r\n",
        "Connection: close\r\n",
        "\r\n",
        Body
    ],
    gen_tcp:send(Sock, Resp).
