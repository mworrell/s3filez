%% @author Marc Worrell
%% @copyright 2013-2014 Marc Worrell

%% Copyright 2014 Marc Worrell
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

-module(s3filez).

-export([
    queue_get/3,
    queue_get_id/4,
    queue_put/3,
    queue_put/4,
    queue_put/5,
    queue_put_id/5,
    queue_delete/2,
    queue_delete/3,
    queue_delete_id/4,

    queue_stream/3,
    queue_stream_id/4,

    get/2,
    delete/2,
    put/3,
    put/4,
    stream/3
    ]).

-export([
    put_body_file/1,
    stream_loop/4
    ]).

-define(BLOCK_SIZE, 65536).
-define(CONNECT_TIMEOUT, 30000).    % 30s
-define(TIMEOUT, 1800000).          % 30m

-type config() :: {binary(), binary()}.
-type url() :: binary().
-type ready_fun() :: undefined | {atom(),atom(),list()} | fun() | pid().
-type stream_fun() :: {atom(),atom(),list()} | fun() | pid().
-type put_data() :: {data, binary()} | {filename, pos_integer(), file:filename()}.
-type queue_reply() :: {ok, any(), pid()} | {error, {already_started, pid()}}.

-type put_opts() :: [put_opt()].
-type put_opt() :: {acl, acl_type()} | {content_type, string()}.
-type acl_type() :: private | public_read | public_read_write | authenticated_read | bucket_owner_read | bucket_owner_full_control.


%%% Queuing functions, starts a process and signals the ready_fun() when ready.

-spec queue_get(config(), url(), ready_fun()) -> queue_reply().

queue_get(Config, Url, ReadyFun) ->
    s3filez_jobs_sup:queue({get, Config, Url, ReadyFun}).


-spec queue_get_id(any(), config(), url(), ready_fun()) -> queue_reply().

queue_get_id(JobId, Config, Url, ReadyFun) ->
    s3filez_jobs_sup:queue(JobId, {get, Config, Url, ReadyFun}).

-spec queue_put(config(), url(), put_data()) -> queue_reply().

queue_put(Config, Url, What) ->
    queue_put(Config, Url, What, undefined).

-spec queue_put(config(), url(), put_data(), ready_fun()) -> queue_reply().

queue_put(Config, Url, What, ReadyFun) ->
    queue_put(Config, Url, What, ReadyFun, []).

-spec queue_put(config(), url(), put_data(), ready_fun(), put_opts()) -> queue_reply().

queue_put(Config, Url, What, ReadyFun, Opts) ->
    s3filez_jobs_sup:queue({put, Config, Url, What, ReadyFun, Opts}).

-spec queue_put_id(any(), config(), url(), put_data(), ready_fun()) -> queue_reply().

queue_put_id(JobId, Config, Url, What, ReadyFun) ->
    s3filez_jobs_sup:queue(JobId, {put, Config, Url, What, ReadyFun}).

-spec queue_delete(config(), url()) -> queue_reply().

queue_delete(Config, Url) ->
    queue_delete(Config, Url, undefined).

-spec queue_delete(config(), url(), ready_fun()) -> queue_reply().

queue_delete(Config, Url, ReadyFun) ->
    s3filez_jobs_sup:queue({delete, Config, Url, ReadyFun}).

-spec queue_delete_id(any(), config(), url(), ready_fun()) -> queue_reply().

queue_delete_id(JobId, Config, Url, ReadyFun) ->
    s3filez_jobs_sup:queue(JobId, {delete, Config, Url, ReadyFun}).

-spec queue_stream(config(), url(), stream_fun()) -> queue_reply().

queue_stream(Config, Url, StreamFun) ->
    s3filez_jobs_sup:queue({stream, Config, Url, StreamFun}).

-spec queue_stream_id(any(), config(), url(), stream_fun()) -> queue_reply().

queue_stream_id(JobId, Config, Url, StreamFun) ->
    s3filez_jobs_sup:queue(JobId, {stream, Config, Url, StreamFun}).


%%% Nornal API - blocking on the process

get(Config, Url) ->
    Result = jobs:run(s3filez_jobs, fun() -> request(Config, get, Url, [], []) end),
    case Result of
        {ok, {{_Http, 200, _Ok}, Headers, Body}} ->
            {ok, ct(Headers), Body};
        Other ->
            ret_status(Other)
    end.

delete(Config, Url) ->
    ret_status(jobs:run(s3filez_jobs, fun() -> request(Config, delete, Url, [], []) end)).

put(Config, Url, Payload) ->
    put(Config, Url, Payload, []).

put(Config, Url, {data, Data}, Opts) ->
    Ctx1 = crypto:hash_update(crypto:hash_init(md5), Data),
    Hash = base64:encode(crypto:hash_final(Ctx1)),
    Hs = [
          {"Content-MD5", binary_to_list(Hash)}
          | opts_to_headers(Opts)
    ],
    ret_status(request_with_body(Config, put, Url, Hs, Data));

put(Config, Url, {filename, Size, Filename}, Opts) ->
    Hash = base64:encode(checksum(Filename)),
    Hs = [
          {"Content-MD5", binary_to_list(Hash)},
          {"Content-Length", integer_to_list(Size)}
          | opts_to_headers(Opts)
    ],
    ret_status(request_with_body(Config, put, Url, Hs, {fun ?MODULE:put_body_file/1, {file, Filename}})).

put_body_file({file, Filename}) ->
    {ok, FD} = file:open(Filename, [read,binary]),
    put_body_file({fd, FD});
put_body_file({fd, FD}) ->
    case file:read(FD, ?BLOCK_SIZE) of
        eof ->
            file:close(FD),
            eof;
        {ok, Data} ->
            {ok, Data, {fd, FD}}
    end.

opts_to_headers(Opts) ->
    Hs = lists:foldl(
           fun({acl, AclOption}, Hs) ->
                   [{"x-amz-acl", encode_acl(AclOption)} | Hs];
              ({content_type, CT}, Hs) ->
                   [{"Content-Type", CT} | Hs];
              (Unknown, _) ->
                   throw({error, {unknown_option, Unknown}})
           end,
           [],
           Opts),
    case proplists:get_value("Content-Type", Hs) of
        undefined ->
            [{"Content-Type", "binary/octet-stream"} | Hs];
        _ ->
            Hs
    end.

encode_acl(private)                   -> "private";
encode_acl(public_read)               -> "public-read";
encode_acl(public_read_write)         -> "public-read-write";
encode_acl(authenticated_read)        -> "authenticated-read";
encode_acl(bucket_owner_read)         -> "bucket-owner-read";
encode_acl(bucket_owner_full_control) -> "bucket-owner-full-control".

    
  
%%% Stream the contents of the url to the function, callback or to the httpc-streaming option.

stream(Config, Url, Fun) when is_function(Fun,1) ->
    stream_to_fun(Config, Url, Fun);
stream(Config, Url, {_M,_F,_A} = MFA) ->
    stream_to_fun(Config, Url, MFA);
stream(Config, Url, HttpcStreamOption) ->
    request(Config, get, Url, [], [{stream,HttpcStreamOption}, {sync,false}]).

stream_to_fun(Config, Url, Fun) ->
    {ok, RequestId} = request(Config, get, Url, [], [{stream,{self,once}}, {sync,false}]),
    receive
        {http, {RequestId, stream_start, Headers, Pid}} ->
            call_fun(Fun, {content_type, ct(Headers)}),
            httpc:stream_next(Pid),
            ?MODULE:stream_loop(RequestId, Pid, Url, Fun);
        {http, {RequestId, {_,_,_} = HttpRet}}->
            Status = http_status(HttpRet),
            call_fun(Fun, Status),
            Status;
        {http, {RequestId, Other}} ->
            error_logger:error_msg("Unexpected HTTP msg for ~p: ~p", [Url,Other]),
            {error, Other}
    after ?CONNECT_TIMEOUT ->
        call_fun(Fun, {error, timeout}),
        {error, timeout}
    end.

stream_loop(RequestId, Pid, Url, Fun) ->
    receive
        {http, {RequestId, stream_end, Headers}} ->
            call_fun(Fun, {headers, Headers}),
            call_fun(Fun, eof),
            ok;
        {http, {RequestId, stream, Data}} ->
            call_fun(Fun, Data),
            httpc:stream_next(Pid),
            ?MODULE:stream_loop(RequestId, Pid, Url, Fun);
        {http, {RequestId, Other}} ->
            error_logger:error_msg("Unexpected HTTP msg for ~p: ~p", [Url,Other]),
            call_fun(Fun, {error, Other}),
            {error, Other}
    after ?TIMEOUT ->
        call_fun(Fun, timeout),
        {error, {error, timeout}}
    end.

call_fun({M,F,A}, Arg) ->
    erlang:apply(M,F,A++[Arg]);
call_fun(Fun, Arg) when is_function(Fun) ->
    Fun(Arg).

ret_status({ok, Rest}) ->
    http_status(Rest);
ret_status({error, _} = Error) ->
    Error.

http_status({{_,Code,_}, _Headers, _Body}) when Code =:= 200; Code =:= 204; Code =:= 206 ->
    ok;
http_status({{_,404,_}, _Headers, _Body}) ->
    {error, enoent};
http_status({{_,403,_}, _Headers, _Body}) ->
    {error, forbidden};
http_status({{_,Code,_}, _Headers, _Body}) ->
    {error, Code}.


request({Key,_} = Config, Method, Url, Headers, Options) ->
    {_Scheme, Host, Path} = urlsplit(Url),
    Date = httpd_util:rfc1123_date(),
    Signature = sign(Config, Method, "", "", Date, Headers, Host, Path),
    AllHeaders = [
        {"Authorization", lists:flatten(["AWS ",binary_to_list(Key),":",binary_to_list(Signature)])},   
        {"Date", Date} | Headers
    ],
    httpc:request(Method, {binary_to_list(Url), AllHeaders}, opts(), [{body_format, binary}|Options]). 

request_with_body({Key,_} = Config, Method, Url, Headers, Body) ->
    {_Scheme, Host, Path} = urlsplit(Url),
    {"Content-Type", ContentType} = proplists:lookup("Content-Type", Headers),
    {"Content-MD5", ContentMD5} = proplists:lookup("Content-MD5", Headers),
    Date = httpd_util:rfc1123_date(),
    Signature = sign(Config, Method, ContentMD5, ContentType, Date, Headers, Host, Path),
    Hs1 = [
        {"Authorization", lists:flatten(["AWS ",binary_to_list(Key),":",binary_to_list(Signature)])},   
        {"Date", Date}
        | Headers
    ],
    jobs:run(s3filez_jobs,
             fun() ->
                httpc:request(Method, {binary_to_list(Url), Hs1, ContentType, Body}, opts(), [])
            end).


opts() ->
    [
        {connect_timeout, ?CONNECT_TIMEOUT},
        {timeout, ?TIMEOUT}
    ].

sign({_Key,Secret}, Method, BodyMD5, ContentType, Date, Headers, Host, Path) ->
    ResourcePrefix =
        case lists:reverse(Split=binary:split(Host, <<".">>, [global])) of
            [<<"com">>, <<"amazonaws">> | _] ->
                ["/", hd(Split)];
            _ ->
                []
        end,
    Data = [
            method_string(Method), $\n,
            BodyMD5, $\n,
            ContentType, $\n,
            Date, $\n,
            canonicalize_amz_headers(Headers),
            iolist_to_binary([ResourcePrefix, Path])
           ],
    base64:encode(crypto:sha_mac(Secret,Data)).

method_string('put') -> "PUT";
method_string('get') -> "GET";
method_string('delete') -> "DELETE";
method_string('post') -> "POST";
method_string('head') -> "HEAD".

canonicalize_amz_headers(Headers) ->
    AmzHeaders = lists:sort(lists:filter(fun({"x-amz-" ++ _, _}) -> true; (_) -> false end, Headers)),
    [
     [
      H, $:, V, $\n
     ]
     || {H, V} <- AmzHeaders
    ].    

ct(Headers) ->
    list_to_binary(proplists:get_value("content-type", Headers, "binary/octet-stream")).

-spec checksum(file:filename()) -> binary().
checksum(Filename) ->
    Ctx = crypto:hash_init(md5),
    {ok, FD} = file:open(Filename, [read,binary]),
    Ctx1 = checksum1(Ctx, FD),
    file:close(FD),
    crypto:hash_final(Ctx1).

checksum1(Ctx, FD) ->
    case file:read(FD, ?BLOCK_SIZE) of
        eof ->
            Ctx;
        {ok, Data} ->
            checksum1(crypto:hash_update(Ctx, Data), FD)
    end.


urlsplit(Url) ->
    case binary:split(Url, <<":">>) of
        [Scheme, <<"//", HostPath/binary>>] ->
            {Host,Path} = urlsplit_hostpath(HostPath),
            {Scheme, Host, Path};
        [<<"//", HostPath/binary>>] ->
            {Host,Path} = urlsplit_hostpath(HostPath),
            {no_scheme, Host, Path};
        [Path] ->
            {no_scheme, <<>>, Path}
    end.

urlsplit_hostpath(HP) ->
    case binary:split(HP, <<"/">>) of
        [Host,Path] -> {Host,<<"/", Path/binary>>};
        [Host] -> {Host, <<"/">>}
    end.
