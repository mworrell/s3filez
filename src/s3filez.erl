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
    get/2,
    delete/2,
    put/3,
    stream/3
    ]).

-export([
    put_body_file/1,
    stream_loop/3
    ]).

-define(BLOCK_SIZE, 65536).
-define(TIMEOUT, 30000).

get(Config, Url) ->
    case request(Config, get, Url, []) of
        {ok, {{_Http, 200, _Ok}, Headers, Body}} ->
            {ok, ct(Headers), Body};
        Other ->
            Other
    end.

delete(Config, Url) ->
    request(Config, delete, Url, []).

put(Config, Url, {data, Data}) ->
    Ctx1 = crypto:hash_update(crypto:hash_init(md5), Data),
    Hash = base64:encode(crypto:hash_final(Ctx1)),
    Hs = [
        {"Content-Type", "binary/octet-stream"},
        {"Content-MD5", binary_to_list(Hash)}
    ],
    request_with_body(Config, put, Url, Hs, Data);
put(Config, Url, {filename, Size, Filename}) ->
    Hash = base64:encode(checksum(Filename)),
    Hs = [
        {"Content-Type", "binary/octet-stream"},
        {"Content-MD5", binary_to_list(Hash)},
        {"Content-Length", integer_to_list(Size)}
    ],
    request_with_body(Config, put, Url, Hs, {fun ?MODULE:put_body_file/1, {file, Filename}}).

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

stream(Config, Url, Fun) when is_function(Fun,1) ->
    {ok, RequestId} = request(Config, get, Url, [{stream,{self,once}}, {sync,false}]),
    receive
        {http, {RequestId, stream_start, Headers, Pid}} ->
            Fun({content_type, ct(Headers)}),
            httpc:stream_next(Pid),
            ?MODULE:stream_loop(RequestId, Pid, Fun)
    after ?TIMEOUT ->
        Fun(timeout),
        {error, timeout}
    end.

stream_loop(RequestId, Pid, Fun) ->
    receive
        {http, {RequestId, stream_end, _Headers}} ->
            Fun(eof),
            ok;
        {http, {RequestId, stream, Data}} ->
            Fun(Data),
            httpc:stream_next(Pid),
            ?MODULE:stream_loop(RequestId, Pid, Fun)
    after ?TIMEOUT ->
        Fun(timeout),
        {error, timeout}
    end.

request({Key,_} = Config, Method, Url, Options) ->
    {_Scheme, _Host, Path} = urlsplit(Url),
    Date = httpd_util:rfc1123_date(),
    Signature = sign(Config, Method, "", "", Date, Path),
    Headers = [
        {"Authorization", lists:flatten(["AWS ",binary_to_list(Key),":",binary_to_list(Signature)])},   
        {"Date", Date}
    ],
    httpc:request(Method, {binary_to_list(Url), Headers}, [{connect_timeout, ?TIMEOUT}], [{body_format, binary}|Options]). 

request_with_body({Key,_} = Config, Method, Url, Headers, Body) ->
    {_Scheme, _Host, Path} = urlsplit(Url),
    {"Content-Type", ContentType} = proplists:lookup("Content-Type", Headers),
    {"Content-MD5", ContentMD5} = proplists:lookup("Content-MD5", Headers),
    Date = httpd_util:rfc1123_date(),
    Signature = sign(Config, Method, ContentMD5, ContentType, Date, Path),
    Hs1 = [
        {"Authorization", lists:flatten(["AWS ",binary_to_list(Key),":",binary_to_list(Signature)])},   
        {"Date", Date}
        | Headers
    ],
    httpc:request(Method, {binary_to_list(Url), Hs1, ContentType, Body}, [{connect_timeout, ?TIMEOUT}], []). 


sign({_Key,Secret}, Method, BodyMD5, ContentType, Date, Path) ->
    Data = [
        method_string(Method), $\n,
        BodyMD5, $\n,
        ContentType, $\n,
        Date, $\n,
        Path
    ],
    base64:encode(crypto:sha_mac(Secret,Data)).

method_string('put') -> "PUT";
method_string('get') -> "GET";
method_string('delete') -> "DELETE";
method_string('post') -> "POST";
method_string('head') -> "HEAD".

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
