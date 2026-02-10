%% @author Marc Worrell
%% @copyright 2026 Marc Worrell
%% @doc AWS Signature Version 4 helpers for s3filez.
%% @end

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
-module(s3filez_sigv4).

-export([
    required_config/1,
    amz_date/0,
    headers/5,
    canonical_headers/1,
    canonical_query_string/1,
    canonical_uri/1,
    payload_hash/1,
    build_request/7,
    string_to_sign/4,
    signature/3,
    authorization/4
]).

-type header() :: {string() | binary(), string() | binary()}.

-define(BLOCK_SIZE, 65536).

-spec required_config(map()) -> ok | {error, region_needed}.
required_config(Config) ->
    case maps:get(region, Config, undefined) of
        undefined -> {error, region_needed};
        _ -> ok
    end.

-spec amz_date() -> string().
amz_date() ->
    {{Y,M,D},{H,Mi,S}} = calendar:universal_time(),
    lists:flatten(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0BZ", [Y,M,D,H,Mi,S])).

-spec headers(binary() | string(), string(), string(), [header()], map()) -> [header()].
headers(Host, AmzDate, PayloadHash, Headers, Config) ->
    Base = [
        {"Host", to_list(Host)},
        {"x-amz-date", AmzDate},
        {"x-amz-content-sha256", PayloadHash}
        | Headers
    ],
    case maps:get(session_token, Config, undefined) of
        undefined -> Base;
        Token -> [{"x-amz-security-token", to_list(Token)} | Base]
    end.

-spec canonical_headers([header()]) -> {binary(), string()}.
canonical_headers(Headers) ->
    Normalized = [ normalize_header(H, V) || {H, V} <- Headers ],
    Combined = combine_headers(Normalized),
    Sorted = lists:sort(Combined),
    Canonical = [
        [Name, $:, Value, $\n]
        || {Name, Value} <- Sorted
    ],
    Signed = string:join([Name || {Name, _} <- Sorted], ";"),
    {iolist_to_binary(Canonical), Signed}.

-spec canonical_query_string(binary()) -> string().
canonical_query_string(<<>>) ->
    "";
canonical_query_string(Query) ->
    Parts = binary:split(Query, <<"&">>, [global]),
    Pairs = [
        case binary:split(P, <<"=">>) of
            [K, V] -> {uri_encode(K), uri_encode(V)};
            [K] -> {uri_encode(K), <<>>}
        end
        || P <- Parts, P =/= <<>>
    ],
    Sorted = lists:sort(Pairs),
    Joined = lists:flatten(
        string:join(
            [binary_to_list(K) ++ "=" ++ binary_to_list(V) || {K, V} <- Sorted],
            "&"
        )
    ),
    Joined.

-spec canonical_uri(binary()) -> binary().
canonical_uri(Path) ->
    uri_encode_path(Path).

-spec build_request(
    map(),
    atom(),
    binary(),
    binary(),
    binary(),
    [header()],
    string()
) -> {string(), [header()]}.
build_request(Config, Method, Host, Path, Query, Headers, PayloadHash) ->
    AmzDate = amz_date(),
    DateStamp = lists:sublist(AmzDate, 8),
    AllHeaders = headers(Host, AmzDate, PayloadHash, Headers, Config),
    {CanonicalHeaders, SignedHeaders} = canonical_headers(AllHeaders),
    CanonicalQuery = canonical_query_string(Query),
    CanonicalRequest = iolist_to_binary([
        method_string(Method), $\n,
        canonical_uri(Path), $\n,
        CanonicalQuery, $\n,
        CanonicalHeaders, $\n,
        SignedHeaders, $\n,
        PayloadHash
    ]),
    StringToSign = string_to_sign(Config, AmzDate, DateStamp, CanonicalRequest),
    Signature = signature(Config, DateStamp, StringToSign),
    Authorization = authorization(Config, DateStamp, SignedHeaders, Signature),
    {Authorization, AllHeaders}.

-spec payload_hash(term()) -> string().
payload_hash(Body) when is_binary(Body) ->
    sha256_hex(Body);
payload_hash({Fun, {file, Filename}}) when is_function(Fun, 1) ->
    bin_to_hex(checksum_sha256(Filename));
payload_hash(_Body) ->
    "UNSIGNED-PAYLOAD".

-spec string_to_sign(map(), string(), string(), binary()) -> binary().
string_to_sign(Config, AmzDate, DateStamp, CanonicalRequest) ->
    Region = to_list(maps:get(region, Config)),
    Scope = string:join([DateStamp, Region, "s3", "aws4_request"], "/"),
    CanonicalHash = bin_to_hex(crypto:hash(sha256, CanonicalRequest)),
    iolist_to_binary([
        "AWS4-HMAC-SHA256\n",
        AmzDate, "\n",
        Scope, "\n",
        CanonicalHash
    ]).

-spec signature(map(), string(), binary()) -> string().
signature(Config, DateStamp, StringToSign) ->
    Region = to_list(maps:get(region, Config)),
    Secret = to_list(maps:get(password, Config)),
    KDate = crypto:mac(hmac, sha256, "AWS4" ++ Secret, DateStamp),
    KRegion = crypto:mac(hmac, sha256, KDate, Region),
    KService = crypto:mac(hmac, sha256, KRegion, "s3"),
    KSigning = crypto:mac(hmac, sha256, KService, "aws4_request"),
    bin_to_hex(crypto:mac(hmac, sha256, KSigning, StringToSign)).

-spec authorization(map(), string(), string(), string()) -> string().
authorization(Config, DateStamp, SignedHeaders, Signature) ->
    Key = to_list(maps:get(username, Config)),
    Region = to_list(maps:get(region, Config)),
    Scope = string:join([DateStamp, Region, "s3", "aws4_request"], "/"),
    lists:flatten([
        "AWS4-HMAC-SHA256 Credential=", Key, "/", Scope,
        ",SignedHeaders=", SignedHeaders,
        ",Signature=", Signature
    ]).

uri_encode_path(Path) ->
    %% Preserve '/' in paths, encode other bytes
    Parts = binary:split(Path, <<"/">>, [global]),
    Encoded = [uri_encode(P) || P <- Parts],
    iolist_to_binary(string:join([binary_to_list(P) || P <- Encoded], "/")).

uri_encode(Bin) ->
    z_url:percent_encode(Bin).

normalize_header(H, V) ->
    Name = string:lowercase(to_list(H)),
    Value = normalize_header_value(to_list(V)),
    {Name, Value}.

normalize_header_value(Value) ->
    Trimmed = string:trim(Value),
    re:replace(Trimmed, "\\s+", " ", [global, {return, list}]).

combine_headers(Headers) ->
    lists:foldl(
      fun({Name, Value}, Acc) ->
              case lists:keyfind(Name, 1, Acc) of
                  {Name, Existing} ->
                      lists:keyreplace(Name, 1, Acc, {Name, Existing ++ "," ++ Value});
                  false ->
                      [{Name, Value} | Acc]
              end
      end, [], Headers).

sha256_hex(Bin) ->
    bin_to_hex(crypto:hash(sha256, Bin)).

bin_to_hex(Bin) ->
    lists:flatten([io_lib:format("~2.16.0b", [B]) || <<B>> <= Bin]).

checksum_sha256(Filename) ->
    Ctx = crypto:hash_init(sha256),
    {ok, FD} = file:open(Filename, [read,binary]),
    Ctx1 = checksum_sha256_1(Ctx, FD),
    file:close(FD),
    crypto:hash_final(Ctx1).

checksum_sha256_1(Ctx, FD) ->
    case file:read(FD, ?BLOCK_SIZE) of
        eof ->
            Ctx;
        {ok, Data} ->
            checksum_sha256_1(crypto:hash_update(Ctx, Data), FD)
    end.

to_list(B) when is_binary(B) -> binary_to_list(B);
to_list(L) when is_list(L) -> L.

method_string('put') -> "PUT";
method_string('get') -> "GET";
method_string('delete') -> "DELETE".
