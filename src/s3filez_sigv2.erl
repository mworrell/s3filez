%% @author Marc Worrell
%% @copyright 2013-2026 Marc Worrell
%% @doc AWS Signature Version 2 helpers for s3filez.
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

-module(s3filez_sigv2).

-export([
    sign/8
]).

-spec sign(
    map(),
    atom(),
    iodata(),
    iodata(),
    iodata(),
    list(),
    binary(),
    binary()
) -> binary().
sign(#{ password := Secret }, Method, BodyMD5, ContentType, Date, Headers, Host, Path) ->
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
    base64:encode(crypto:mac(hmac, sha, Secret, Data)).

method_string('put') -> "PUT";
method_string('get') -> "GET";
method_string('delete') -> "DELETE".

canonicalize_amz_headers(Headers) ->
    AmzHeaders = lists:sort(lists:filter(fun({"x-amz-" ++ _, _}) -> true; (_) -> false end, Headers)),
    [
     [
      H, $:, V, $\n
     ]
     || {H, V} <- AmzHeaders
    ].
