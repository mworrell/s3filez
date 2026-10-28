%% @author Marc Worrell
%% @copyright 2026 Marc Worrell
%% @doc Multipart XML helpers for s3filez.
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
-module(s3filez_multipart).

-export([completion_body/1, response/3]).

%% @doc Validate and sort the caller's part manifest before serializing it.
-spec completion_body(term()) -> {ok, binary()} | {error, invalid_parts}.
completion_body(Parts) when is_list(Parts), Parts =/= [] ->
    Valid = lists:all(fun
        ({N, ETag}) when is_integer(N), N >= 1, N =< 10000,
                        is_binary(ETag), byte_size(ETag) > 0 -> true;
        (_) -> false
    end, Parts),
    case Valid andalso length(lists:ukeysort(1, Parts)) =:= length(Parts) of
        true ->
            {ok, iolist_to_binary([
                "<CompleteMultipartUpload>",
                [["<Part><PartNumber>", integer_to_binary(N),
                  "</PartNumber><ETag>", escape(ETag), "</ETag></Part>"]
                 || {N, ETag} <- lists:keysort(1, Parts)],
                "</CompleteMultipartUpload>"
            ])};
        false -> {error, invalid_parts}
    end;
completion_body(_) ->
    {error, invalid_parts}.

escape(Value) ->
    lists:foldl(fun({From, To}, Acc) ->
        binary:replace(Acc, From, To, [global])
    end, Value, [{<<"&">>, <<"&amp;">>}, {<<"<">>, <<"&lt;">>},
                 {<<">">>, <<"&gt;">>}, {<<"\"">>, <<"&quot;">>},
                 {<<"'">>, <<"&apos;">>}]).

%% @doc Read the expected response or an embedded S3 error. SAX keeps remote
%% element names as strings instead of allocating atoms. DTDs are rejected.
-spec response(iodata(), string(), string()) -> {ok, binary()} | {error, term()}.
response(Body, Root, Field) ->
    try xmerl_sax_parser:stream(iolist_to_binary(Body), [
        {event_fun, fun xml_event/3},
        {event_state, {[], #{}}}
    ]) of
        {ok, {[], Values}, Rest} ->
            case string:trim(Rest) of
                <<>> -> response_value(Values, Root, Field);
                [] -> response_value(Values, Root, Field);
                _ -> {error, invalid_multipart_response}
            end;
        _ -> {error, invalid_multipart_response}
    catch
        _:_ -> {error, invalid_multipart_response}
    end.

response_value(#{root := "Error"} = Values, _Root, _Field) ->
    {error, {s3, value(["Code", "Error"], Values), value(["Message", "Error"], Values)}};
response_value(#{root := Root} = Values, Root, Field) ->
    case value([Field, Root], Values) of
        <<>> -> {error, invalid_multipart_response};
        Value -> {ok, Value}
    end;
response_value(_Values, _Root, _Field) ->
    {error, invalid_multipart_response}.

value(Path, Values) ->
    unicode:characters_to_binary(lists:reverse(maps:get(Path, Values, []))).

xml_event({startDTD, _, _, _}, _Location, _State) ->
    throw(dtd_not_allowed);
xml_event({startElement, _, Name, _, _}, _Location, {[], Values}) ->
    {[Name], Values#{root => Name}};
xml_event({startElement, _, Name, _, _}, _Location, {Path, Values}) ->
    {[Name | Path], Values};
xml_event({endElement, _, _, _}, _Location, {[_ | Path], Values}) ->
    {Path, Values};
xml_event({characters, Text}, _Location, {Path, Values}) ->
    {Path, maps:update_with(Path, fun(Old) -> [Text | Old] end, [Text], Values)};
xml_event(_Event, _Location, State) ->
    State.
