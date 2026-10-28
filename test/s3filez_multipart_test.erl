%% @author Marc Worrell
%% @copyright 2013-2026 Marc Worrell
%% @doc Multipart upload unit tests for s3filez.
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

-module(s3filez_multipart_test).

-include_lib("eunit/include/eunit.hrl").

completion_body_test() ->
    ?assertEqual({ok, <<"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>",
                       "<ETag>a&amp;&lt;&gt;&quot;&apos;</ETag></Part>",
                       "<Part><PartNumber>2</PartNumber><ETag>b</ETag></Part>",
                       "</CompleteMultipartUpload>">>},
        s3filez_multipart:completion_body([{2, <<"b">>}, {1, <<"a&<>\"'">>}])),
    [?assertEqual({error, invalid_parts}, s3filez_multipart:completion_body(Parts))
     || Parts <- [[], [{0, <<"a">>}], [{10001, <<"a">>}], [{1, <<>>}],
                  [{1, <<"a">>}, {1, <<"b">>}], [invalid], invalid]].

response_test() ->
    ?assertEqual({ok, <<"id+/=&">>}, response(
        <<"<?xml version=\"1.0\"?><InitiateMultipartUploadResult xmlns=\"urn:s3\">",
          "<UploadId>id+/=&amp;</UploadId></InitiateMultipartUploadResult>">>)),
    ?assertEqual({error, {s3, <<"InvalidPart">>, <<"Bad & missing">>}}, response(
        <<" \n<Error><Code>InvalidPart</Code><Message>Bad &amp; missing</Message></Error>">>)),
    [?assertEqual({error, invalid_multipart_response}, response(Xml)) || Xml <- [
        <<>>, <<"<broken>">>, <<"<Other><UploadId>a</UploadId></Other>">>,
        <<"<InitiateMultipartUploadResult/>">>,
        <<"<!DOCTYPE x [<!ENTITY id 'bad'>]><InitiateMultipartUploadResult>",
          "<UploadId>&id;</UploadId></InitiateMultipartUploadResult>">>
    ]].

response(Xml) ->
    s3filez_multipart:response(Xml, "InitiateMultipartUploadResult", "UploadId").

v2_multipart_signature_test() ->
    Config = #{password => <<"secret">>},
    Expected = base64:encode(crypto:mac(hmac, sha, <<"secret">>,
        <<"POST\n\napplication/xml\ndate\n/bucket/key?partNumber=2&uploadId=a%2Bb%2F%3D">>)),
    ?assertEqual(Expected, s3filez_sigv2:sign(Config, post, <<>>, "application/xml",
        "date", [], <<"localhost">>, <<"/bucket/key">>,
        <<"uploadId=a%2Bb%2F%3D&ignored=yes&partNumber=2">>)),
    ?assertEqual(base64:encode(crypto:mac(hmac, sha, <<"secret">>,
        <<"POST\n\n\ndate\n/bucket/key?uploads">>)),
        s3filez_sigv2:sign(Config, post, <<>>, <<>>, "date", [],
            <<"localhost">>, <<"/bucket/key">>, <<"uploads">>)).

v4_multipart_query_test() ->
    ?assertEqual("partNumber=2&uploadId=a%2Bb%2F%3D%25%20%26",
        s3filez_sigv4:canonical_query_string(
            <<"uploadId=a%2Bb%2f%3D%25%20%26&partNumber=2">>)),
    ?assertEqual("uploads=", s3filez_sigv4:canonical_query_string(<<"uploads">>)).
