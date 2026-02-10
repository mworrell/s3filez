%% @author Marc Worrell
%% @copyright 2013-2026 Marc Worrell
%% @doc SigV4 unit tests for s3filez.
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

-module(s3filez_v4_test).

-include_lib("eunit/include/eunit.hrl").

v4_missing_region_test() ->
    ?assertEqual({error, region_needed}, s3filez_sigv4:required_config(#{})),
    ?assertEqual({error, region_needed}, s3filez_sigv4:required_config(#{signature_version => v4})),
    ?assertEqual(ok, s3filez_sigv4:required_config(#{region => "us-east-1"})).

v4_example_get_bucket_lifecycle_test() ->
    %% Example from AWS Signature Version 4 header-based auth docs:
    %% https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
    AmzDate = "20130524T000000Z",
    DateStamp = "20130524",
    PayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    Headers = [
        {"Host", "examplebucket.s3.amazonaws.com"},
        {"x-amz-content-sha256", PayloadHash},
        {"x-amz-date", AmzDate}
    ],
    {CanonicalHeaders, SignedHeaders} = s3filez_sigv4:canonical_headers(Headers),
    CanonicalQuery = s3filez_sigv4:canonical_query_string(<<"lifecycle">>),
    CanonicalRequest = iolist_to_binary([
        "GET\n",
        "/\n",
        CanonicalQuery, "\n",
        CanonicalHeaders, "\n",
        SignedHeaders, "\n",
        PayloadHash
    ]),
    ExpectedCanonicalRequest =
        "GET\n"
        "/\n"
        "lifecycle=\n"
        "host:examplebucket.s3.amazonaws.com\n"
        "x-amz-content-sha256:" ++ PayloadHash ++ "\n"
        "x-amz-date:20130524T000000Z\n"
        "\n"
        "host;x-amz-content-sha256;x-amz-date\n"
        ++ PayloadHash,
    ?assertEqual(ExpectedCanonicalRequest, binary_to_list(CanonicalRequest)),
    Config = #{
        username => "AKIAIOSFODNN7EXAMPLE",
        password => "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        region => "us-east-1"
    },
    StringToSign = s3filez_sigv4:string_to_sign(Config, AmzDate, DateStamp, CanonicalRequest),
    ExpectedStringToSign =
        "AWS4-HMAC-SHA256\n"
        "20130524T000000Z\n"
        "20130524/us-east-1/s3/aws4_request\n"
        "9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca",
    ?assertEqual(ExpectedStringToSign, binary_to_list(StringToSign)),
    Signature = s3filez_sigv4:signature(Config, DateStamp, StringToSign),
    ExpectedSignature = "964c7e476ea67fd0dbe754c179c24b69f45f4484575238740e4eef8ee26697ff",
    ?assertEqual(ExpectedSignature, Signature),
    Authorization = s3filez_sigv4:authorization(Config, DateStamp, SignedHeaders, Signature),
    ExpectedAuth =
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
        "Signature=" ++ ExpectedSignature,
    ?assertEqual(ExpectedAuth, Authorization).
