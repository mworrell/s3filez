%% @author Marc Worrell
%% @copyright 2026 Marc Worrell
%% @doc Tests for s3filez.
%% @end

%% Copyright 2026 Marc Worrell
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

-module(s3filez_test).

-include_lib("eunit/include/eunit.hrl").

get_test() ->
	s3_get( s3_config() ).


s3_config() -> s3_config( os:getenv("ERCDF_S3_SECRET") ).

s3_config( false ) -> nos3;
s3_config( Secret ) ->
	Access = os:getenv( "ERCDF_S3_ACCESS" ),
	Bucket = os:getenv("ERCDF_S3_BUCKET" ),
	URL = os:getenv("ERCDF_S3_URL" ),
	Region = os:getenv("ERCDF_S3_REGION"),
	SigVsn = os:getenv("ERCDF_S3_SIGNATURE_VERSION"),
	Cfg0 = #{
		username => list_to_binary(Access),
		password => list_to_binary(Secret)
	},
	Cfg1 = case SigVsn of
		"v4" -> Cfg0#{ signature_version => v4 };
		"v2" -> Cfg0#{ signature_version => v2 };
		_ -> Cfg0
	end,
	Cfg = case Region of
		false -> Cfg1;
		R -> Cfg1#{ region => list_to_binary(R) }
	end,
	#{
		s3_bucket => s3_config_adjust_end( lists:last(Bucket), Bucket ),
		s3_config => Cfg,
		s3_url => s3_config_adjust_url( URL )
	}.

s3_config_adjust_end( $/, X ) -> list_to_binary( X );
s3_config_adjust_end( _, X ) -> list_to_binary( [X, <<"/">>] ).

s3_config_adjust_url( "https://"++_=URL ) ->
	s3_config_adjust_end( lists:last(URL), URL );
s3_config_adjust_url( URL ) ->
	list_to_binary( [<<"https://">>, s3_config_adjust_end(lists:last(URL), URL)] ).


s3_get( nos3 ) -> skip;
s3_get( #{s3_bucket := B, s3_config := Cfg, s3_url := U} ) ->
	[{ok, _} = application:ensure_all_started(X) || X <- [crypto, public_key, ssl, ssl_verify_fun, inets, jobs, gproc, tls_certificate_check, s3filez]],

	Result = s3filez:get( Cfg, list_to_binary([U, B]) ),

	{ok, _Application, _Binary} =Result.
