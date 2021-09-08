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
	#{
		s3_bucket => s3_config_adjust_end( lists:last(Bucket), Bucket ),
		s3_config => {binary:list_to_bin(Access), binary:list_to_bin(Secret)},
		s3_url => s3_config_adjust_url( URL )
	}.

s3_config_adjust_end( $/, X ) -> binary:list_to_bin( X );
s3_config_adjust_end( _, X ) -> binary:list_to_bin( [X, <<"/">>] ).

s3_config_adjust_url( "https://"++_=URL ) ->
	s3_config_adjust_end( lists:last(URL), URL );
s3_config_adjust_url( URL ) ->
	binary:list_to_bin( [<<"https://">>, s3_config_adjust_end(lists:last(URL), URL)] ).


s3_get( nos3 ) -> skip;
s3_get( #{s3_bucket := B, s3_config := C, s3_url := U} ) ->
	[ok = application:ensure_started(X) || X <- [crypto, public_key, ssl, ssl_verify_fun, inets, jobs, tls_certificate_check, s3filez]],

	Result = s3filez:get( C, binary:list_to_bin([U, B]) ),

	{ok, _Application, _Binary} =Result.
