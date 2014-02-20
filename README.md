s3filez
=======

Really tiny S3 client - only put, get and delete.

This client is used in combination with filezcache and zotonic.

Distinction with other s3 clients is:

 * Only get, put and delete are supported
 * put of files and/or binaries
 * get with optional streaming function, to be able to stream to the filezcache

Example
-------

    $ erl -pa ebin
    Erlang R15B03 (erts-5.9.3.1) [source] [64-bit] [smp:4:4] [async-threads:0] [kernel-poll:false]

    Eshell V5.9.3.1  (abort with ^G)
    1> application:start(crypto).  
    ok
    2> application:start(public_key).   
    ok
    3> application:start(ssl).          
    ok
    4> application:start(inets).
    ok
    5> Cfg = {<<"your-aws-key">>, <<"your-aws-secret">>}.
    {<<"your-aws-key">>, <<"your-aws-secret">>}
    6> s3filez:put(Cfg, <<"https://s.greenqloud.com/youraccount-default/Documents/LICENSE">>, {filename, 10175, "LICENSE"}).
    {ok,{{"HTTP/1.1",200,"OK"},
     [{"cache-control","no-cache"},
      {"connection","close"},
      {"date","Wed, 19 Feb 2014 22:25:56 GMT"},
      {"etag","\"ff253ad767462c46be284da12dda33e8\""},
      {"server","RestServer/1.0"},
      {"content-length","0"},
      {"content-type","binary/octet-stream"}],
     []}}
    7> s3filez:stream(Cfg, <<"https://s.greenqloud.com/mworrell-default/Documents/LICENSE">>, fun(X) -> io:format("~p~n", [X]) end).
    {content_type,<<"binary/octet-stream">>}
    <<"\n    Apache License\n", ...>>
    eof
    8> s3filez:delete(Cfg, <<"https://s.greenqloud.com/mworrell-default/Documents/LICENSE">>).
    {ok,{{"HTTP/1.1",204,"No Content"},
     [{"cache-control","no-cache"},
      {"connection","close"},
      {"date","Wed, 19 Feb 2014 22:28:42 GMT"},
      {"server","RestServer/1.0"},
      {"content-length","0"},
      {"content-type","binary/octet-stream"}],
     []}}
