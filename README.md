s3filez
=======

Really tiny S3 client - only put, get and delete.

This client is used in combination with filezcache and zotonic.

Distinction with other s3 clients is:

 * Only get, put and delete are supported
 * put of files and/or binaries
 * get with optional streaming function, to be able to stream to the filezcache
 * simple jobs queue, using the 'jobs' scheduler

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
    ok
    7> s3filez:stream(Cfg, <<"https://s.greenqloud.com/youraccount-default/Documents/LICENSE">>, fun(X) -> io:format("!! ~p~n", [X]) end).
    !! {content_type,<<"binary/octet-stream">>}
    !! <<"\n    Apache License\n", ...>>
    !! eof
    8> s3filez:delete(Cfg, <<"https://s.greenqloud.com/youraccount-default/Documents/LICENSE">>).
    ok

Request Queue
-------------

Requests can be queued. They will be placed in a supervisor and scheduled using https://github.com/esl/jobs
The current scheduler restricts the number of parallel S3 requests. The default maximum is 100.

The `get`, `put` and `delete` requests can be queued. A function or pid can be given as a callback for the job result.
The `stream` command can’t be queued: it is already running asynchronously.

Example:

     9> {ok, ReqId, JobPid} = s3filez:queue_put(Cfg, <<"https://s.greenqloud.com/youraccount-default/Documents/LICENSE">>, fun(ReqId,Result) -> nop end).
     {ok,#Ref<0.0.0.3684>,<0.854.0>}

The returned `JobPid` is the pid of the process in the s3filez queue supervisor.
The callback can be a function (arity 2), `{M,F,A}` or a pid.

If the callback is a pid then it will receive the message `{s3filez_done, ReqId, Result}`.

