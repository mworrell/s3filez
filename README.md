[![Test](https://github.com/mworrell/s3filez/workflows/Test/badge.svg)](https://github.com/mworrell/s3filez/actions)
[![Hex.pm Version](https://img.shields.io/hexpm/v/s3filez.svg)](https://hex.pm/packages/s3filez)
[![Hex.pm Downloads](https://img.shields.io/hexpm/dt/s3filez.svg)](https://hex.pm/packages/s3filez)

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

```erlang
rebar3 shell
===> Verifying dependencies...
===> Analyzing applications...
===> Compiling s3filez
Erlang/OTP 23 [erts-11.1] [source] [64-bit] [smp:12:12] [ds:12:12:10] [async-threads:1] [hipe]

Eshell V11.1  (abort with ^G)
1> application:ensure_all_started(s3filez).
{ok,[jobs,s3filez]}
2> Cfg = {<<"your-aws-key">>, <<"your-aws-secret">>}.
{<<"your-aws-key">>, <<"your-aws-secret">>}
3> s3filez:put(Cfg, <<"https://your-bucket.s3-eu-west-1.amazonaws.com/LICENSE">>, {filename, "LICENSE"}).
ok
4> s3filez:stream(Cfg, <<"https://your-bucket.s3-eu-west-1.amazonaws.com/LICENSE">>, fun(X) -> io:format("!! ~p~n", [X]) end).
!! stream_start
!! {content_type,<<"binary/octet-stream">>}
!! <<"\n    Apache License\n", ...>>
!! eof
5> s3filez:delete(Cfg, <<"https://your-bucket.s3-eu-west-1.amazonaws.com/LICENSE">>).
ok
```

Request Queue
-------------

Requests can be queued. They will be placed in a supervisor and scheduled using https://github.com/uwiger/jobs
The current scheduler restricts the number of parallel S3 requests. The default maximum is 100.

The `get`, `put` and `delete` requests can be queued. A function or pid can be given as a callback for the job result.
The `stream` command can’t be queued: it is already running asynchronously.

Example:

```erlang
6> {ok, ReqId, JobPid} = s3filez:queue_put(Cfg, <<"https://your-bucket.s3-eu-west-1.amazonaws.com/LICENSE">>, {filename, 10175, "LICENSE"}, fun(ReqId, Result) -> nop end).
{ok,#Ref<0.0.0.3684>,<0.854.0>}
```

The returned `JobPid` is the pid of the process in the s3filez queue supervisor.
The callback can be a function (arity 2), `{M,F,A}` or a pid.

If the callback is a pid then it will receive the message `{s3filez_done, ReqId, Result}`.

