[![Test](https://github.com/mworrell/s3filez/workflows/Test/badge.svg)](https://github.com/mworrell/s3filez/actions)
[![Hex.pm Version](https://img.shields.io/hexpm/v/s3filez.svg)](https://hex.pm/packages/s3filez)
[![Hex.pm Downloads](https://img.shields.io/hexpm/dt/s3filez.svg)](https://hex.pm/packages/s3filez)

s3filez
=======

Really tiny S3 client - put, get, delete and multipart uploads.

This client is used in combination with filezcache and zotonic.

Distinction with other s3 clients is:

 * get, put, delete and multipart uploads
 * put of files and/or binaries
 * get with optional streaming function, to be able to stream to the filezcache
 * simple jobs queue, using the 'jobs' scheduler

Example (Signature V2 default)
------------------------------

```erlang
rebar3 shell
===> Verifying dependencies...
===> Analyzing applications...
===> Compiling s3filez
Erlang/OTP 23 [erts-11.1] [source] [64-bit] [smp:12:12] [ds:12:12:10] [async-threads:1] [hipe]

Eshell V11.1  (abort with ^G)
1> application:ensure_all_started(s3filez).
{ok,[jobs,s3filez]}
2> Cfg = #{ username => <<"your-aws-key">>, password => <<"your-aws-secret">> }.
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

Signature V4 example
--------------------

```erlang
1> application:ensure_all_started(s3filez).
{ok,[jobs,s3filez]}
2> Cfg = #{
       username => <<"your-aws-key">>,
       password => <<"your-aws-secret">>,
       signature_version => v4,
       region => <<"eu-west-1">>
   }.
#{region => <<"eu-west-1">>,signature_version => v4,username => <<"your-aws-key">>,password => <<"your-aws-secret">>}
3> s3filez:get(Cfg, <<"https://your-bucket.s3-eu-west-1.amazonaws.com/LICENSE">>).
{ok,<<"binary/octet-stream">>,_}
```

Signature V2 is the default if `signature_version` is not set.
When using Signature V4, `region` must be set; otherwise calls return `{error, region_needed}`.

Multipart uploads
-----------------

Use multipart uploads when data arrives in chunks or its final size is unknown.
The API works with the same configuration and object URL as `put/3`, using either
Signature V2 or V4. Initiation accepts the same ACL and content type options as `put/4`.

```erlang
{ok, UploadId} = s3filez:create_multipart_upload(Cfg, Url,
    [{content_type, "application/octet-stream"}]),
{ok, ETag1} = s3filez:upload_part(Cfg, Url, UploadId, 1, {data, FirstPart}),
{ok, ETag2} = s3filez:upload_part(Cfg, Url, UploadId, 2, {data, LastPart}),
{ok, ObjectETag} = s3filez:complete_multipart_upload(Cfg, Url, UploadId,
    [{1, ETag1}, {2, ETag2}]).
```

`upload_part/5` also accepts `{filename, Filename}` or
`{filename, Size, Filename}`, where the file contains that part's data.
Each call blocks until its request finishes and uses the existing job limiter.
The caller controls buffering, concurrency and retries; no whole-object buffering
or total size is required. Accumulate incoming chunks into parts of at least
5 MiB, except for the final part, which may be smaller. Part numbers are 1–10,000.
Retrying a part with the same number replaces it; retain its latest returned ETag.
Completion sorts the supplied parts and rejects duplicate or invalid numbers.

If abandoning an upload, release its stored parts with:

```erlang
ok = s3filez:abort_multipart_upload(Cfg, Url, UploadId).
```

Completion returns `{ok, ObjectETag}` only after parsing the success response.
An S3 error embedded in HTTP 200 returns `{error, {s3, Code, Message}}`, with binary
code and message. Malformed success responses return
`{error, invalid_multipart_response}`. Other HTTP and transport errors follow the
existing API conventions. Failed calls do not automatically abort the upload,
so the caller can retry or explicitly abort it. Persist the upload ID if cleanup
must survive a caller restart.

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
