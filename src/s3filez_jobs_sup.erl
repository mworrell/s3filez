%% @doc Supervisor for all s3 async queued jobs.
%% @author Marc Worrell
%% @copyright 2013-2020 Marc Worrell

%% Copyright 2013-2020 Marc Worrell
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(s3filez_jobs_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    queue/1,
    queue/2
    ]).

%% Supervisor callbacks
-export([init/1]).

queue(S3Job) ->
    queue(erlang:make_ref(), S3Job).

queue(Ref, S3Job) ->
    case supervisor:start_child(?MODULE, [Ref, S3Job]) of
        {ok, Pid} -> {ok, Ref, Pid};
        {error, {already_started, Pid}} -> {ok, Ref, Pid}
    end.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Worker = {s3filez_job, {s3filez_job, start_link, []},
              temporary, 10000, worker, [s3filez_job]},
    {ok, {{simple_one_for_one, 4, 3600}, [Worker]}}.
