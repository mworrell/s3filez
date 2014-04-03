%% @author Marc Worrell
%% @copyright 2013-2014 Marc Worrell

%% Copyright 2014 Marc Worrell
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

-module(s3filez_job).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-export([
    start_link/2
   ]).

-record(state, {job_id, cmd}).

start_link(JobId, Cmd) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [JobId, Cmd], []).

init([JobId, Cmd]) ->
    gen_server:cast(self(), run),
    {ok, #state{job_id=JobId, cmd=Cmd}}.

handle_call(Msg, _From, State) ->
    {reply, {unknown_msg, Msg}, State}.

handle_cast(run, #state{cmd=Cmd, job_id=JobId} = State) ->
    do_cmd(JobId, Cmd),
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVersion, State, _Extra) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.


do_cmd(JobId, {get, Config, Url, OnReady}) ->
    Result = s3filez:get(Config, Url),
    on_ready(JobId, Result, OnReady);
do_cmd(JobId, {delete, Config, Url, OnReady}) ->
    Result = s3filez:delete(Config, Url),
    on_ready(JobId, Result, OnReady);
do_cmd(JobId, {put, Config, Url, What, OnReady}) ->
    Result = s3filez:put(Config, Url, What),
    on_ready(JobId, Result, OnReady).


on_ready(_JobId, _Result, undefined) ->
    ok;
on_ready(JobId, Result, {M,F,A}) ->
    erlang:apply(M, F, A++[JobId, Result]);
on_ready(JobId, Result, Fun) when is_function(Fun,2) ->
    Fun(JobId, Result);
on_ready(JobId, Result, Pid) when is_pid(Pid) ->
    Pid ! {s3filez_done, JobId, Result}.


