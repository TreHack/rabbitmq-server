%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(per_vhost_connection_limit_partitions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-import(rabbit_ct_client_helpers, [open_unmanaged_connection/2,
                                   open_unmanaged_connection/3]).


all() ->
    [
     {group, partition_handling}
    ].

groups() ->
    [
     {partition_handling, [], [
          cluster_full_partition_test
     ]}
    ].

%% see partitions_SUITE
-define(DELAY, 9000).

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, [
                                               fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1
                                              ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(partition_handling, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{net_ticktime, 1}]),
    init_per_multinode_group(partition_handling, Config1, 3).

init_per_multinode_group(_GroupName, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:setup_steps(),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:teardown_steps(),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

cluster_full_partition_test(Config) ->
    VHost = <<"/">>,
    rabbit_ct_broker_helpers:set_partition_handling_mode_globally(Config, autoheal),

    ?assertEqual(0, count_connections_in(Config, VHost)),
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% 3 connections, 1 per node
    Conn1 = open_unmanaged_connection(Config, 0),
    Conn2 = open_unmanaged_connection(Config, 1),
    Conn3 = open_unmanaged_connection(Config, 2),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    %% B drops off the network, non-reachable by either A or C
    rabbit_ct_broker_helpers:block_traffic_between(A, B),
    rabbit_ct_broker_helpers:block_traffic_between(B, C),
    timer:sleep(?DELAY),

    ?assertEqual(2, count_connections_in(Config, VHost)),

    rabbit_ct_broker_helpers:allow_traffic_between(A, B),
    rabbit_ct_broker_helpers:allow_traffic_between(B, C),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    lists:foreach(fun (Conn) ->
                          (catch amqp_connection:close(Conn))
                  end, [Conn1, Conn2, Conn3]),

    passed.


%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

count_connections_in(Config, VHost) ->
    count_connections_in(Config, VHost, 0).
count_connections_in(Config, VHost, NodeIndex) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 count_connections_in, [VHost]).

connections_in(Config, VHost) ->
    connections_in(Config, 0, VHost).
connections_in(Config, NodeIndex, VHost) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list, [VHost]).

connections_on_node(Config) ->
    connections_on_node(Config, 0).
connections_on_node(Config, NodeIndex) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, NodeIndex, nodename),
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list_on_node, [Node]).
connections_on_node(Config, NodeIndex, NodeForListing) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list_on_node, [NodeForListing]).

all_connections(Config) ->
    all_connections(Config, 0).
all_connections(Config, NodeIndex) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list, []).
