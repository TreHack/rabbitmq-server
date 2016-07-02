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

-module(per_vhost_connection_limit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-import(rabbit_ct_client_helpers, [open_unmanaged_connection/2,
                                   open_unmanaged_connection/3]).

all() ->
    [
      {group, cluster_size_1},
      {group, cluster_size_2}
    ].

groups() ->
    [
      {cluster_size_1, [], [
          most_basic_single_node_connection_tracking_test,
          single_node_single_vhost_connection_tracking_test,
          single_node_multiple_vhost_connection_tracking_test
        ]},
      {cluster_size_2, [], [
          most_basic_cluster_connection_tracking_test,
          cluster_single_vhost_connection_tracking_test,
          cluster_multiple_vhost_connection_tracking_test
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_1, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, 1},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_group(cluster_size_2, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, 3},
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

most_basic_single_node_connection_tracking_test(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),
    Conn = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    amqp_connection:close(Conn),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

single_node_single_vhost_connection_tracking_test(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn1 = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    amqp_connection:close(Conn1),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn2 = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    Conn3 = open_unmanaged_connection(Config, 0),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn4 = open_unmanaged_connection(Config, 0),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    (catch exit(Conn4, please_terminate)),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn5 = open_unmanaged_connection(Config, 0),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    lists:foreach(fun (C) ->
                          amqp_connection:close(C)
                  end, [Conn2, Conn3, Conn5]),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

single_node_multiple_vhost_connection_tracking_test(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    rabbit_ct_broker_helpers:add_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost1),

    rabbit_ct_broker_helpers:add_vhost(Config, VHost2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    Conn1 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    amqp_connection:close(Conn1),
    ?assertEqual(0, count_connections_in(Config, VHost1)),

    Conn2 = open_unmanaged_connection(Config, 0, VHost2),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    Conn3 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    Conn4 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(2, count_connections_in(Config, VHost1)),

    (catch exit(Conn4, please_terminate)),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    Conn5 = open_unmanaged_connection(Config, 0, VHost2),
    ?assertEqual(2, count_connections_in(Config, VHost2)),

    Conn6 = open_unmanaged_connection(Config, 0, VHost2),
    ?assertEqual(3, count_connections_in(Config, VHost2)),

    lists:foreach(fun (C) ->
                          amqp_connection:close(C)
                  end, [Conn2, Conn3, Conn5, Conn6]),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),

    passed.

most_basic_cluster_connection_tracking_test(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),
    Conn1 = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    Conn2 = open_unmanaged_connection(Config, 1),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn3 = open_unmanaged_connection(Config, 1),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    lists:foreach(fun (C) ->
                          amqp_connection:close(C)
                  end, [Conn1, Conn2, Conn3]),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

cluster_single_vhost_connection_tracking_test(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn1 = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    amqp_connection:close(Conn1),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn2 = open_unmanaged_connection(Config, 1),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    Conn3 = open_unmanaged_connection(Config, 0),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn4 = open_unmanaged_connection(Config, 1),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    (catch exit(Conn4, please_terminate)),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn5 = open_unmanaged_connection(Config, 1),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    lists:foreach(fun (C) ->
                          amqp_connection:close(C)
                  end, [Conn2, Conn3, Conn5]),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

cluster_multiple_vhost_connection_tracking_test(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    rabbit_ct_broker_helpers:add_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost1),

    rabbit_ct_broker_helpers:add_vhost(Config, VHost2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    Conn1 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    amqp_connection:close(Conn1),
    ?assertEqual(0, count_connections_in(Config, VHost1)),

    Conn2 = open_unmanaged_connection(Config, 1, VHost2),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    Conn3 = open_unmanaged_connection(Config, 1, VHost1),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    Conn4 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(2, count_connections_in(Config, VHost1)),

    (catch exit(Conn4, please_terminate)),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    Conn5 = open_unmanaged_connection(Config, 1, VHost2),
    ?assertEqual(2, count_connections_in(Config, VHost2)),

    Conn6 = open_unmanaged_connection(Config, 0, VHost2),
    ?assertEqual(3, count_connections_in(Config, VHost2)),

    lists:foreach(fun (C) ->
                          amqp_connection:close(C)
                  end, [Conn2, Conn3, Conn5, Conn6]),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),

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
