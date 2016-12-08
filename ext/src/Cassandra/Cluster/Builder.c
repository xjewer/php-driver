/**
 * Copyright 2015-2016 DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "php_cassandra.h"
#include "php_cassandra_globals.h"
#include "util/consistency.h"

zend_class_entry *cassandra_cluster_builder_ce = NULL;

ZEND_BEGIN_ARG_INFO_EX(arginfo_none, 0, ZEND_RETURN_VALUE, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_consistency, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, consistency)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_page_size, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, pageSize)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_contact_points, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, host)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_port, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, port)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_dc_aware, 0, ZEND_RETURN_VALUE, 3)
  ZEND_ARG_INFO(0, localDatacenter)
  ZEND_ARG_INFO(0, hostPerRemoteDatacenter)
  ZEND_ARG_INFO(0, useRemoteDatacenterForLocalConsistencies)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_blacklist_nodes, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, hosts)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_whitelist_nodes, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, hosts)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_blacklist_dcs, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, dcs)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_whitelist_dcs, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, dcs)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_enabled, 0, ZEND_RETURN_VALUE, 0)
  ZEND_ARG_INFO(0, enabled)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_credentials, 0, ZEND_RETURN_VALUE, 2)
  ZEND_ARG_INFO(0, username)
  ZEND_ARG_INFO(0, password)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_timeout, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, timeout)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_ssl, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_OBJ_INFO(0, options, Cassandra\\SSLOptions, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_version, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, version)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_count, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, count)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_connections, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, core)
  ZEND_ARG_INFO(0, max)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_interval, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, interval)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_delay, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, delay)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_retry_policy, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_OBJ_INFO(0, policy, Cassandra\\RetryPolicy, 0)
ZEND_END_ARG_INFO()

  ZEND_BEGIN_ARG_INFO_EX(arginfo_timestamp_gen, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_OBJ_INFO(0, generator, Cassandra\\TimestampGenerator, 0)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_cluster_builder_methods[] = {
  PHP_ABSTRACT_ME(ClusterBuilder, build, arginfo_none)
  PHP_ABSTRACT_ME(ClusterBuilder, withDefaultConsistency, arginfo_consistency)
  PHP_ABSTRACT_ME(ClusterBuilder, withDefaultPageSize, arginfo_page_size)
  PHP_ABSTRACT_ME(ClusterBuilder, withDefaultTimeout, arginfo_timeout)
  PHP_ABSTRACT_ME(ClusterBuilder, withContactPoints, arginfo_contact_points)
  PHP_ABSTRACT_ME(ClusterBuilder, withPort, arginfo_port)
  PHP_ABSTRACT_ME(ClusterBuilder, withRoundRobinLoadBalancingPolicy, arginfo_none)
  PHP_ABSTRACT_ME(ClusterBuilder, withDatacenterAwareRoundRobinLoadBalancingPolicy, arginfo_dc_aware)
  PHP_ABSTRACT_ME(ClusterBuilder, withBlackListHosts, arginfo_blacklist_nodes)
  PHP_ABSTRACT_ME(ClusterBuilder, withWhiteListHosts, arginfo_whitelist_nodes)
  PHP_ABSTRACT_ME(ClusterBuilder, withBlackListDCs, arginfo_blacklist_dcs)
  PHP_ABSTRACT_ME(ClusterBuilder, withWhiteListDCs, arginfo_whitelist_dcs)
  PHP_ABSTRACT_ME(ClusterBuilder, withTokenAwareRouting, arginfo_enabled)
  PHP_ABSTRACT_ME(ClusterBuilder, withCredentials, arginfo_credentials)
  PHP_ABSTRACT_ME(ClusterBuilder, withConnectTimeout, arginfo_timeout)
  PHP_ABSTRACT_ME(ClusterBuilder, withRequestTimeout, arginfo_timeout)
  PHP_ABSTRACT_ME(ClusterBuilder, withSSL, arginfo_ssl)
  PHP_ABSTRACT_ME(ClusterBuilder, withPersistentSessions, arginfo_enabled)
  PHP_ABSTRACT_ME(ClusterBuilder, withProtocolVersion, arginfo_version)
  PHP_ABSTRACT_ME(ClusterBuilder, withIOThreads, arginfo_count)
  PHP_ABSTRACT_ME(ClusterBuilder, withConnectionsPerHost, arginfo_connections)
  PHP_ABSTRACT_ME(ClusterBuilder, withReconnectInterval, arginfo_interval)
  PHP_ABSTRACT_ME(ClusterBuilder, withLatencyAwareRouting, arginfo_enabled)
  PHP_ABSTRACT_ME(ClusterBuilder, withTCPNodelay, arginfo_enabled)
  PHP_ABSTRACT_ME(ClusterBuilder, withTCPKeepalive, arginfo_delay)
  PHP_ABSTRACT_ME(ClusterBuilder, withRetryPolicy, arginfo_retry_policy)
  PHP_ABSTRACT_ME(ClusterBuilder, withTimestampGenerator, arginfo_timestamp_gen)
  PHP_ABSTRACT_ME(ClusterBuilder, withSchemaMetadata, arginfo_enabled)
  PHP_ABSTRACT_ME(ClusterBuilder, withHostnameResolution, arginfo_enabled)
  PHP_ABSTRACT_ME(ClusterBuilder, withRandomizedContactPoints, arginfo_enabled)
  PHP_ABSTRACT_ME(ClusterBuilder, withConnectionHeartbeatInterval, arginfo_interval)
  PHP_FE_END
};

void cassandra_define_ClusterBuilder(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\Cluster\\Builder", cassandra_cluster_builder_methods);
  cassandra_cluster_builder_ce = zend_register_internal_class(&ce TSRMLS_CC);
  cassandra_cluster_builder_ce->ce_flags |= ZEND_ACC_INTERFACE;
}

void php_cassandra_cluster_builder_generate_hash_key(cassandra_cluster_builder_base *builder,
                                                     smart_str *hash_key) {
  smart_str_appendc(hash_key, ':');
  smart_str_appends(hash_key, builder->contact_points);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->port);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->load_balancing_policy);

  smart_str_appendc(hash_key, ':');
  smart_str_appends(hash_key, SAFE_STR(builder->local_dc));

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->allow_remote_dcs_for_local_cl);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->used_hosts_per_remote_dc);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->use_token_aware_routing);

  smart_str_appendc(hash_key, ':');
  smart_str_appends(hash_key, SAFE_STR(builder->username));

  smart_str_appendc(hash_key, ':');
  smart_str_appends(hash_key, SAFE_STR(builder->password));

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->connect_timeout);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->request_timeout);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->protocol_version);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->io_threads);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->core_connections_per_host);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->max_connections_per_host);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->reconnect_interval);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->enable_latency_aware_routing);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->enable_tcp_nodelay);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->enable_tcp_keepalive);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->tcp_keepalive_delay);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->enable_schema);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->enable_hostname_resolution);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->enable_randomized_contact_points);

  smart_str_appendc(hash_key, ':');
  smart_str_append_long(hash_key, builder->connection_heartbeat_interval);

  smart_str_appendc(hash_key, ':');
  smart_str_appends(hash_key, SAFE_STR(builder->whitelist_hosts));

  smart_str_appendc(hash_key, ':');
  smart_str_appends(hash_key, SAFE_STR(builder->whitelist_dcs));

  smart_str_appendc(hash_key, ':');
  smart_str_appends(hash_key, SAFE_STR(builder->blacklist_hosts));

  smart_str_appendc(hash_key, ':');
  smart_str_appends(hash_key, SAFE_STR(builder->blacklist_dcs));
}

CassCluster *php_cassandra_cluster_builder_get_cache(cassandra_cluster_builder_base *builder,
                                                     const char *hash_key, int hash_key_len TSRMLS_DC) {
  if (builder->persist) {
    php5to7_zend_resource_le *le;

    if (PHP5TO7_ZEND_HASH_FIND(&EG(persistent_list), hash_key, hash_key_len + 1, le)) {
      if (Z_TYPE_P(le) == php_le_cassandra_cluster()) {
        return (CassCluster*) Z_RES_P(le)->ptr;
      }
    }
  }

  return NULL;
}

void php_cassandra_cluster_builder_add_cache(cassandra_cluster_builder_base *builder,
                                             const char *hash_key, int hash_key_len,
                                             CassCluster *cluster TSRMLS_DC) {
  if (builder->persist) {
    php5to7_zend_resource_le resource;

#if PHP_MAJOR_VERSION >= 7
    ZVAL_NEW_PERSISTENT_RES(&resource, 0, cluster, php_le_cassandra_cluster());

    if (PHP5TO7_ZEND_HASH_UPDATE(&EG(persistent_list), hash_key, hash_key_len + 1, &resource, sizeof(php5to7_zend_resource_le))) {
      CASSANDRA_G(persistent_clusters)++;
    }
#else
    resource.type = php_le_cassandra_cluster();
    resource.ptr = cluster;

    if (PHP5TO7_ZEND_HASH_UPDATE(&EG(persistent_list), hash_key, hash_key_len + 1, resource, sizeof(php5to7_zend_resource_le))) {
      CASSANDRA_G(persistent_clusters)++;
    }
#endif
  }
}

void php_cassandra_cluster_builder_init(cassandra_cluster_builder_base *builder)
{
  builder->contact_points = estrdup("127.0.0.1");
  builder->port = 9042;
  builder->load_balancing_policy = LOAD_BALANCING_DEFAULT;
  builder->local_dc = NULL;
  builder->used_hosts_per_remote_dc = 0;
  builder->allow_remote_dcs_for_local_cl = 0;
  builder->use_token_aware_routing = 1;
  builder->username = NULL;
  builder->password = NULL;
  builder->connect_timeout = 5000;
  builder->request_timeout = 12000;
  builder->default_consistency = PHP_CASSANDRA_DEFAULT_CONSISTENCY;
  builder->default_page_size = 5000;
  builder->persist = 1;
  builder->protocol_version = 4;
  builder->io_threads = 1;
  builder->core_connections_per_host = 1;
  builder->max_connections_per_host = 2;
  builder->reconnect_interval = 2000;
  builder->enable_latency_aware_routing = 1;
  builder->enable_tcp_nodelay = 1;
  builder->enable_tcp_keepalive = 0;
  builder->tcp_keepalive_delay = 0;
  builder->enable_schema = 1;
  builder->blacklist_hosts = NULL;
  builder->whitelist_hosts = NULL;
  builder->blacklist_dcs = NULL;
  builder->whitelist_dcs = NULL;
  builder->enable_hostname_resolution = 0;
  builder->enable_randomized_contact_points = 1;
  builder->connection_heartbeat_interval = 30;

  PHP5TO7_ZVAL_UNDEF(builder->ssl_options);
  PHP5TO7_ZVAL_UNDEF(builder->default_timeout);
  PHP5TO7_ZVAL_UNDEF(builder->retry_policy);
  PHP5TO7_ZVAL_UNDEF(builder->timestamp_gen);
}

void php_cassandra_cluster_builder_destroy(cassandra_cluster_builder_base *builder)
{
  efree(builder->contact_points);
  builder->contact_points = NULL;

  if (builder->local_dc) {
    efree(builder->local_dc);
    builder->local_dc = NULL;
  }

  if (builder->username) {
    efree(builder->username);
    builder->username = NULL;
  }

  if (builder->password) {
    efree(builder->password);
    builder->password = NULL;
  }

  if (builder->whitelist_hosts) {
    efree(builder->whitelist_hosts);
    builder->whitelist_hosts = NULL;
  }

  if (builder->blacklist_hosts) {
    efree(builder->blacklist_hosts);
    builder->blacklist_hosts = NULL;
  }

  if (builder->whitelist_dcs) {
    efree(builder->whitelist_dcs);
    builder->whitelist_dcs = NULL;
  }

  if (builder->blacklist_dcs) {
    efree(builder->blacklist_dcs);
    builder->whitelist_dcs = NULL;
  }

  PHP5TO7_ZVAL_MAYBE_DESTROY(builder->ssl_options);
  PHP5TO7_ZVAL_MAYBE_DESTROY(builder->default_timeout);
  PHP5TO7_ZVAL_MAYBE_DESTROY(builder->retry_policy);
  PHP5TO7_ZVAL_MAYBE_DESTROY(builder->timestamp_gen);
}

void php_cassandra_cluster_builder_properties(cassandra_cluster_builder_base *builder,
                                              HashTable *props) {
  php5to7_zval contactPoints;
  php5to7_zval loadBalancingPolicy;
  php5to7_zval localDatacenter;
  php5to7_zval hostPerRemoteDatacenter;
  php5to7_zval useRemoteDatacenterForLocalConsistencies;
  php5to7_zval useTokenAwareRouting;
  php5to7_zval username;
  php5to7_zval password;
  php5to7_zval connectTimeout;
  php5to7_zval requestTimeout;
  php5to7_zval sslOptions;
  php5to7_zval defaultConsistency;
  php5to7_zval defaultPageSize;
  php5to7_zval defaultTimeout;
  php5to7_zval usePersistentSessions;
  php5to7_zval protocolVersion;
  php5to7_zval ioThreads;
  php5to7_zval coreConnectionPerHost;
  php5to7_zval maxConnectionsPerHost;
  php5to7_zval reconnectInterval;
  php5to7_zval latencyAwareRouting;
  php5to7_zval tcpNodelay;
  php5to7_zval tcpKeepalive;
  php5to7_zval retryPolicy;
  php5to7_zval blacklistHosts;
  php5to7_zval whitelistHosts;
  php5to7_zval blacklistDCs;
  php5to7_zval whitelistDCs;
  php5to7_zval timestampGen;
  php5to7_zval schemaMetadata;
  php5to7_zval hostnameResolution;
  php5to7_zval randomizedContactPoints;
  php5to7_zval connectionHeartbeatInterval;

  PHP5TO7_ZVAL_MAYBE_MAKE(contactPoints);
  PHP5TO7_ZVAL_STRING(PHP5TO7_ZVAL_MAYBE_P(contactPoints), builder->contact_points);

  PHP5TO7_ZVAL_MAYBE_MAKE(loadBalancingPolicy);
  ZVAL_LONG(PHP5TO7_ZVAL_MAYBE_P(loadBalancingPolicy), builder->load_balancing_policy);

  PHP5TO7_ZVAL_MAYBE_MAKE(localDatacenter);
  PHP5TO7_ZVAL_MAYBE_MAKE(hostPerRemoteDatacenter);
  PHP5TO7_ZVAL_MAYBE_MAKE(useRemoteDatacenterForLocalConsistencies);
  if (builder->load_balancing_policy == LOAD_BALANCING_DC_AWARE_ROUND_ROBIN) {
    PHP5TO7_ZVAL_STRING(PHP5TO7_ZVAL_MAYBE_P(localDatacenter), builder->local_dc);
    ZVAL_LONG(PHP5TO7_ZVAL_MAYBE_P(hostPerRemoteDatacenter), builder->used_hosts_per_remote_dc);
    ZVAL_BOOL(PHP5TO7_ZVAL_MAYBE_P(useRemoteDatacenterForLocalConsistencies), builder->allow_remote_dcs_for_local_cl);
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(localDatacenter));
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(hostPerRemoteDatacenter));
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(useRemoteDatacenterForLocalConsistencies));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(useTokenAwareRouting);
  ZVAL_BOOL(PHP5TO7_ZVAL_MAYBE_P(useTokenAwareRouting), builder->use_token_aware_routing);

  PHP5TO7_ZVAL_MAYBE_MAKE(username);
  PHP5TO7_ZVAL_MAYBE_MAKE(password);
  if (builder->username) {
    PHP5TO7_ZVAL_STRING(PHP5TO7_ZVAL_MAYBE_P(username), builder->username);
    PHP5TO7_ZVAL_STRING(PHP5TO7_ZVAL_MAYBE_P(password), builder->password);
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(username));
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(password));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(connectTimeout);
  ZVAL_DOUBLE(PHP5TO7_ZVAL_MAYBE_P(connectTimeout), (double) builder->connect_timeout / 1000);
  PHP5TO7_ZVAL_MAYBE_MAKE(requestTimeout);
  ZVAL_DOUBLE(PHP5TO7_ZVAL_MAYBE_P(requestTimeout), (double) builder->request_timeout / 1000);

  PHP5TO7_ZVAL_MAYBE_MAKE(sslOptions);
  if (!PHP5TO7_ZVAL_IS_UNDEF(builder->ssl_options)) {
    PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(sslOptions), PHP5TO7_ZVAL_MAYBE_P(builder->ssl_options));
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(sslOptions));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(defaultConsistency);
  ZVAL_LONG(PHP5TO7_ZVAL_MAYBE_P(defaultConsistency), builder->default_consistency);
  PHP5TO7_ZVAL_MAYBE_MAKE(defaultPageSize);
  ZVAL_LONG(PHP5TO7_ZVAL_MAYBE_P(defaultPageSize), builder->default_page_size);
  PHP5TO7_ZVAL_MAYBE_MAKE(defaultTimeout);
  if (!PHP5TO7_ZVAL_IS_UNDEF(builder->default_timeout)) {
    ZVAL_LONG(PHP5TO7_ZVAL_MAYBE_P(defaultTimeout), PHP5TO7_Z_LVAL_MAYBE_P(builder->default_timeout));
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(defaultTimeout));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(usePersistentSessions);
  ZVAL_BOOL(PHP5TO7_ZVAL_MAYBE_P(usePersistentSessions), builder->persist);

  PHP5TO7_ZVAL_MAYBE_MAKE(protocolVersion);
  ZVAL_LONG(PHP5TO7_ZVAL_MAYBE_P(protocolVersion), builder->protocol_version);

  PHP5TO7_ZVAL_MAYBE_MAKE(ioThreads);
  ZVAL_LONG(PHP5TO7_ZVAL_MAYBE_P(ioThreads), builder->io_threads);

  PHP5TO7_ZVAL_MAYBE_MAKE(coreConnectionPerHost);
  ZVAL_LONG(PHP5TO7_ZVAL_MAYBE_P(coreConnectionPerHost), builder->core_connections_per_host);

  PHP5TO7_ZVAL_MAYBE_MAKE(maxConnectionsPerHost);
  ZVAL_LONG(PHP5TO7_ZVAL_MAYBE_P(maxConnectionsPerHost), builder->max_connections_per_host);

  PHP5TO7_ZVAL_MAYBE_MAKE(reconnectInterval);
  ZVAL_DOUBLE(PHP5TO7_ZVAL_MAYBE_P(reconnectInterval), (double) builder->reconnect_interval / 1000);

  PHP5TO7_ZVAL_MAYBE_MAKE(latencyAwareRouting);
  ZVAL_BOOL(PHP5TO7_ZVAL_MAYBE_P(latencyAwareRouting), builder->enable_latency_aware_routing);

  PHP5TO7_ZVAL_MAYBE_MAKE(tcpNodelay);
  ZVAL_BOOL(PHP5TO7_ZVAL_MAYBE_P(tcpNodelay), builder->enable_tcp_nodelay);

  PHP5TO7_ZVAL_MAYBE_MAKE(tcpKeepalive);
  if (builder->enable_tcp_keepalive) {
    ZVAL_DOUBLE(PHP5TO7_ZVAL_MAYBE_P(tcpKeepalive), (double) builder->tcp_keepalive_delay / 1000);
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(tcpKeepalive));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(retryPolicy);
  if (!PHP5TO7_ZVAL_IS_UNDEF(builder->retry_policy)) {
    PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(retryPolicy), PHP5TO7_ZVAL_MAYBE_P(builder->retry_policy));
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(retryPolicy));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(blacklistHosts);
  if (builder->blacklist_hosts) {
    PHP5TO7_ZVAL_STRING(PHP5TO7_ZVAL_MAYBE_P(blacklistHosts), builder->blacklist_hosts);
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(blacklistHosts));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(whitelistHosts);
  if (builder->whitelist_hosts) {
    PHP5TO7_ZVAL_STRING(PHP5TO7_ZVAL_MAYBE_P(whitelistHosts), builder->whitelist_hosts);
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(whitelistHosts));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(blacklistDCs);
  if (builder->blacklist_dcs) {
    PHP5TO7_ZVAL_STRING(PHP5TO7_ZVAL_MAYBE_P(blacklistDCs), builder->blacklist_dcs);
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(blacklistDCs));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(whitelistDCs);
  if (builder->whitelist_dcs) {
    PHP5TO7_ZVAL_STRING(PHP5TO7_ZVAL_MAYBE_P(whitelistDCs), builder->whitelist_dcs);
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(whitelistDCs));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(timestampGen);
  if (!PHP5TO7_ZVAL_IS_UNDEF(builder->timestamp_gen)) {
    PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(timestampGen), PHP5TO7_ZVAL_MAYBE_P(builder->timestamp_gen));
  } else {
    ZVAL_NULL(PHP5TO7_ZVAL_MAYBE_P(timestampGen));
  }

  PHP5TO7_ZVAL_MAYBE_MAKE(schemaMetadata);
  ZVAL_BOOL(PHP5TO7_ZVAL_MAYBE_P(schemaMetadata), builder->enable_schema);

  PHP5TO7_ZVAL_MAYBE_MAKE(hostnameResolution);
  ZVAL_BOOL(PHP5TO7_ZVAL_MAYBE_P(hostnameResolution), builder->enable_hostname_resolution);

  PHP5TO7_ZVAL_MAYBE_MAKE(randomizedContactPoints);
  ZVAL_BOOL(PHP5TO7_ZVAL_MAYBE_P(randomizedContactPoints), builder->enable_randomized_contact_points);

  PHP5TO7_ZVAL_MAYBE_MAKE(connectionHeartbeatInterval);
  ZVAL_LONG(PHP5TO7_ZVAL_MAYBE_P(connectionHeartbeatInterval), builder->connection_heartbeat_interval);

  PHP5TO7_ZEND_HASH_UPDATE(props, "contactPoints", sizeof("contactPoints"),
                           PHP5TO7_ZVAL_MAYBE_P(contactPoints), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "loadBalancingPolicy", sizeof("loadBalancingPolicy"),
                           PHP5TO7_ZVAL_MAYBE_P(loadBalancingPolicy), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "localDatacenter", sizeof("localDatacenter"),
                           PHP5TO7_ZVAL_MAYBE_P(localDatacenter), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "hostPerRemoteDatacenter", sizeof("hostPerRemoteDatacenter"),
                           PHP5TO7_ZVAL_MAYBE_P(hostPerRemoteDatacenter), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "useRemoteDatacenterForLocalConsistencies", sizeof("useRemoteDatacenterForLocalConsistencies"),
                           PHP5TO7_ZVAL_MAYBE_P(useRemoteDatacenterForLocalConsistencies), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "useTokenAwareRouting", sizeof("useTokenAwareRouting"),
                           PHP5TO7_ZVAL_MAYBE_P(useTokenAwareRouting), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "username", sizeof("username"),
                           PHP5TO7_ZVAL_MAYBE_P(username), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "password", sizeof("password"),
                           PHP5TO7_ZVAL_MAYBE_P(password), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "connectTimeout", sizeof("connectTimeout"),
                           PHP5TO7_ZVAL_MAYBE_P(connectTimeout), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "requestTimeout", sizeof("requestTimeout"),
                           PHP5TO7_ZVAL_MAYBE_P(requestTimeout), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "sslOptions", sizeof("sslOptions"),
                           PHP5TO7_ZVAL_MAYBE_P(sslOptions), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "defaultConsistency", sizeof("defaultConsistency"),
                           PHP5TO7_ZVAL_MAYBE_P(defaultConsistency), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "defaultPageSize", sizeof("defaultPageSize"),
                           PHP5TO7_ZVAL_MAYBE_P(defaultPageSize), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "defaultTimeout", sizeof("defaultTimeout"),
                           PHP5TO7_ZVAL_MAYBE_P(defaultTimeout), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "usePersistentSessions", sizeof("usePersistentSessions"),
                           PHP5TO7_ZVAL_MAYBE_P(usePersistentSessions), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "protocolVersion", sizeof("protocolVersion"),
                           PHP5TO7_ZVAL_MAYBE_P(protocolVersion), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "ioThreads", sizeof("ioThreads"),
                           PHP5TO7_ZVAL_MAYBE_P(ioThreads), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "coreConnectionPerHost", sizeof("coreConnectionPerHost"),
                           PHP5TO7_ZVAL_MAYBE_P(coreConnectionPerHost), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "maxConnectionsPerHost", sizeof("maxConnectionsPerHost"),
                           PHP5TO7_ZVAL_MAYBE_P(maxConnectionsPerHost), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "reconnectInterval", sizeof("reconnectInterval"),
                           PHP5TO7_ZVAL_MAYBE_P(reconnectInterval), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "latencyAwareRouting", sizeof("latencyAwareRouting"),
                           PHP5TO7_ZVAL_MAYBE_P(latencyAwareRouting), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "tcpNodelay", sizeof("tcpNodelay"),
                           PHP5TO7_ZVAL_MAYBE_P(tcpNodelay), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "tcpKeepalive", sizeof("tcpKeepalive"),
                           PHP5TO7_ZVAL_MAYBE_P(tcpKeepalive), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "retryPolicy", sizeof("retryPolicy"),
                           PHP5TO7_ZVAL_MAYBE_P(retryPolicy), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "timestampGenerator", sizeof("timestampGenerator"),
                           PHP5TO7_ZVAL_MAYBE_P(timestampGen), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "schemaMetadata", sizeof("schemaMetadata"),
                           PHP5TO7_ZVAL_MAYBE_P(schemaMetadata), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "blacklist_hosts", sizeof("blacklist_hosts"),
                           PHP5TO7_ZVAL_MAYBE_P(blacklistHosts), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "whitelist_hosts", sizeof("whitelist_hosts"),
                           PHP5TO7_ZVAL_MAYBE_P(whitelistHosts), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "blacklist_dcs", sizeof("blacklist_dcs"),
                           PHP5TO7_ZVAL_MAYBE_P(blacklistDCs), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "whitelist_dcs", sizeof("whitelist_dcs"),
                           PHP5TO7_ZVAL_MAYBE_P(whitelistDCs), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "hostnameResolution", sizeof("hostnameResolution"),
                           PHP5TO7_ZVAL_MAYBE_P(hostnameResolution), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "randomizedContactPoints", sizeof("randomizedContactPoints"),
                           PHP5TO7_ZVAL_MAYBE_P(randomizedContactPoints), sizeof(zval));
  PHP5TO7_ZEND_HASH_UPDATE(props, "connectionHeartbeatInterval", sizeof("connectionHeartbeatInterval"),
                           PHP5TO7_ZVAL_MAYBE_P(connectionHeartbeatInterval), sizeof(zval));
}

void php_cassandra_cluster_builder_build(cassandra_cluster_builder_base *builder,
                                         CassCluster *cluster TSRMLS_DC) {
  if (builder->load_balancing_policy == LOAD_BALANCING_ROUND_ROBIN) {
    cass_cluster_set_load_balance_round_robin(cluster);
  }

  if (builder->load_balancing_policy == LOAD_BALANCING_DC_AWARE_ROUND_ROBIN) {
    ASSERT_SUCCESS(cass_cluster_set_load_balance_dc_aware(cluster, builder->local_dc,
                                                          builder->used_hosts_per_remote_dc, builder->allow_remote_dcs_for_local_cl));
  }

  if (builder->blacklist_hosts != NULL) {
    cass_cluster_set_blacklist_filtering(cluster, builder->blacklist_hosts);
  }

  if (builder->whitelist_hosts != NULL) {
    cass_cluster_set_whitelist_filtering(cluster, builder->whitelist_hosts);
  }

  if (builder->blacklist_dcs != NULL) {
    cass_cluster_set_blacklist_dc_filtering(cluster, builder->blacklist_dcs);
  }

  if (builder->whitelist_dcs != NULL) {
    cass_cluster_set_whitelist_dc_filtering(cluster, builder->whitelist_dcs);
  }

  cass_cluster_set_token_aware_routing(cluster, builder->use_token_aware_routing);

  if (builder->username) {
    cass_cluster_set_credentials(cluster, builder->username, builder->password);
  }

  cass_cluster_set_connect_timeout(cluster, builder->connect_timeout);
  cass_cluster_set_request_timeout(cluster, builder->request_timeout);

  if (!PHP5TO7_ZVAL_IS_UNDEF(builder->ssl_options)) {
    cassandra_ssl *options = PHP_CASSANDRA_GET_SSL(PHP5TO7_ZVAL_MAYBE_P(builder->ssl_options));
    cass_cluster_set_ssl(cluster, options->ssl);
  }

  ASSERT_SUCCESS(cass_cluster_set_contact_points(cluster, builder->contact_points));
  ASSERT_SUCCESS(cass_cluster_set_port(cluster, builder->port));

  ASSERT_SUCCESS(cass_cluster_set_protocol_version(cluster, builder->protocol_version));
  ASSERT_SUCCESS(cass_cluster_set_num_threads_io(cluster, builder->io_threads));
  ASSERT_SUCCESS(cass_cluster_set_core_connections_per_host(cluster, builder->core_connections_per_host));
  ASSERT_SUCCESS(cass_cluster_set_max_connections_per_host(cluster, builder->max_connections_per_host));
  cass_cluster_set_reconnect_wait_time(cluster, builder->reconnect_interval);
  cass_cluster_set_latency_aware_routing(cluster, builder->enable_latency_aware_routing);
  cass_cluster_set_tcp_nodelay(cluster, builder->enable_tcp_nodelay);
  cass_cluster_set_tcp_keepalive(cluster, builder->enable_tcp_keepalive, builder->tcp_keepalive_delay);
  cass_cluster_set_use_schema(cluster, builder->enable_schema);
  ASSERT_SUCCESS(cass_cluster_set_use_hostname_resolution(cluster, builder->enable_hostname_resolution));
  ASSERT_SUCCESS(cass_cluster_set_use_randomized_contact_points(cluster, builder->enable_randomized_contact_points));
  cass_cluster_set_connection_heartbeat_interval(cluster, builder->connection_heartbeat_interval);

  if (!PHP5TO7_ZVAL_IS_UNDEF(builder->timestamp_gen)) {
    cassandra_timestamp_gen *timestamp_gen =
        PHP_CASSANDRA_GET_TIMESTAMP_GEN(PHP5TO7_ZVAL_MAYBE_P(builder->timestamp_gen));
    cass_cluster_set_timestamp_gen(cluster, timestamp_gen->gen);
  }

  if (!PHP5TO7_ZVAL_IS_UNDEF(builder->retry_policy)) {
    cassandra_retry_policy *retry_policy =
        PHP_CASSANDRA_GET_RETRY_POLICY(PHP5TO7_ZVAL_MAYBE_P(builder->retry_policy));
    cass_cluster_set_retry_policy(cluster, retry_policy->policy);
  }
}

void php_cassandra_cluster_builder_with_default_consistency(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS)
{
  zval *consistency = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &consistency) == FAILURE) {
    return;
  }

  if (php_cassandra_get_consistency(consistency, &builder->default_consistency TSRMLS_CC) == FAILURE) {
    return;
  }

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_default_page_size(cassandra_cluster_builder_base *builder,
                                                          INTERNAL_FUNCTION_PARAMETERS)
{
  zval *pageSize = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &pageSize) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(pageSize) == IS_NULL) {
    builder->default_page_size = -1;
  } else if (Z_TYPE_P(pageSize) == IS_LONG &&
             Z_LVAL_P(pageSize) > 0) {
    builder->default_page_size = Z_LVAL_P(pageSize);
  } else {
    INVALID_ARGUMENT(pageSize, "a positive integer or null");
  }

  RETURN_ZVAL(getThis(), 1, 0);

}

void php_cassandra_cluster_builder_with_with_default_timeout(cassandra_cluster_builder_base *builder,
                                                             INTERNAL_FUNCTION_PARAMETERS)
{
  zval *timeout = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &timeout) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(timeout) == IS_NULL) {
    PHP5TO7_ZVAL_MAYBE_DESTROY(builder->default_timeout);
    PHP5TO7_ZVAL_UNDEF(builder->default_timeout);
  } else if ((Z_TYPE_P(timeout) == IS_LONG && Z_LVAL_P(timeout) > 0) ||
             (Z_TYPE_P(timeout) == IS_DOUBLE && Z_LVAL_P(timeout) > 0)) {
    PHP5TO7_ZVAL_MAYBE_DESTROY(builder->default_timeout);
    PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(builder->default_timeout), timeout);
  } else {
    INVALID_ARGUMENT(timeout, "a number of seconds greater than zero or null");
  }

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_with_contact_points(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS)
{
  zval *host = NULL;
  php5to7_zval_args args = NULL;
  int argc = 0, i;
  smart_str contactPoints = PHP5TO7_SMART_STR_INIT;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "+", &args, &argc) == FAILURE) {
    return;
  }

  for (i = 0; i < argc; i++) {
    host = PHP5TO7_ZVAL_ARG(args[i]);

    if (Z_TYPE_P(host) != IS_STRING) {
      smart_str_free(&contactPoints);
      throw_invalid_argument(host, "host", "a string ip address or hostname" TSRMLS_CC);
      PHP5TO7_MAYBE_EFREE(args);
      return;
    }

    if (i > 0) {
      smart_str_appendl(&contactPoints, ",", 1);
    }

    smart_str_appendl(&contactPoints, Z_STRVAL_P(host), Z_STRLEN_P(host));
  }

  PHP5TO7_MAYBE_EFREE(args);
  smart_str_0(&contactPoints);

  efree(builder->contact_points);
#if PHP_MAJOR_VERSION >= 7
  builder->contact_points = estrndup(contactPoints.s->val, contactPoints.s->len);
  smart_str_free(&contactPoints);
#else
  builder->contact_points = contactPoints.c;
#endif

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_port(cassandra_cluster_builder_base *builder,
                                             INTERNAL_FUNCTION_PARAMETERS)
{
  zval *port = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &port) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(port) == IS_LONG &&
      Z_LVAL_P(port) > 0 &&
      Z_LVAL_P(port) < 65536) {
    builder->port = Z_LVAL_P(port);
  } else {
    INVALID_ARGUMENT(port, "an integer between 1 and 65535");
  }

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_round_robin_lb_policy(cassandra_cluster_builder_base *builder,
                                                              INTERNAL_FUNCTION_PARAMETERS)
{
  if (zend_parse_parameters_none() == FAILURE) {
    return;
  }

  if (builder->local_dc) {
    efree(builder->local_dc);
    builder->local_dc = NULL;
  }

  builder->load_balancing_policy = LOAD_BALANCING_ROUND_ROBIN;

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_dc_aware_lb_policy(cassandra_cluster_builder_base *builder,
                                                           INTERNAL_FUNCTION_PARAMETERS)
{
  char *local_dc;
  php5to7_size local_dc_len;
  zval *hostPerRemoteDatacenter = NULL;
  zend_bool allow_remote_dcs_for_local_cl;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "szb", &local_dc, &local_dc_len, &hostPerRemoteDatacenter, &allow_remote_dcs_for_local_cl) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(hostPerRemoteDatacenter) != IS_LONG ||
      Z_LVAL_P(hostPerRemoteDatacenter) < 0) {
    INVALID_ARGUMENT(hostPerRemoteDatacenter, "a positive integer");
  }

  if (builder->local_dc) {
    efree(builder->local_dc);
    builder->local_dc = NULL;
  }

  builder->load_balancing_policy         = LOAD_BALANCING_DC_AWARE_ROUND_ROBIN;
  builder->local_dc                      = estrndup(local_dc, local_dc_len);
  builder->used_hosts_per_remote_dc      = Z_LVAL_P(hostPerRemoteDatacenter);
  builder->allow_remote_dcs_for_local_cl = allow_remote_dcs_for_local_cl;

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_blacklist_hosts(cassandra_cluster_builder_base *builder,
                                                        INTERNAL_FUNCTION_PARAMETERS)
{
  zval *hosts = NULL;
  php5to7_zval_args args = NULL;
  int argc = 0, i;
  smart_str blacklist_hosts = PHP5TO7_SMART_STR_INIT;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "+", &args, &argc) == FAILURE) {
    return;
  }

  for (i = 0; i < argc; i++) {
    hosts = PHP5TO7_ZVAL_ARG(args[i]);

    if (Z_TYPE_P(hosts) != IS_STRING) {
      smart_str_free(&blacklist_hosts);
      throw_invalid_argument(hosts, "hosts", "a string ip address or hostname" TSRMLS_CC);
      PHP5TO7_MAYBE_EFREE(args);
      return;
    }

    if (i > 0) {
      smart_str_appendl(&blacklist_hosts, ",", 1);
    }

    smart_str_appendl(&blacklist_hosts, Z_STRVAL_P(hosts), Z_STRLEN_P(hosts));
  }

  PHP5TO7_MAYBE_EFREE(args);
  smart_str_0(&blacklist_hosts);

  efree(builder->blacklist_hosts);
#if PHP_MAJOR_VERSION >= 7
  builder->blacklist_hosts = estrndup(blacklist_hosts.s->val, blacklist_hosts.s->len);
  smart_str_free(&blacklist_hosts);
#else
  builder->blacklist_hosts = blacklist_hosts.c;
#endif

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_whitelist_hosts(cassandra_cluster_builder_base *builder,
                                                        INTERNAL_FUNCTION_PARAMETERS)
{
  zval *hosts = NULL;
  php5to7_zval_args args = NULL;
  int argc = 0, i;
  smart_str whitelist_hosts = PHP5TO7_SMART_STR_INIT;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "+", &args, &argc) == FAILURE) {
    return;
  }

  for (i = 0; i < argc; i++) {
    hosts = PHP5TO7_ZVAL_ARG(args[i]);

    if (Z_TYPE_P(hosts) != IS_STRING) {
      smart_str_free(&whitelist_hosts);
      throw_invalid_argument(hosts, "hosts", "a string ip address or hostname" TSRMLS_CC);
      PHP5TO7_MAYBE_EFREE(args);
      return;
    }

    if (i > 0) {
      smart_str_appendl(&whitelist_hosts, ",", 1);
    }

    smart_str_appendl(&whitelist_hosts, Z_STRVAL_P(hosts), Z_STRLEN_P(hosts));
  }

  PHP5TO7_MAYBE_EFREE(args);
  smart_str_0(&whitelist_hosts);

  efree(builder->whitelist_hosts);
#if PHP_MAJOR_VERSION >= 7
  builder->whitelist_hosts = estrndup(whitelist_hosts.s->val, whitelist_hosts.s->len);
  smart_str_free(&whitelist_hosts);
#else
  builder->whitelist_hosts = whitelist_hosts.c;
#endif

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_blacklist_dcs(cassandra_cluster_builder_base *builder,
                                                      INTERNAL_FUNCTION_PARAMETERS)
{
  zval *dcs = NULL;
  php5to7_zval_args args = NULL;
  int argc = 0, i;
  smart_str blacklist_dcs = PHP5TO7_SMART_STR_INIT;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "+", &args, &argc) == FAILURE) {
    return;
  }

  for (i = 0; i < argc; i++) {
    dcs = PHP5TO7_ZVAL_ARG(args[i]);

    if (Z_TYPE_P(dcs) != IS_STRING) {
      smart_str_free(&blacklist_dcs);
      throw_invalid_argument(dcs, "dcs", "a string" TSRMLS_CC);
      PHP5TO7_MAYBE_EFREE(args);
      return;
    }

    if (i > 0) {
      smart_str_appendl(&blacklist_dcs, ",", 1);
    }

    smart_str_appendl(&blacklist_dcs, Z_STRVAL_P(dcs), Z_STRLEN_P(dcs));
  }

  PHP5TO7_MAYBE_EFREE(args);
  smart_str_0(&blacklist_dcs);

  efree(builder->blacklist_dcs);
#if PHP_MAJOR_VERSION >= 7
  builder->blacklist_dcs = estrndup(blacklist_dcs.s->val, blacklist_dcs.s->len);
  smart_str_free(&blacklist_dcs);
#else
  builder->blacklist_dcs = blacklist_dcs.c;
#endif

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_whitelist_dcs(cassandra_cluster_builder_base *builder,
                                                      INTERNAL_FUNCTION_PARAMETERS)
{
  zval *dcs = NULL;
  php5to7_zval_args args = NULL;
  int argc = 0, i;
  smart_str whitelist_dcs = PHP5TO7_SMART_STR_INIT;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "+", &args, &argc) == FAILURE) {
    return;
  }

  for (i = 0; i < argc; i++) {
    dcs = PHP5TO7_ZVAL_ARG(args[i]);

    if (Z_TYPE_P(dcs) != IS_STRING) {
      smart_str_free(&whitelist_dcs);
      throw_invalid_argument(dcs, "dcs", "a string" TSRMLS_CC);
      PHP5TO7_MAYBE_EFREE(args);
      return;
    }

    if (i > 0) {
      smart_str_appendl(&whitelist_dcs, ",", 1);
    }

    smart_str_appendl(&whitelist_dcs, Z_STRVAL_P(dcs), Z_STRLEN_P(dcs));
  }

  PHP5TO7_MAYBE_EFREE(args);
  smart_str_0(&whitelist_dcs);

  efree(builder->whitelist_dcs);
#if PHP_MAJOR_VERSION >= 7
  builder->whitelist_dcs = estrndup(whitelist_dcs.s->val, whitelist_dcs.s->len);
  smart_str_free(&whitelist_dcs);
#else
  builder->whitelist_dcs = whitelist_dcs.c;
#endif

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_token_aware_routing(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS)
{
  zend_bool enabled = 1;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|b", &enabled) == FAILURE) {
    return;
  }

  builder->use_token_aware_routing = enabled;

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_credentials(cassandra_cluster_builder_base *builder,
                                                    INTERNAL_FUNCTION_PARAMETERS)
{
  zval *username = NULL;
  zval *password = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "zz", &username, &password) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(username) != IS_STRING) {
    INVALID_ARGUMENT(username, "a string");
  }

  if (Z_TYPE_P(password) != IS_STRING) {
    INVALID_ARGUMENT(password, "a string");
  }

  if (builder->username) {
    efree(builder->username);
    efree(builder->password);
  }

  builder->username = estrndup(Z_STRVAL_P(username), Z_STRLEN_P(username));
  builder->password = estrndup(Z_STRVAL_P(password), Z_STRLEN_P(password));

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_connect_timeout(cassandra_cluster_builder_base *builder,
                                                        INTERNAL_FUNCTION_PARAMETERS)
{
  zval *timeout = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &timeout) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(timeout) == IS_LONG &&
      Z_LVAL_P(timeout) > 0) {
    builder->connect_timeout = Z_LVAL_P(timeout) * 1000;
  } else if (Z_TYPE_P(timeout) == IS_DOUBLE &&
             Z_DVAL_P(timeout) > 0) {
    builder->connect_timeout = ceil(Z_DVAL_P(timeout) * 1000);
  } else {
    INVALID_ARGUMENT(timeout, "a positive number");
  }

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_request_timeout(cassandra_cluster_builder_base *builder,
                                                        INTERNAL_FUNCTION_PARAMETERS)
{
  double timeout;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "d", &timeout) == FAILURE) {
    return;
  }

  builder->request_timeout = ceil(timeout * 1000);

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_ssl(cassandra_cluster_builder_base *builder,
                                            INTERNAL_FUNCTION_PARAMETERS)
{
  zval *ssl_options = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "O", &ssl_options, cassandra_ssl_ce) == FAILURE) {
    return;
  }

  if (!PHP5TO7_ZVAL_IS_UNDEF(builder->ssl_options))
    zval_ptr_dtor(&builder->ssl_options);

  PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(builder->ssl_options), ssl_options);

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_persistent_sessions(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS)
{
  zend_bool enabled = 1;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|b", &enabled) == FAILURE) {
    return;
  }

  builder->persist = enabled;

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_protocol_version(cassandra_cluster_builder_base *builder,
                                                         INTERNAL_FUNCTION_PARAMETERS)
{
  zval *version = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &version) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(version) == IS_LONG &&
      Z_LVAL_P(version) >= 1) {
    builder->protocol_version = Z_LVAL_P(version);
  } else {
    INVALID_ARGUMENT(version, "must be >= 1");
  }

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_io_threads(cassandra_cluster_builder_base *builder,
                                                   INTERNAL_FUNCTION_PARAMETERS)
{
  zval *count = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &count) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(count) == IS_LONG &&
      Z_LVAL_P(count) < 129 &&
      Z_LVAL_P(count) > 0) {
    builder->io_threads = Z_LVAL_P(count);
  } else {
    INVALID_ARGUMENT(count, "a number between 1 and 128");
  }

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_connections_per_host(cassandra_cluster_builder_base *builder,
                                                             INTERNAL_FUNCTION_PARAMETERS)
{
  zval *core = NULL;
  zval *max = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z|z", &core, &max) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(core) == IS_LONG &&
      Z_LVAL_P(core) < 129 &&
      Z_LVAL_P(core) > 0) {
    builder->core_connections_per_host = Z_LVAL_P(core);
  } else {
    INVALID_ARGUMENT(core, "a number between 1 and 128");
  }

  if (max == NULL || Z_TYPE_P(max) == IS_NULL) {
    builder->max_connections_per_host = Z_LVAL_P(core);
  } else if (Z_TYPE_P(max) == IS_LONG) {
    if (Z_LVAL_P(max) < Z_LVAL_P(core)) {
      INVALID_ARGUMENT(max, "greater than core");
    } else if (Z_LVAL_P(max) > 128) {
      INVALID_ARGUMENT(max, "less than 128");
    } else {
      builder->max_connections_per_host = Z_LVAL_P(max);
    }
  } else {
    INVALID_ARGUMENT(max, "a number between 1 and 128");
  }

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_reconnect_interval(cassandra_cluster_builder_base *builder,
                                                           INTERNAL_FUNCTION_PARAMETERS)
{
  zval *interval = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &interval) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(interval) == IS_LONG &&
      Z_LVAL_P(interval) > 0) {
    builder->reconnect_interval = Z_LVAL_P(interval) * 1000;
  } else if (Z_TYPE_P(interval) == IS_DOUBLE &&
             Z_DVAL_P(interval) > 0) {
    builder->reconnect_interval = ceil(Z_DVAL_P(interval) * 1000);
  } else {
    INVALID_ARGUMENT(interval, "a positive number");
  }

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_latency_aware_routing(cassandra_cluster_builder_base *builder,
                                                              INTERNAL_FUNCTION_PARAMETERS)
{
  zend_bool enabled = 1;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|b", &enabled) == FAILURE) {
    return;
  }

  builder->enable_latency_aware_routing = enabled;

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_tcp_nodelay(cassandra_cluster_builder_base *builder,
                                                    INTERNAL_FUNCTION_PARAMETERS)
{
  zend_bool enabled = 1;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|b", &enabled) == FAILURE) {
    return;
  }

  builder->enable_tcp_nodelay = enabled;

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_tcp_keepalive(cassandra_cluster_builder_base *builder,
                                                      INTERNAL_FUNCTION_PARAMETERS)
{
  zval *delay = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &delay) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(delay) == IS_NULL) {
    builder->enable_tcp_keepalive = 0;
    builder->tcp_keepalive_delay  = 0;
  } else if (Z_TYPE_P(delay) == IS_LONG &&
             Z_LVAL_P(delay) > 0) {
    builder->enable_tcp_keepalive = 1;
    builder->tcp_keepalive_delay  = Z_LVAL_P(delay) * 1000;
  } else if (Z_TYPE_P(delay) == IS_DOUBLE &&
             Z_DVAL_P(delay) > 0) {
    builder->enable_tcp_keepalive = 1;
    builder->tcp_keepalive_delay  = ceil(Z_DVAL_P(delay) * 1000);
  } else {
    INVALID_ARGUMENT(delay, "a positive number or null");
  }

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_retry_policy(cassandra_cluster_builder_base *builder,
                                                     INTERNAL_FUNCTION_PARAMETERS)
{
  zval *retry_policy = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "O",
                            &retry_policy, cassandra_retry_policy_ce) == FAILURE) {
    return;
  }

  if (!PHP5TO7_ZVAL_IS_UNDEF(builder->retry_policy))
    zval_ptr_dtor(&builder->retry_policy);

  PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(builder->retry_policy), retry_policy);

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_timestamp_generator(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS)
{
  zval *timestamp_gen = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "O",
                            &timestamp_gen, cassandra_timestamp_gen_ce) == FAILURE) {
    return;
  }

  if (!PHP5TO7_ZVAL_IS_UNDEF(builder->timestamp_gen))
    zval_ptr_dtor(&builder->timestamp_gen);

  PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(builder->timestamp_gen), timestamp_gen);

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_schema_metadata(cassandra_cluster_builder_base *builder,
                                                        INTERNAL_FUNCTION_PARAMETERS)
{
  zend_bool enabled = 1;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|b", &enabled) == FAILURE) {
    return;
  }

  builder->enable_schema = enabled;

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_hostname_resolution(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS)
{
  zend_bool enabled = 1;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|b", &enabled) == FAILURE) {
    return;
  }

  builder->enable_hostname_resolution = enabled;

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_randomized_contact_points(cassandra_cluster_builder_base *builder,
                                                                  INTERNAL_FUNCTION_PARAMETERS)
{
  zend_bool enabled = 1;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|b", &enabled) == FAILURE) {
    return;
  }

  builder->enable_randomized_contact_points = enabled;

  RETURN_ZVAL(getThis(), 1, 0);
}

void php_cassandra_cluster_builder_with_connection_heartbeat_interval(cassandra_cluster_builder_base *builder,
                                                                      INTERNAL_FUNCTION_PARAMETERS)
{
  zval *interval = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &interval) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(interval) == IS_LONG &&
      Z_LVAL_P(interval) >= 0) {
    builder->connection_heartbeat_interval = Z_LVAL_P(interval);
  } else if (Z_TYPE_P(interval) == IS_DOUBLE &&
             Z_DVAL_P(interval) >= 0) {
    builder->connection_heartbeat_interval = ceil(Z_DVAL_P(interval));
  } else {
    INVALID_ARGUMENT(interval, "a positive number (or 0 to disable)");
  }

  RETURN_ZVAL(getThis(), 1, 0);
}
