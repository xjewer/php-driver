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
#include "util/consistency.h"

#include "Builder.h"

#if PHP_MAJOR_VERSION >= 7
#include <zend_smart_str.h>
#else
#include <ext/standard/php_smart_str.h>
#endif

zend_class_entry *cassandra_default_cluster_builder_ce = NULL;

PHP_METHOD(ClusterBuilder, build)
{
  cassandra_cluster_base *cluster = NULL;

  cassandra_cluster_builder_base* builder = &PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis())->base;

  object_init_ex(return_value, cassandra_default_cluster_ce);
  cluster = &PHP_CASSANDRA_GET_CLUSTER(return_value)->base;

  cluster->persist             = builder->persist;
  cluster->default_consistency = builder->default_consistency;
  cluster->default_page_size   = builder->default_page_size;

  PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(cluster->default_timeout),
                    PHP5TO7_ZVAL_MAYBE_P(builder->default_timeout));

  if (builder->persist) {
    php_cassandra_cluster_builder_generate_hash_key(builder,
                                                    &cluster->hash_key,
                                                    &cluster->hash_key_len);

    cluster->cluster = php_cassandra_cluster_builder_get_cache(builder,
                                                               cluster->hash_key,
                                                               cluster->hash_key_len TSRMLS_CC);
    if (cluster->cluster) {
      return;
    }
  }

  cluster->cluster = cass_cluster_new();
  php_cassandra_cluster_builder_build(builder, cluster->cluster TSRMLS_CC);

  if (builder->persist) {
    php_cassandra_cluster_builder_add_cache(builder,
                                            cluster->hash_key,
                                            cluster->hash_key_len,
                                            cluster->cluster TSRMLS_CC);
  }
}

PHP_METHOD(ClusterBuilder, withDefaultConsistency)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_default_consistency(&self->base,
                                                         INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withDefaultPageSize)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_default_page_size(&self->base,
                                                       INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withDefaultTimeout)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_with_default_timeout(&self->base,
                                                          INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withContactPoints)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_with_contact_points(&self->base,
                                                         INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withPort)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_port(&self->base,
                                          INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withRoundRobinLoadBalancingPolicy)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_round_robin_lb_policy(&self->base,
                                                           INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withDatacenterAwareRoundRobinLoadBalancingPolicy)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_dc_aware_lb_policy(&self->base,
                                                        INTERNAL_FUNCTION_PARAM_PASSTHRU);
}


PHP_METHOD(ClusterBuilder, withBlackListHosts)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_blacklist_hosts(&self->base,
                                                     INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withWhiteListHosts)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_whitelist_hosts(&self->base,
                                                     INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withBlackListDCs)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_blacklist_dcs(&self->base,
                                                   INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withWhiteListDCs)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_whitelist_dcs(&self->base,
                                                   INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withTokenAwareRouting)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_token_aware_routing(&self->base,
                                                         INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withCredentials)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_credentials(&self->base,
                                                 INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withConnectTimeout)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_connect_timeout(&self->base,
                                                     INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withRequestTimeout)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_request_timeout(&self->base,
                                                     INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withSSL)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_ssl(&self->base,
                                         INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withPersistentSessions)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_persistent_sessions(&self->base,
                                                         INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withProtocolVersion)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_protocol_version(&self->base,
                                                      INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withIOThreads)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_io_threads(&self->base,
                                                INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withConnectionsPerHost)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_connections_per_host(&self->base,
                                                          INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withReconnectInterval)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_reconnect_interval(&self->base,
                                                        INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withLatencyAwareRouting)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_latency_aware_routing(&self->base,
                                                           INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withTCPNodelay)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_tcp_nodelay(&self->base,
                                                 INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withTCPKeepalive)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_tcp_keepalive(&self->base,
                                                   INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withRetryPolicy)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_retry_policy(&self->base,
                                                  INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withTimestampGenerator)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_timestamp_generator(&self->base,
                                                         INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withSchemaMetadata)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_schema_metadata(&self->base,
                                                     INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withHostnameResolution)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_hostname_resolution(&self->base,
                                                         INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withRandomizedContactPoints)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_randomized_contact_points(&self->base,
                                                               INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(ClusterBuilder, withConnectionHeartbeatInterval)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(getThis());
  php_cassandra_cluster_builder_with_connection_heartbeat_interval(&self->base,
                                                                   INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

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

static zend_function_entry cassandra_default_cluster_builder_methods[] = {
  PHP_ME(ClusterBuilder, build, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withDefaultConsistency, arginfo_consistency, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withDefaultPageSize, arginfo_page_size, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withDefaultTimeout, arginfo_timeout, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withContactPoints, arginfo_contact_points, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withPort, arginfo_port, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withRoundRobinLoadBalancingPolicy, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withDatacenterAwareRoundRobinLoadBalancingPolicy, arginfo_dc_aware, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withBlackListHosts, arginfo_blacklist_nodes, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withWhiteListHosts, arginfo_whitelist_nodes, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withBlackListDCs, arginfo_blacklist_dcs, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withWhiteListDCs, arginfo_whitelist_dcs, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withTokenAwareRouting, arginfo_enabled, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withCredentials, arginfo_credentials, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withConnectTimeout, arginfo_timeout, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withRequestTimeout, arginfo_timeout, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withSSL, arginfo_ssl, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withPersistentSessions, arginfo_enabled, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withProtocolVersion, arginfo_version, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withIOThreads, arginfo_count, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withConnectionsPerHost, arginfo_connections, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withReconnectInterval, arginfo_interval, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withLatencyAwareRouting, arginfo_enabled, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withTCPNodelay, arginfo_enabled, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withTCPKeepalive, arginfo_delay, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withRetryPolicy, arginfo_retry_policy, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withTimestampGenerator, arginfo_timestamp_gen, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withSchemaMetadata, arginfo_enabled, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withHostnameResolution, arginfo_enabled, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withRandomizedContactPoints, arginfo_enabled, ZEND_ACC_PUBLIC)
  PHP_ME(ClusterBuilder, withConnectionHeartbeatInterval, arginfo_interval, ZEND_ACC_PUBLIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_default_cluster_builder_handlers;

static HashTable*
php_cassandra_default_cluster_builder_gc(zval *object, php5to7_zval_gc table, int *n TSRMLS_DC)
{
  *table = NULL;
  *n = 0;
  return zend_std_get_properties(object TSRMLS_CC);
}

static HashTable*
php_cassandra_default_cluster_builder_properties(zval *object TSRMLS_DC)
{
  cassandra_cluster_builder *self = PHP_CASSANDRA_GET_CLUSTER_BUILDER(object);
  HashTable *props = zend_std_get_properties(object TSRMLS_CC);

  php_cassandra_cluster_builder_properties(&self->base, props);

  return props;
}

static int
php_cassandra_default_cluster_builder_compare(zval *obj1, zval *obj2 TSRMLS_DC)
{
  if (Z_OBJCE_P(obj1) != Z_OBJCE_P(obj2))
    return 1; /* different classes */

  return Z_OBJ_HANDLE_P(obj1) != Z_OBJ_HANDLE_P(obj1);
}

static void
php_cassandra_default_cluster_builder_free(php5to7_zend_object_free *object TSRMLS_DC)
{
  cassandra_cluster_builder *self =
      PHP5TO7_ZEND_OBJECT_GET(cassandra_cluster_builder, object);

  php_cassandra_cluster_builder_destroy(&self->base);

  zend_object_std_dtor(&self->zval TSRMLS_CC);
  PHP5TO7_MAYBE_EFREE(self);
}

static php5to7_zend_object
php_cassandra_default_cluster_builder_new(zend_class_entry *ce TSRMLS_DC)
{
  cassandra_cluster_builder *self =
      PHP5TO7_ZEND_OBJECT_ECALLOC(cassandra_cluster_builder, ce);

  php_cassandra_cluster_builder_init(&self->base);

  PHP5TO7_ZEND_OBJECT_INIT_EX(cassandra_cluster_builder, cassandra_default_cluster_builder, self, ce);
}

void cassandra_define_DefaultClusterBuilder(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\Cluster\\DefaultBuilder", cassandra_default_cluster_builder_methods);
  cassandra_default_cluster_builder_ce = zend_register_internal_class(&ce TSRMLS_CC);
  zend_class_implements(cassandra_default_cluster_builder_ce TSRMLS_CC, 1, cassandra_cluster_builder_ce);
  cassandra_default_cluster_builder_ce->ce_flags     |= PHP5TO7_ZEND_ACC_FINAL;
  cassandra_default_cluster_builder_ce->create_object = php_cassandra_default_cluster_builder_new;

  memcpy(&cassandra_default_cluster_builder_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_default_cluster_builder_handlers.get_properties  = php_cassandra_default_cluster_builder_properties;
#if PHP_VERSION_ID >= 50400
  cassandra_default_cluster_builder_handlers.get_gc          = php_cassandra_default_cluster_builder_gc;
#endif
  cassandra_default_cluster_builder_handlers.compare_objects = php_cassandra_default_cluster_builder_compare;
}
