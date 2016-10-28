#ifndef PHP_CASSANDRA_CLUSTER_BUILDER_H
#define PHP_CASSANDRA_CLUSTER_BUILDER_H

#include "php_cassandra_types.h"

void php_cassandra_cluster_builder_generate_hash_key(cassandra_cluster_builder_base *builder,
                                                     char **hash_key, int *hash_key_len);

CassCluster *php_cassandra_cluster_builder_get_cache(cassandra_cluster_builder_base *builder,
                                                     const char *hash_key, int hash_key_len);

void php_cassandra_cluster_builder_add_cache(cassandra_cluster_builder_base *builder,
                                             const char *hash_key, int hash_key_len,
                                             CassCluster *cluster);

void php_cassandra_cluster_builder_init(cassandra_cluster_builder_base *builder);

void php_cassandra_cluster_builder_destroy(cassandra_cluster_builder_base *builder);

void php_cassandra_cluster_builder_properties(cassandra_cluster_builder_base *builder,
                                              HashTable *props);

void php_cassandra_cluster_builder_build(cassandra_cluster_builder_base *builder,
                                         CassCluster *cluster);

void php_cassandra_cluster_builder_with_default_consistency(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_default_page_size(cassandra_cluster_builder_base *builder,
                                                          INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_with_default_timeout(cassandra_cluster_builder_base *builder,
                                                             INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_with_contact_points(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_port(cassandra_cluster_builder_base *builder,
                                             INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_round_robin_lb_policy(cassandra_cluster_builder_base *builder,
                                                              INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_dc_aware_lb_policy(cassandra_cluster_builder_base *builder,
                                                           INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_blacklist_hosts(cassandra_cluster_builder_base *builder,
                                                        INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_whitelist_hosts(cassandra_cluster_builder_base *builder,
                                                        INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_blacklist_dcs(cassandra_cluster_builder_base *builder,
                                                      INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_whitelist_dcs(cassandra_cluster_builder_base *builder,
                                                      INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_token_aware_routing(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_credentials(cassandra_cluster_builder_base *builder,
                                                    INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_connect_timeout(cassandra_cluster_builder_base *builder,
                                                        INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_request_timeout(cassandra_cluster_builder_base *builder,
                                                        INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_ssl(cassandra_cluster_builder_base *builder,
                                            INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_persistent_sessions(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_protocol_version(cassandra_cluster_builder_base *builder,
                                                         INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_io_threads(cassandra_cluster_builder_base *builder,
                                                   INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_connections_per_host(cassandra_cluster_builder_base *builder,
                                                             INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_reconnect_interval(cassandra_cluster_builder_base *builder,
                                                           INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_latency_aware_routing(cassandra_cluster_builder_base *builder,
                                                              INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_tcp_nodelay(cassandra_cluster_builder_base *builder,
                                                    INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_tcp_keepalive(cassandra_cluster_builder_base *builder,
                                                      INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_retry_policy(cassandra_cluster_builder_base *builder,
                                                     INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_timestamp_generator(cassandra_cluster_builder_base *builder,
                                                            INTERNAL_FUNCTION_PARAMETERS);

void php_cassandra_cluster_builder_with_schema_metadata(cassandra_cluster_builder_base *builder,
                                                        INTERNAL_FUNCTION_PARAMETERS);

#endif /* PHP_CASSANDRA_CLUSTER_BUILDER_H */
