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

#ifndef PHP_CASSANDRA_CLUSTER_H
#define PHP_CASSANDRA_CLUSTER_H

void php_cassandra_cluster_init(cassandra_cluster_base *cluster);

void php_cassandra_cluster_destroy(cassandra_cluster_base *cluster);

void php_cassandra_cluster_connect(cassandra_cluster_base *cluster,
                                   char *keyspace, php5to7_size keyspace_len,
                                   zval *timeout,
                                   cassandra_session_base *session);

void php_cassandra_cluster_connect_async(cassandra_cluster_base *cluster,
                                         char *keyspace, php5to7_size keyspace_len,
                                         cassandra_future_session_base *future);

#endif /* PHP_CASSANDRA_CLUSTER_H */
