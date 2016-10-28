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
#include "util/future.h"

#include "Cluster.h"

zend_class_entry *cassandra_default_cluster_ce = NULL;

PHP_METHOD(DefaultCluster, connect)
{
  char *keyspace = NULL;
  php5to7_size keyspace_len;
  zval *timeout = NULL;
  cassandra_cluster_base *cluster = NULL;
  cassandra_session_base *session = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|sz", &keyspace, &keyspace_len, &timeout) == FAILURE) {
    return;
  }

  cluster = &PHP_CASSANDRA_GET_CLUSTER(getThis())->base;

  object_init_ex(return_value, cassandra_default_session_ce);
  session = &PHP_CASSANDRA_GET_SESSION(return_value)->base;

  session->default_consistency = cluster->default_consistency;
  session->default_page_size   = cluster->default_page_size;
  session->persist             = cluster->persist;

  if (!PHP5TO7_ZVAL_IS_UNDEF(session->default_timeout)) {
    PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(session->default_timeout),
                      PHP5TO7_ZVAL_MAYBE_P(cluster->default_timeout));
  }

  php_cassandra_cluster_connect(cluster,
                                keyspace, keyspace_len,
                                timeout,
                                session);

}

PHP_METHOD(DefaultCluster, connectAsync)
{
  char *keyspace = NULL;
  php5to7_size keyspace_len;
  cassandra_cluster_base *cluster = NULL;
  cassandra_future_session_base *future = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|s", &keyspace, &keyspace_len) == FAILURE) {
    return;
  }

  cluster = &PHP_CASSANDRA_GET_CLUSTER(getThis())->base;

  object_init_ex(return_value, cassandra_future_session_ce);
  future = &PHP_CASSANDRA_GET_FUTURE_SESSION(return_value)->base;

  future->persist = cluster->persist;

  php_cassandra_cluster_connect_async(cluster,
                                      keyspace, keyspace_len,
                                      future);

}

ZEND_BEGIN_ARG_INFO_EX(arginfo_connect, 0, ZEND_RETURN_VALUE, 0)
  ZEND_ARG_INFO(0, keyspace)
  ZEND_ARG_INFO(0, timeout)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_connectAsync, 0, ZEND_RETURN_VALUE, 0)
  ZEND_ARG_INFO(0, keyspace)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_default_cluster_methods[] = {
  PHP_ME(DefaultCluster, connect, arginfo_connect, ZEND_ACC_PUBLIC)
  PHP_ME(DefaultCluster, connectAsync, arginfo_connectAsync, ZEND_ACC_PUBLIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_default_cluster_handlers;

static HashTable *
php_cassandra_default_cluster_properties(zval *object TSRMLS_DC)
{
  HashTable *props = zend_std_get_properties(object TSRMLS_CC);

  return props;
}

static int
php_cassandra_default_cluster_compare(zval *obj1, zval *obj2 TSRMLS_DC)
{
  if (Z_OBJCE_P(obj1) != Z_OBJCE_P(obj2))
    return 1; /* different classes */

  return Z_OBJ_HANDLE_P(obj1) != Z_OBJ_HANDLE_P(obj1);
}

static void
php_cassandra_default_cluster_free(php5to7_zend_object_free *object TSRMLS_DC)
{
  cassandra_cluster *self = PHP5TO7_ZEND_OBJECT_GET(cassandra_cluster, object);

  php_cassandra_cluster_destroy(&self->base);

  zend_object_std_dtor(&self->zval TSRMLS_CC);
  PHP5TO7_MAYBE_EFREE(self);
}

static php5to7_zend_object
php_cassandra_default_cluster_new(zend_class_entry *ce TSRMLS_DC)
{
  cassandra_cluster *self =
      PHP5TO7_ZEND_OBJECT_ECALLOC(cassandra_cluster, ce);

  php_cassandra_cluster_init(&self->base);

  PHP5TO7_ZEND_OBJECT_INIT_EX(cassandra_cluster, cassandra_default_cluster, self, ce);
}

void cassandra_define_DefaultCluster(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\DefaultCluster", cassandra_default_cluster_methods);
  cassandra_default_cluster_ce = zend_register_internal_class(&ce TSRMLS_CC);
  zend_class_implements(cassandra_default_cluster_ce TSRMLS_CC, 1, cassandra_cluster_ce);
  cassandra_default_cluster_ce->ce_flags     |= PHP5TO7_ZEND_ACC_FINAL;
  cassandra_default_cluster_ce->create_object = php_cassandra_default_cluster_new;

  memcpy(&cassandra_default_cluster_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_default_cluster_handlers.get_properties  = php_cassandra_default_cluster_properties;
  cassandra_default_cluster_handlers.compare_objects = php_cassandra_default_cluster_compare;
}
