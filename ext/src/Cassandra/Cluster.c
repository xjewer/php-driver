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
#include "util/ref.h"

zend_class_entry *cassandra_cluster_ce = NULL;

ZEND_BEGIN_ARG_INFO_EX(arginfo_keyspace, 0, ZEND_RETURN_VALUE, 0)
  ZEND_ARG_INFO(0, keyspace)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_cluster_methods[] = {
  PHP_ABSTRACT_ME(Cluster, connect, arginfo_keyspace)
  PHP_ABSTRACT_ME(Cluster, connectAsync, arginfo_keyspace)
  PHP_FE_END
};

void cassandra_define_Cluster(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\Cluster", cassandra_cluster_methods);
  cassandra_cluster_ce = zend_register_internal_class(&ce TSRMLS_CC);
  cassandra_cluster_ce->ce_flags |= ZEND_ACC_INTERFACE;
}

static void free_session(void *session)
{
  cass_session_free((CassSession*) session);
}


void php_cassandra_cluster_init(cassandra_cluster_base *cluster)
{
  cluster->cluster             = NULL;
  cluster->default_consistency = PHP_CASSANDRA_DEFAULT_CONSISTENCY;
  cluster->default_page_size   = 5000;
  cluster->persist             = 0;
  cluster->hash_key            = NULL;

  PHP5TO7_ZVAL_UNDEF(cluster->default_timeout);
}


void php_cassandra_cluster_destroy(cassandra_cluster_base *cluster)
{
  if (cluster->persist) {
    efree(cluster->hash_key);
  } else {
    if (cluster->cluster) {
      cass_cluster_free(cluster->cluster);
    }
  }

  PHP5TO7_ZVAL_MAYBE_DESTROY(cluster->default_timeout);
}

void php_cassandra_cluster_connect(cassandra_cluster_base *cluster,
                                   char *keyspace, php5to7_size keyspace_len,
                                   zval *timeout,
                                   cassandra_session_base *session)
{
  CassFuture *future = NULL;
  char *hash_key;
  php5to7_size hash_key_len = 0;
  cassandra_psession *psession;

  session->default_consistency = cluster->default_consistency;
  session->default_page_size   = cluster->default_page_size;
  session->persist             = cluster->persist;

  if (!PHP5TO7_ZVAL_IS_UNDEF(session->default_timeout)) {
    PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(session->default_timeout),
                      PHP5TO7_ZVAL_MAYBE_P(cluster->default_timeout));
  }

  if (session->persist) {
    php5to7_zend_resource_le *le;

    hash_key_len = spprintf(&hash_key, 0, "%s:session:%s",
                            cluster->hash_key, SAFE_STR(keyspace));

    if (PHP5TO7_ZEND_HASH_FIND(&EG(persistent_list), hash_key, hash_key_len + 1, le) &&
        Z_RES_P(le)->type == php_le_cassandra_session()) {
      psession = (cassandra_psession *) Z_RES_P(le)->ptr;
      session->session = php_cassandra_add_ref(psession->session);
      future = psession->future;
    }
  }

  if (future == NULL) {
    php5to7_zend_resource_le resource;

    session->session = php_cassandra_new_peref(cass_session_new(), free_session, 1);

    if (keyspace) {
      future = cass_session_connect_keyspace((CassSession *) session->session->data,
                                             cluster->cluster,
                                             keyspace);
    } else {
      future = cass_session_connect((CassSession *) session->session->data,
                                    cluster->cluster);
    }

    if (session->persist) {
      psession = (cassandra_psession *) pecalloc(1, sizeof(cassandra_psession), 1);
      psession->session = php_cassandra_add_ref(session->session);
      psession->future  = future;

#if PHP_MAJOR_VERSION >= 7
      ZVAL_NEW_PERSISTENT_RES(&resource, 0, psession, php_le_cassandra_session());
      PHP5TO7_ZEND_HASH_UPDATE(&EG(persistent_list), hash_key, hash_key_len + 1, &resource, sizeof(php5to7_zend_resource_le));
      CASSANDRA_G(persistent_sessions)++;
#else
      resource.type = php_le_cassandra_session();
      resource.ptr = psession;
      PHP5TO7_ZEND_HASH_UPDATE(&EG(persistent_list), hash_key, hash_key_len + 1, resource, sizeof(php5to7_zend_resource_le));
      CASSANDRA_G(persistent_sessions)++;
#endif
    }
  }

  if (php_cassandra_future_wait_timed(future, timeout TSRMLS_CC) == FAILURE) {
    if (session->persist) {
      efree(hash_key);
    } else {
      cass_future_free(future);
    }

    return;
  }

  if (php_cassandra_future_is_error(future TSRMLS_CC) == FAILURE) {
    if (session->persist) {
      if (PHP5TO7_ZEND_HASH_DEL(&EG(persistent_list), hash_key, hash_key_len + 1)) {
        // TODO: Is this correct?
        php_cassandra_del_peref(&session->session, 1);
      }

      efree(hash_key);
    } else {
      cass_future_free(future);
    }

    return;
  }

  if (session->persist)
    efree(hash_key);
}


void php_cassandra_cluster_connect_async(cassandra_cluster_base *cluster,
                                         char *keyspace, php5to7_size keyspace_len,
                                         cassandra_future_session_base *future)
{
  char *hash_key;
  php5to7_size hash_key_len = 0;


  future->persist = cluster->persist;

  if (cluster->persist) {
    php5to7_zend_resource_le *le;

    hash_key_len = spprintf(&hash_key, 0,
      "%s:session:%s", cluster->hash_key, SAFE_STR(keyspace));

    future->hash_key     = hash_key;
    future->hash_key_len = hash_key_len;

    if (PHP5TO7_ZEND_HASH_FIND(&EG(persistent_list), hash_key, hash_key_len + 1, le)) {
      if (Z_TYPE_P(le) == php_le_cassandra_session()) {
        cassandra_psession *psession = (cassandra_psession *) Z_RES_P(le)->ptr;
        future->session = php_cassandra_add_ref(psession->session);
        future->future  = psession->future;
        return;
      }
    }
  }

  future->session = php_cassandra_new_peref(cass_session_new(), free_session, 1);

  if (keyspace) {
    future->future = cass_session_connect_keyspace((CassSession *) future->session->data,
                                                   cluster->cluster,
                                                   keyspace);
  } else {
    future->future = cass_session_connect((CassSession *) future->session->data,
                                          cluster->cluster);
  }

  if (cluster->persist) {
    php5to7_zend_resource_le resource;
    cassandra_psession *psession =
      (cassandra_psession *) pecalloc(1, sizeof(cassandra_psession), 1);
    psession->session = php_cassandra_add_ref(future->session);
    psession->future  = future->future;

#if PHP_MAJOR_VERSION >= 7
    ZVAL_NEW_PERSISTENT_RES(&resource, 0, psession, php_le_cassandra_session());
    PHP5TO7_ZEND_HASH_UPDATE(&EG(persistent_list), hash_key, hash_key_len + 1, &resource, sizeof(php5to7_zend_resource_le));
    CASSANDRA_G(persistent_sessions)++;
#else
      resource.type = php_le_cassandra_session();
      resource.ptr = psession;
      PHP5TO7_ZEND_HASH_UPDATE(&EG(persistent_list), hash_key, hash_key_len + 1, resource, sizeof(php5to7_zend_resource_le));
      CASSANDRA_G(persistent_sessions)++;
#endif

  }
}
