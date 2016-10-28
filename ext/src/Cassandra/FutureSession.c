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

#include "FutureSession.h"

zend_class_entry *cassandra_future_session_ce = NULL;

PHP_METHOD(FutureSession, get)
{
  zval *timeout = NULL;
  cassandra_session_base *session = NULL;
  cassandra_future_session_base *future = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|z", &timeout) == FAILURE) {
    return;
  }

  future = &PHP_CASSANDRA_GET_FUTURE_SESSION(getThis())->base;

  if (!PHP5TO7_ZVAL_IS_UNDEF(future->default_session)) {
    RETURN_ZVAL(PHP5TO7_ZVAL_MAYBE_P(future->default_session), 1, 0);
  }

  object_init_ex(return_value, cassandra_default_session_ce);
  session = &PHP_CASSANDRA_GET_SESSION(return_value)->base;

  php_cassandra_future_session_get(future, timeout, session);

  PHP5TO7_ZVAL_COPY(PHP5TO7_ZVAL_MAYBE_P(future->default_session), return_value);
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_timeout, 0, ZEND_RETURN_VALUE, 0)
  ZEND_ARG_INFO(0, timeout)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_future_session_methods[] = {
  PHP_ME(FutureSession, get, arginfo_timeout, ZEND_ACC_PUBLIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_future_session_handlers;

static HashTable *
php_cassandra_future_session_properties(zval *object TSRMLS_DC)
{
  HashTable *props = zend_std_get_properties(object TSRMLS_CC);

  return props;
}

static int
php_cassandra_future_session_compare(zval *obj1, zval *obj2 TSRMLS_DC)
{
  if (Z_OBJCE_P(obj1) != Z_OBJCE_P(obj2))
    return 1; /* different classes */

  return Z_OBJ_HANDLE_P(obj1) != Z_OBJ_HANDLE_P(obj1);
}

static void
php_cassandra_future_session_free(php5to7_zend_object_free *object TSRMLS_DC)
{
  cassandra_future_session *self =
      PHP5TO7_ZEND_OBJECT_GET(cassandra_future_session, object);

  php_cassandra_future_session_init(&self->base);

  zend_object_std_dtor(&self->zval TSRMLS_CC);
  PHP5TO7_MAYBE_EFREE(self);
}

static php5to7_zend_object
php_cassandra_future_session_new(zend_class_entry *ce TSRMLS_DC)
{
  cassandra_future_session *self
      = PHP5TO7_ZEND_OBJECT_ECALLOC(cassandra_future_session, ce);

  php_cassandra_future_session_destroy(&self->base);

  PHP5TO7_ZEND_OBJECT_INIT(cassandra_future_session, self, ce);
}

void cassandra_define_FutureSession(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\FutureSession", cassandra_future_session_methods);
  cassandra_future_session_ce = zend_register_internal_class(&ce TSRMLS_CC);
  zend_class_implements(cassandra_future_session_ce TSRMLS_CC, 1, cassandra_future_ce);
  cassandra_future_session_ce->ce_flags     |= PHP5TO7_ZEND_ACC_FINAL;
  cassandra_future_session_ce->create_object = php_cassandra_future_session_new;

  memcpy(&cassandra_future_session_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_future_session_handlers.get_properties  = php_cassandra_future_session_properties;
  cassandra_future_session_handlers.compare_objects = php_cassandra_future_session_compare;
  cassandra_future_session_handlers.clone_obj = NULL;
}

void php_cassandra_future_session_init(cassandra_future_session_base *future)
{
  future->session           = NULL;
  future->future            = NULL;
  future->exception_message = NULL;
  future->hash_key          = NULL;
  future->persist           = 0;
}

void php_cassandra_future_session_destroy(cassandra_future_session_base *future)
{
  if (future->persist) {
    efree(future->hash_key);
  } else {
    if (future->future) {
      cass_future_free(future->future);
    }
  }

  php_cassandra_del_peref(&future->session, 1);

  if (future->exception_message) {
    efree(future->exception_message);
  }
}

void php_cassandra_future_session_get(cassandra_future_session_base *future,
                                      zval *timeout,
                                      cassandra_session_base *session)
{
  CassError rc = CASS_OK;

  session->session = php_cassandra_add_ref(future->session);
  session->persist = future->persist;

  if (future->exception_message) {
    zend_throw_exception_ex(exception_class(future->exception_code),
                            future->exception_code TSRMLS_CC, future->exception_message);
    return;
  }

  if (php_cassandra_future_wait_timed(future->future, timeout TSRMLS_CC) == FAILURE) {
    return;
  }

  rc = cass_future_error_code(future->future);

  if (rc != CASS_OK) {
    const char *message;
    size_t message_len;
    cass_future_error_message(future->future, &message, &message_len);

    if (future->persist) {
      future->exception_message = estrndup(message, message_len);
      future->exception_code    = rc;

      if (PHP5TO7_ZEND_HASH_DEL(&EG(persistent_list), future->hash_key, future->hash_key_len + 1)) {
        // TODO: Is this right?
        php_cassandra_del_peref(&future->session, 1);
        future->future  = NULL;
      }

      zend_throw_exception_ex(exception_class(future->exception_code),
                              future->exception_code TSRMLS_CC, future->exception_message);
      return;
    }

    zend_throw_exception_ex(exception_class(rc), rc TSRMLS_CC,
                            "%.*s", (int) message_len, message);
    return;
  }
}
