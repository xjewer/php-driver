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

#include "Session.h"

zend_class_entry *cassandra_default_session_ce = NULL;

PHP_METHOD(DefaultSession, execute)
{
  zval *statement = NULL;
  zval *options = NULL;
  cassandra_session *self = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z|z", &statement, &options) == FAILURE) {
    return;
  }

  self = PHP_CASSANDRA_GET_SESSION(getThis());

  php_cassandra_session_execute(&self->base, statement, options, return_value TSRMLS_CC);
}

PHP_METHOD(DefaultSession, executeAsync)
{
  zval *statement = NULL;
  zval *options = NULL;
  cassandra_session *self = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z|z", &statement, &options) == FAILURE) {
    return;
  }

  self = PHP_CASSANDRA_GET_SESSION(getThis());

  php_cassandra_session_execute_async(&self->base, statement, options, return_value TSRMLS_CC);
}

PHP_METHOD(DefaultSession, prepare)
{
  zval *cql = NULL;
  zval *options = NULL;
  cassandra_session *self = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z|z", &cql, &options) == FAILURE) {
    return;
  }

  self = PHP_CASSANDRA_GET_SESSION(getThis());

  php_cassandra_session_prepare(&self->base, cql, options, return_value TSRMLS_CC);
}

PHP_METHOD(DefaultSession, prepareAsync)
{
  zval *cql = NULL;
  zval *options = NULL;
  cassandra_session *self = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z|z", &cql, &options) == FAILURE) {
    return;
  }

  self = PHP_CASSANDRA_GET_SESSION(getThis());

  php_cassandra_session_prepare_async(&self->base, cql, options, return_value TSRMLS_CC);
}

PHP_METHOD(DefaultSession, close)
{
  zval *timeout = NULL;

  cassandra_session *self = PHP_CASSANDRA_GET_SESSION(getThis());

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|z", &timeout) == FAILURE) {
    return;
  }

  php_cassandra_session_close(&self->base, timeout, return_value TSRMLS_CC);
}

PHP_METHOD(DefaultSession, closeAsync)
{
  cassandra_session *self = PHP_CASSANDRA_GET_SESSION(getThis());

  if (zend_parse_parameters_none() == FAILURE) {
    return;
  }

  php_cassandra_session_close_async(&self->base, return_value TSRMLS_CC);
}

PHP_METHOD(DefaultSession, schema)
{
  cassandra_session *self = PHP_CASSANDRA_GET_SESSION(getThis());

  if (zend_parse_parameters_none() == FAILURE)
    return;

  php_cassandra_session_schema(&self->base, return_value TSRMLS_CC);
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_execute, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_OBJ_INFO(0, statement, Cassandra\\Statement, 0)
  ZEND_ARG_OBJ_INFO(0, options, Cassandra\\ExecutionOptions, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_prepare, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, cql)
  ZEND_ARG_OBJ_INFO(0, options, Cassandra\\ExecutionOptions, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_timeout, 0, ZEND_RETURN_VALUE, 0)
  ZEND_ARG_INFO(0, timeout)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_none, 0, ZEND_RETURN_VALUE, 0)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_default_session_methods[] = {
  PHP_ME(DefaultSession, execute, arginfo_execute, ZEND_ACC_PUBLIC)
  PHP_ME(DefaultSession, executeAsync, arginfo_execute, ZEND_ACC_PUBLIC)
  PHP_ME(DefaultSession, prepare, arginfo_prepare, ZEND_ACC_PUBLIC)
  PHP_ME(DefaultSession, prepareAsync, arginfo_prepare, ZEND_ACC_PUBLIC)
  PHP_ME(DefaultSession, close, arginfo_timeout, ZEND_ACC_PUBLIC)
  PHP_ME(DefaultSession, closeAsync, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(DefaultSession, schema, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_default_session_handlers;

static HashTable *
php_cassandra_default_session_properties(zval *object TSRMLS_DC)
{
  HashTable *props = zend_std_get_properties(object TSRMLS_CC);

  return props;
}

static int
php_cassandra_default_session_compare(zval *obj1, zval *obj2 TSRMLS_DC)
{
  if (Z_OBJCE_P(obj1) != Z_OBJCE_P(obj2))
    return 1; /* different classes */

  return Z_OBJ_HANDLE_P(obj1) != Z_OBJ_HANDLE_P(obj1);
}

static void
php_cassandra_default_session_free(php5to7_zend_object_free *object TSRMLS_DC)
{
  cassandra_session *self = PHP5TO7_ZEND_OBJECT_GET(cassandra_session, object);

  php_cassandra_session_destroy(&self->base);

  zend_object_std_dtor(&self->zval TSRMLS_CC);
  PHP5TO7_MAYBE_EFREE(self);
}

static php5to7_zend_object
php_cassandra_default_session_new(zend_class_entry *ce TSRMLS_DC)
{
  cassandra_session *self =
      PHP5TO7_ZEND_OBJECT_ECALLOC(cassandra_session, ce);

  php_cassandra_session_init(&self->base);

  PHP5TO7_ZEND_OBJECT_INIT_EX(cassandra_session, cassandra_default_session, self, ce);
}

void cassandra_define_DefaultSession(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\DefaultSession", cassandra_default_session_methods);
  cassandra_default_session_ce = zend_register_internal_class(&ce TSRMLS_CC);
  zend_class_implements(cassandra_default_session_ce TSRMLS_CC, 1, cassandra_session_ce);
  cassandra_default_session_ce->ce_flags     |= PHP5TO7_ZEND_ACC_FINAL;
  cassandra_default_session_ce->create_object = php_cassandra_default_session_new;

  memcpy(&cassandra_default_session_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_default_session_handlers.get_properties  = php_cassandra_default_session_properties;
  cassandra_default_session_handlers.compare_objects = php_cassandra_default_session_compare;
  cassandra_default_session_handlers.clone_obj = NULL;
}
