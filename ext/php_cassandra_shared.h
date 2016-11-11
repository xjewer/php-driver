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

#ifndef PHP_CASSANDRA_SHARED_H
#define PHP_CASSANDRA_SHARED_H

#include "php_driver.h"
#include "php_cassandra_types.h"


zend_class_entry *exception_class(CassError rc);

void throw_invalid_argument(zval *object,
                            const char *object_name,
                            const char *expected_type TSRMLS_DC);

#define INVALID_ARGUMENT(object, expected) \
{ \
  throw_invalid_argument(object, #object, expected TSRMLS_CC); \
  return; \
}

#define INVALID_ARGUMENT_VALUE(object, expected, failed_value) \
{ \
  throw_invalid_argument(object, #object, expected TSRMLS_CC); \
  return failed_value; \
}

#define ASSERT_SUCCESS_BLOCK(rc, block) \
{ \
  if (rc != CASS_OK) { \
    zend_throw_exception_ex(exception_class(rc), rc TSRMLS_CC, \
                            "%s", cass_error_desc(rc)); \
    block \
  } \
}

#define ASSERT_SUCCESS(rc) ASSERT_SUCCESS_BLOCK(rc, return;)

#define ASSERT_SUCCESS_VALUE(rc, value) ASSERT_SUCCESS_BLOCK(rc, return value;)

#define PHP_CASSANDRA_DEFAULT_CONSISTENCY CASS_CONSISTENCY_LOCAL_ONE

#define PHP_CASSANDRA_DEFAULT_LOG       "cassandra.log"
#define PHP_CASSANDRA_DEFAULT_LOG_LEVEL "ERROR"

#define PHP_CASSANDRA_INI_ENTRY_LOG \
  PHP_INI_ENTRY("cassandra.log", PHP_CASSANDRA_DEFAULT_LOG, PHP_INI_ALL, OnUpdateLog)

#define PHP_CASSANDRA_INI_ENTRY_LOG_LEVEL \
  PHP_INI_ENTRY("cassandra.log_level", PHP_CASSANDRA_DEFAULT_LOG_LEVEL, PHP_INI_ALL, OnUpdateLogLevel)

PHP_INI_MH(OnUpdateLogLevel);
PHP_INI_MH(OnUpdateLog);

void php_cassandra_ginit(TSRMLS_D);
void php_cassandra_gshutdown(TSRMLS_D);

int php_cassandra_minit(INIT_FUNC_ARGS);
int php_cassandra_mshutdown(SHUTDOWN_FUNC_ARGS);

int php_cassandra_rinit(INIT_FUNC_ARGS);
int php_cassandra_rshutdown(SHUTDOWN_FUNC_ARGS);

#endif /* PHP_CASSANDRA_SHARED_H */
