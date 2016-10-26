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

#include <php_ini.h>
#include <ext/standard/info.h>

#if CURRENT_CPP_DRIVER_VERSION < CPP_DRIVER_VERSION(2, 3, 0)
#error C/C++ driver version 2.3.0 or greater required
#endif

static PHP_GINIT_FUNCTION(cassandra);
static PHP_GSHUTDOWN_FUNCTION(cassandra);

const zend_function_entry cassandra_functions[] = {
  PHP_FE_END /* Must be the last line in cassandra_functions[] */
};

#if ZEND_MODULE_API_NO >= 20050617
static zend_module_dep php_cassandra_deps[] = {
  ZEND_MOD_REQUIRED("spl")
  ZEND_MOD_END
};
#endif

zend_module_entry cassandra_module_entry = {
#if ZEND_MODULE_API_NO >= 20050617
  STANDARD_MODULE_HEADER_EX, NULL, php_cassandra_deps,
#elif ZEND_MODULE_API_NO >= 20010901
  STANDARD_MODULE_HEADER,
#endif
  PHP_CASSANDRA_NAME,
  cassandra_functions,      /* Functions */
  PHP_MINIT(cassandra),     /* MINIT */
  PHP_MSHUTDOWN(cassandra), /* MSHUTDOWN */
  PHP_RINIT(cassandra),     /* RINIT */
  PHP_RSHUTDOWN(cassandra), /* RSHUTDOWN */
  PHP_MINFO(cassandra),     /* MINFO */
#if ZEND_MODULE_API_NO >= 20010901
  PHP_CASSANDRA_VERSION,
#endif
  PHP_MODULE_GLOBALS(cassandra),
  PHP_GINIT(cassandra),
  PHP_GSHUTDOWN(cassandra),
  NULL,
  STANDARD_MODULE_PROPERTIES_EX
};

#ifdef COMPILE_DL_CASSANDRA
ZEND_GET_MODULE(cassandra)
#endif

PHP_INI_BEGIN()
  PHP_CASSANDRA_INI_ENTRY_LOG
  PHP_CASSANDRA_INI_ENTRY_LOG_LEVEL
PHP_INI_END()

static PHP_GINIT_FUNCTION(cassandra)
{
  php_cassandra_ginit();
}

static PHP_GSHUTDOWN_FUNCTION(cassandra)
{
  php_cassandra_gshutdown();
}

PHP_MINIT_FUNCTION(cassandra)
{
  REGISTER_INI_ENTRIES();

  return php_cassandra_minit(INIT_FUNC_ARGS_PASSTHRU);
}

PHP_MSHUTDOWN_FUNCTION(cassandra)
{
  /* UNREGISTER_INI_ENTRIES(); */

  return php_cassandra_mshutdown(SHUTDOWN_FUNC_ARGS_PASSTHRU);
}

PHP_RINIT_FUNCTION(cassandra)
{
  return php_cassandra_rinit(INIT_FUNC_ARGS_PASSTHRU);
}

PHP_RSHUTDOWN_FUNCTION(cassandra)
{
  return php_cassandra_rshutdown(SHUTDOWN_FUNC_ARGS_PASSTHRU);
}

PHP_MINFO_FUNCTION(cassandra)
{
  char buf[256];
  php_info_print_table_start();
  php_info_print_table_header(2, "Cassandra support", "enabled");

  snprintf(buf, sizeof(buf), "%d.%d.%d%s",
           CASS_VERSION_MAJOR, CASS_VERSION_MINOR, CASS_VERSION_PATCH,
           strlen(CASS_VERSION_SUFFIX) > 0 ? "-" CASS_VERSION_SUFFIX : "");
  php_info_print_table_row(2, "C/C++ driver version", buf);

  snprintf(buf, sizeof(buf), "%d", CASSANDRA_G(persistent_clusters));
  php_info_print_table_row(2, "Persistent Clusters", buf);

  snprintf(buf, sizeof(buf), "%d", CASSANDRA_G(persistent_sessions));
  php_info_print_table_row(2, "Persistent Sessions", buf);

  php_info_print_table_end();

  DISPLAY_INI_ENTRIES();
}
