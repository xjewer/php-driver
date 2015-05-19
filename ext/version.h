#ifndef PHP_CASSANDRA_VERSION_H
#define PHP_CASSANDRA_VERSION_H

#define STRINGIZE(x) #x
#define STR(x) STRINGIZE(x)
/* Define Extension and Version Properties */
#define PHP_CASSANDRA_NAME "cassandra"
#define PHP_CASSANDRA_MAJOR 1
#define PHP_CASSANDRA_MINOR 0
#define PHP_CASSANDRA_RELEASE 0
#define PHP_CASSANDRA_STABILITY "-beta"
#define PHP_CASSANDRA_VERSION STR(PHP_CASSANDRA_MAJOR) "." STR(PHP_CASSANDRA_MINOR) "." STR(PHP_CASSANDRA_RELEASE) PHP_CASSANDRA_STABILITY

#endif /* PHP_CASSANDRA_VERSION_H */
