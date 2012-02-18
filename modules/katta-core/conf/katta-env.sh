# Set Katta-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
# export JAVA_HOME=/usr/lib/j2sdk1.5-sun

# Extra Java CLASSPATH elements.  Optional.
# export KATTA_CLASSPATH=

# The maximum amount of heap to use, in MB. Default is 1000.
# export KATTA_HEAPSIZE=2000

# Extra Java runtime options.  Empty by default.
# export KATTA_OPTS=-server

# Extra ssh options.  Empty by default.
# export KATTA_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=KATTA_CONF_DIR"

# Where log files are stored.  $KATTA_HOME/logs by default.
# export KATTA_LOG_DIR=${KATTA_HOME}/logs

# File naming remote node hosts.  $KATTA_HOME/conf/nodes by default.
# export KATTA_NODES=${KATTA_HOME}/conf/nodes

# host:path where katta code should be rsync'd from.  Unset by default.
# export KATTA_MASTER=master:/home/$USER/src/katta-version

# Seconds to sleep between node commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., node rsyncs can
# otherwise arrive faster than the master can service them.
# export KATTA_NODE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
# export KATTA_PID_DIR=/var/katta/pids

# A string representing this instance of katta. $USER by default.
# export KATTA_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export KATTA_NICENESS=10

# The level of logging. Possible values are Debug, Info, Error, Warn and None.
export KATTA_LOG_LEVEL=Debug
