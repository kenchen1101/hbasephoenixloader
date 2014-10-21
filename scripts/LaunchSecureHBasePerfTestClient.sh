#!/bin/bash
#######################################################################
#### Loads the entire data into memory before calling batch puts  #####
#### Requires lot's and lot's of memory for huge data set         #####
#######################################################################
# change to the script dir
BASEDIR=$(dirname $0)
if [ ! -z "$BASEDIR" ]; then
    echo "changing current dir to $BASEDIR"
    cd $BASEDIR
fi

# script usage
usage() {
    echo "`basename $0` " 1>&2
    echo "    [-num_hconn        [Default value = 4]  HBase Connection pool size]" 1>&2 
    echo "    [-num_writers      [Default value = 4]  Number of writer threads that will load HBase service concurrently]" 1>&2
    echo "    [-num_xchg         [Default value = 30] Number of exchange, defines row keys and region splits]" 1>&2 
    echo "    [-num_topic        [Default value = 30] Number of Topics, defines row keys and region splits]" 1>&2
    echo "    [-num_keys_per_ET  [Default value = 300] Defines total number of keys when mutipled with num_xchg and num_topic]" 1>&2
    echo "    [-rowkey_data_len  [in characters, Default value = 100] defines data size, rowkey size is 36 char]" 1>&2
    echo "    [-write_batch_size [Default value = 300] number of rows to write in one batch, vary to increase network tput]" 1>&2         
    echo "    [-help display script usage]" 1>&2 
    echo "    [Example: sh LaunchSecureHBaseMaxPerfTestClient.sh --num_hconn 4 --num_writers 4 --num_xchg 30 --num_topic 30 --num_keys_per_ET 300 --rowkey_data_len 100 --write_batch_size 300]" 1>&2    
}

# Set all the environment variables
configEnv() {
    export PERF_CLIENT_HOME=/home/vikuser/vikas/perfdist/
    echo "Setting PERF_CLIENT_HOME = $PERF_CLIENT_HOME"
    export HBASE_HOME=/home/vikuser/current/
    echo "Setting HBASE_HOME = $HBASE_HOME"
    export JAVA_HOME=$HBASE_HOME/bigdata-util/tools/Linux/jdk/jdk1.7.0_67_x64/
    echo "Setting JAVA_HOME = $JAVA_HOME"
    export PATH=$JAVA_HOME/bin:$PATH
    echo "Setting PATH = $PATH"

    #Kerberos
    export KEYTAB_HOME=/home/vikuser/.keytab/
    echo "Setting KEYTAB_HOME = $KEYTAB_HOME"
    export KRB5_CONFIG=$KEYTAB_HOME/krb5.conf
    echo "Setting KRB5_CONFIG = $KRB5_CONFIG"
    #kinit -k -t $KEYTAB_HOME/hbase.keytab hbase/principal-hostname@HYDDR1.SFDC.NET

    mkdir -p $PERF_CLIENT_HOME/logs
    echo "Creating dir $PERF_CLIENT_HOME/logs"
    mkdir -p $PERF_CLIENT_HOME/sgdata
    echo "Creating dir $PERF_CLIENT_HOME/sgdata"

    echo "Changing current dir to $PERF_CLIENT_HOME"
    cd $PERF_CLIENT_HOME
    
}

# Start the java client loader
executeLoader() {
    #enable kerberos debugging
    #export HBASE_OPTS="$HBASE_OPTS -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$PERF_CLIENT_HOME/logs/perftestclient.gc.log -Djava.security.auth.login.config=$HBASE_HOME/bigdata-hbase/hbase/hbase//conf/zk-jaas.conf -Djava.security.krb5.conf=$KEYTAB_HOME/krb5.conf -Dsun.security.krb5.debug=true -Djavax.net.debug=all"
    export HBASE_OPTS="$HBASE_OPTS -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$PERF_CLIENT_HOME/logs/perftestclient.gc.log -Djava.security.auth.login.config=$HBASE_HOME/bigdata-hbase/hbase/hbase//conf/zk-jaas.conf -Djava.security.krb5.conf=$KEYTAB_HOME/krb5.conf"
    echo "Executing the HBase Perf Client $JAVA_HOME/bin/java -Xmx20G $HBASE_OPTS -classpath .:$PERF_CLIENT_HOME/lib/*:$PERF_CLIENT_HOME/config/*:$HBASE_HOME/bigdata-hbase/hbase/hbase/conf/hbase-site.xml:$HBASE_HOME/bigdata-hbase/hbase/hbase/conf/:$HBASE_HOME/bigdata-hadoop/hadoop/hadoop/etc/hadoop/:$HBASE_HOME/bigdata-hadoop/hadoop/hadoop/share/hadoop/common/*:$HBASE_HOME/bigdata-hadoop/hadoop/hadoop/share/hadoop/common/lib/*:$HBASE_HOME/bigdata-hadoop/hadoop/hadoop/share/hadoop/hdfs/*:$HBASE_HOME/bigdata-hbase/hbase/hbase/lib/*:$HBASE_HOME/bigdata-hbase/hbase/hbase/*:$PERF_CLIENT_HOME:$PERF_CLIENT_HOME/HBasePhoenixLoader.jar:$PERF_CLIENT_HOME/config/HBaseTest.properties:$PERF_CLIENT_HOME/config/HdfsTest.properties:$PERF_CLIENT_HOME/config/CommonTest.properties com.vikkarma.hbase.loader.HBasePerfTestClient"
    $JAVA_HOME/bin/java -Xmx20G $HBASE_OPTS -classpath .:$PERF_CLIENT_HOME/lib/*:$PERF_CLIENT_HOME/config/*:$HBASE_HOME/bigdata-hbase/hbase/hbase/conf/hbase-site.xml:$HBASE_HOME/bigdata-hbase/hbase/hbase/conf/:$HBASE_HOME/bigdata-hadoop/hadoop/hadoop/etc/hadoop/:$HBASE_HOME/bigdata-hadoop/hadoop/hadoop/share/hadoop/common/*:$HBASE_HOME/bigdata-hadoop/hadoop/hadoop/share/hadoop/common/lib/*:$HBASE_HOME/bigdata-hadoop/hadoop/hadoop/share/hadoop/hdfs/*:$HBASE_HOME/bigdata-hbase/hbase/hbase/lib/*:$HBASE_HOME/bigdata-hbase/hbase/hbase/*:$PERF_CLIENT_HOME:$PERF_CLIENT_HOME/HBasePhoenixLoader.jar:$PERF_CLIENT_HOME/config/HBaseTest.properties:$PERF_CLIENT_HOME/config/HdfsTest.properties:$PERF_CLIENT_HOME/config/CommonTest.properties com.vikkarma.hbase.loader.HBasePerfTestClient "$@"

}

echo "====================================="
echo "Configuring tests env"
echo "====================================="
configEnv
echo "====================================="
echo "Executing perf client loader"
echo "====================================="
executeLoader "$@"
