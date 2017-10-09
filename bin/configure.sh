#!/usr/bin/env bash

MAPR_HOME=${MAPR_HOME:-/opt/mapr}

. ${MAPR_HOME}/server/common-ecosystem.sh 2> /dev/null # prevent verbose output, set by 'set -x'
if [ $? -ne 0 ]; then
  echo 'Error: Seems that MAPR_HOME is not correctly set or mapr-core is not installed.'
  exit 1
fi 2> /dev/null
{ set +x; } 2>/dev/null

initCfgEnv

env="$MAPR_HOME"/conf/env.sh
[ -f $env ] && . $env

# general
HBASE_VERSION_FILE="$MAPR_HOME"/hbase/hbaseversion
HBASE_VERSION=$(cat "$HBASE_VERSION_FILE")
HBASE_HOME="$MAPR_HOME"/hbase/hbase-"$HBASE_VERSION"
HBASE_SITE=${HBASE_HOME}/conf/hbase-site.xml


function change_permissions() {
    chown -R ${MAPR_USER} ${HBASE_HOME}
    chgrp -R ${MAPR_GROUP} ${HBASE_HOME}
    chmod 600 ${HBASE_SITE}
    chmod u+x ${HBASE_HOME}/bin/*
}

function remove_property() {
  property_name=$1
  sed -i "/<property>/,/<\/property>/!b;/<property>/{h;d};H;/<\/property/!d;x;/<name>${property_name}.*<\/name>/d" ${HBASE_SITE}
}

function add_property() {
  property_name=$1
  property_value=$2
  if ! grep -q $property_name "$HBASE_SITE" ; then
    sed -i -e "s|</configuration>|  <property>\n    <name>${property_name}</name>\n    <value>${property_value}</value>\n  </property>\n</configuration>|" ${HBASE_SITE}
  fi
}

function configure_thrift_impersonation() {
  add_property hbase.regionserver.thrift.http true
  add_property hbase.thrift.support.proxyuser true
}

function configure_thrift_authentication() {
  if [ "$MAPR_SECURITY_STATUS" = "true" ]; then
    add_property hbase.thrift.security.authentication maprsasl
    if [[  $MAPR_HBASE_SERVER_OPTS != *"Dhadoop.login=maprsasl_keytab" ]] ; then
      echo "export MAPR_HBASE_SERVER_OPTS=\"\${MAPR_HBASE_SERVER_OPTS} -Dhadoop.login=maprsasl_keytab\"" >> $env
    fi
  fi
}

function configure_thrift_encryption() {
  add_property hbase.thrift.ssl.enabled true
  add_property hbase.thrift.ssl.keystore.store "$MAPR_HOME"/conf/ssl_keystore
  add_property hbase.thrift.ssl.keystore.password mapr123
  add_property hbase.thrift.ssl.keystore.keypassword mapr123
}

function configure_rest_authentication() {
  add_property hbase.rest.authentication.type org.apache.hadoop.security.authentication.server.MultiMechsAuthenticationHandler
}

function configure_rest_encryption() { 
  add_property hbase.rest.ssl.enabled true
  add_property hbase.rest.ssl.keystore.store "$MAPR_HOME"/conf/ssl_keystore
  add_property hbase.rest.ssl.keystore.password mapr123
  add_property hbase.rest.ssl.keystore.keypassword mapr123
}

function configure_thrift_unsecure(){
  #disable encryption
  remove_property hbase.thrift.ssl.enabled
  remove_property hbase.thrift.ssl.keystore.store
  remove_property hbase.thrift.ssl.keystore.password
  remove_property hbase.thrift.ssl.keystore.keypassword

  #disable authentification
  remove_property hbase.thrift.security.authentication
  sed -i "/\b\(MAPR_HBASE_SERVER_OPTS.\|maprsasl_keytab\)\b/d" $env
} 

function configure_rest_unsecure() {
  #disable encryption
  remove_property hbase.rest.ssl.enabled
  remove_property hbase.rest.ssl.keystore.store
  remove_property hbase.rest.ssl.keystore.password
  remove_property hbase.rest.ssl.keystore.keypassword
}


{ OPTS=`getopt -n "$0" -a -o suhR --long secure,unsecure,help,EC -- "$@"`; } 2>/dev/null
eval set -- "$OPTS"

SECURE=false
CUSTOM=false
HELP=false
while true; do
  case "$1" in
    -s | --secure )
    SECURE=true;
    shift ;;

    -u | --unsecure )
    SECURE=false;
    shift ;;

    -cs | --customSecure)  
      if [ -f "$HBASE_HOME/conf/.not_configured_yet" ]; then
        SECURE=true;
      else
        SECURE=false;
        CUSTOM=true;
      fi
    shift ;;
    
    -h | --help ) HELP=true; shift ;;

    -R)
     shift ;;

    --EC)
     # ignoring
     shift ;;

    -- ) shift; break ;;

    * ) break ;;
  esac
done

if [ -f "$HBASE_HOME/conf/.not_configured_yet" ]  ; then
    rm -f "$HBASE_HOME/conf/.not_configured_yet"
fi

change_permissions

if $SECURE; then
    configure_thrift_authentication
    configure_thrift_encryption
    configure_thrift_impersonation
    configure_rest_authentication
    configure_rest_encryption
else
    if $CUSTOM; then
      exit 0
    fi
    configure_rest_unsecure
    configure_thrift_unsecure
fi

exit 0