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

# Warden-specific
MAPR_CONF_DIR=${MAPR_CONF_DIR:-"$MAPR_HOME/conf"}


function change_permissions() {
    chown -R ${MAPR_USER} ${HBASE_HOME}
    chgrp -R ${MAPR_GROUP} ${HBASE_HOME}
    chmod 644 ${HBASE_SITE}
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

function add_comment(){
  line_name=$1
      sed -i -e "s|</configuration>| \n <!--${line_name}-->\n</configuration>|" ${HBASE_SITE}
}

function remove_comment(){
  line_name=$1
  sed  -i  "/<!--${line_name}-->/d" ${HBASE_SITE}
}

function configure_thrift_impersonation() {
  if ! grep -q hbase.regionserver.thrift.http "$HBASE_SITE" ; then
    add_comment "Enabling Hbase thrift impersonation"
  fi
  add_property hbase.regionserver.thrift.http true
  add_property hbase.thrift.support.proxyuser true
}

function configure_thrift_authentication() {
  if [ "$MAPR_SECURITY_STATUS" = "true" ]; then
    if ! grep -q hbase.thrift.security.authentication "$HBASE_SITE" ; then
        add_comment "Enabling Hbase thrift authentication"
    fi
    add_property hbase.thrift.security.authentication maprsasl
    if [[  $MAPR_HBASE_SERVER_OPTS != *"Dhadoop.login=maprsasl_keytab" ]] ; then
      echo "export MAPR_HBASE_SERVER_OPTS=\"\${MAPR_HBASE_SERVER_OPTS} -Dhadoop.login=maprsasl_keytab\"" >> $env
    fi
  fi
}

function configure_thrift_encryption() {
  if ! grep -q hbase.thrift.ssl.enabled "$HBASE_SITE" ; then
    add_comment "Enabling Hbase thrift encryption"
  fi
  add_property hbase.thrift.ssl.enabled true
}

function configure_rest_authentication() {
  if ! grep -q hbase.rest.authentication.type "$HBASE_SITE" ; then
    add_comment "Enabling Hbase REST authentication"
  fi
  add_property hbase.rest.authentication.type org.apache.hadoop.security.authentication.server.MultiMechsAuthenticationHandler
}

function configure_rest_encryption() {
  if ! grep -q hbase.rest.ssl.enabled "$HBASE_SITE" ; then
    add_comment "Enabling Hbase REST encryption"
  fi
}

function configure_rest_impersonation() {
  if ! grep -q hbase.rest.support.proxyuser "$HBASE_SITE" ; then
    add_comment "Enabling Hbase REST impersonation"
  fi
  add_property hbase.rest.support.proxyuser true
}

function configure_thrift_unsecure(){
  #disable encryption
  remove_comment "Enabling Hbase thrift encryption"
  remove_property hbase.thrift.ssl.enabled
  remove_property hbase.thrift.ssl.keystore.store

  #disable authentication
  remove_comment "Enabling Hbase thrift authentication"
  remove_property hbase.thrift.security.authentication
  sed -i "/\b\(MAPR_HBASE_SERVER_OPTS.\|maprsasl_keytab\)\b/d" $env
}

function configure_rest_unsecure() {
  remove_comment "Enabling Hbase REST encryption"

  #disable encryption
  remove_property hbase.rest.ssl.enabled
  remove_property hbase.rest.ssl.keystore.store
}

#
# Add warden files
#
function copyWardenFile() {
	if [ -f $HBASE_HOME/conf/warden.${1}.conf ] ; then
		cp "${HBASE_HOME}/conf/warden.${1}.conf" "${MAPR_CONF_DIR}/conf.d/" 2>/dev/null || :
		sed -i s/\${VERSION}/`cat ${HBASE_HOME}/../hbaseversion`/g ${MAPR_CONF_DIR}/conf.d/warden.${1}.conf
	fi
}

function copyWardenConfFiles() {
	mkdir -p "$MAPR_HOME"/conf/conf.d
	copyWardenFile hbasethrift
	copyWardenFile hbaserest
}

function stopService() {
	if [ -e ${MAPR_CONF_DIR}/conf.d/warden.${1}.conf ]; then
		logInfo "Stopping hbase-$1..."
		${HBASE_HOME}/bin/hbase-daemon.sh stop ${2}
	fi
}

function stopServicesForRestartByWarden() {
	stopService hbase-rest rest
	stopService hbase-thrift thrift
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

if $SECURE; then
	if [ -f $HBASE_HOME/conf/warden.hbasethrift.conf ] ; then
    	configure_thrift_authentication
    	configure_thrift_encryption
    	configure_thrift_impersonation
   	fi
	if [ -f $HBASE_HOME/conf/warden.hbaserest.conf ] ; then
    	configure_rest_authentication
    	configure_rest_encryption
    	configure_rest_impersonation
   	fi
else
    if $CUSTOM; then
      exit 0
    fi
		if [ -f $HBASE_HOME/conf/warden.hbasethrift.conf ] ; then
			configure_thrift_unsecure
   		fi
		if [ -f $HBASE_HOME/conf/warden.hbaserest.conf ] ; then
	    	configure_rest_unsecure
	   	fi
fi

change_permissions
copyWardenConfFiles
stopServicesForRestartByWarden

if [ -f "$HBASE_HOME/conf/.not_configured_yet" ]  ; then
    rm -f "$HBASE_HOME/conf/.not_configured_yet"
fi

exit 0