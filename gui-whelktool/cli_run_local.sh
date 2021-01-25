#!/bin/bash

set -uex

username=$(whoami)

java -DsecretBaseUri=http://kblocalhost.kb.se:5000/ -DsecretSqlUrl=jdbc:postgresql://$username:_XL_PASSWORD_@localhost/whelk_dev -DsecretElasticHost=localhost -DsecretElasticCluster=elasticsearch_$username -DsecretElasticIndex=whelk_dev -jar build/libs/gui-whelktool.jar
