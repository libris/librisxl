#!/bin/bash

set -uex

username=$(whoami)

java -DsecretBaseUri=http://kblocalhost.kb.se:5000/ -DsecretSqlUrl=jdbc:postgresql://$username:_XL_PASSWORD_@localhost/whelk_dev -DsecretElasticHost=localhost -DsecretElasticCluster=elasticsearch_$username -DsecretElasticIndex=whelk_dev -DsecretApplicationId=https://libris.kb.se/ -DsecretSystemContextUri=https://id.kb.se/sys/context/kbv -DsecretLocales=sv,en -DsecretTimezone=Europe/Stockholm -jar build/libs/gui-whelktool.jar
