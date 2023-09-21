#!/bin/bash

set -uex

username=$(whoami)
java -DsecretBaseUri=http://libris.kb.se.localhost:5000/ -DsecretSqlUrl=jdbc:postgresql://$username:_XL_PASSWORD_@localhost/whelk_dev -DsecretElasticHost=localhost -DsecretElasticCluster=elasticsearch_$username -DsecretElasticIndex=libris_local -DsecretElasticUser=elastic -DsecretElasticPassword=elastic -DsecretApplicationId=https://libris.kb.se/ -DsecretSystemContextUri=https://id.kb.se/sys/context/kbv -DsecretLocales=sv,en -DsecretTimezone=Europe/Stockholm -jar build/libs/gui-whelktool.jar
