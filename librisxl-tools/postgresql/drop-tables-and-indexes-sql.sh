#!/bin/bash
awk '/^CREATE TABLE IF NOT EXISTS/ { print "DROP TABLE " $6 ";" }' $(dirname $0)/tables.sql
awk '/^CREATE INDEX/ { print "DROP INDEX " $3 ";" }' $(dirname $0)/indexes.sql
