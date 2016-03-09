#!/bin/bash
awk '/^CREATE INDEX/ { print "DROP INDEX " $3 ";" }' $(dirname $0)/indexes.sql
