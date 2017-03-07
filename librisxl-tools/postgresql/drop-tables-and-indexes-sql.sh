#!/bin/bash
awk '/^CREATE TABLE IF NOT EXISTS/ { print "DROP TABLE IF EXISTS " $6 ";" }' $(dirname $0)/tables.sql
awk '/^CREATE INDEX/ { print "DROP INDEX IF EXISTS " $3 ";" }' $(dirname $0)/indexes.sql
