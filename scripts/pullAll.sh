#!/bin/bash

BRANCHES=`git branch | grep '^[[:space:]]*m' | sed -e 's/^\( \)*m/m/g'`
MESSAGE=${1}
CURBRANCH=`git rev-parse --abbrev-ref HEAD`

for branch in ${BRANCHES};
do
    repos=${branch:1};
    echo "*************** pull ${branch} @ ${repos} *****************"
    git pull "${repos}" master
done