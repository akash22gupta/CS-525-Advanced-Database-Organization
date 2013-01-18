#!/bin/bash

BRANCHES=`git branch | grep '^[[:space:]]*m' | sed -e 's/^\( \)*m/m/g'`
MESSAGE=${1}

for branch in ${BRANCHES};
do
    repos=${branch:1};
    echo "*************** ${branch} @ ${repos} *****************"
    git pull "${repos}"
    git checkout "${branch}"
    git merge -m "${MESSAGE}" master
    git push "${repos}"
done