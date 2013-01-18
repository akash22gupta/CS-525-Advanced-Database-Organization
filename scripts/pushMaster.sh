#!/bin/bash

BRANCH=${1}
MESSAGE=${2}

repos=${BRANCH:1};
echo "*************** ${BRANCH} @ ${repos} *****************"
git pull "${repos}"
git checkout "${BRANCH}"
git merge -m "${MESSAGE}" master
git push "${repos}"
