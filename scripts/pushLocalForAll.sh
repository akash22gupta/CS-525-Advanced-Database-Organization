#!/bin/bash

BRANCHES=`git branch | grep '^[[:space:]]*m' | sed -e 's/^\( \)*m/m/g'`
MESSAGE=${1}
CURBRANCH=`git rev-parse --abbrev-ref HEAD`

for branch in ${BRANCHES};
do
    repos=${branch:1};
    echo "*************** ${branch} @ ${repos} *****************"
    git checkout "${branch}"
    git push "${repos}"
done

git checkout "${CURBRANCH}"