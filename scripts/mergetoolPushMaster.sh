#!/bin/bash

BRANCHES=`git branch | grep '^[[:space:]]*m' | sed -e 's/^\( \)*m/m/g'`
MESSAGE=${1}
CURBRANCH=`git rev-parse --abbrev-ref HEAD`

for branch in ${BRANCHES};
do
    repos=${branch:1};
    echo "*************** ${branch} @ ${repos} *****************"
    git checkout "${branch}"
    git pull "${repos}" master
    git merge -m "${MESSAGE}" master
    git mergetool --tool=keepremote -y
    find . -name \*.orig -exec rm {} \;
    git commit -m "${1}"
    git push "${repos}"
done

git checkout "${CURBRANCH}"