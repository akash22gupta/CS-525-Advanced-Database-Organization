#!/bin/bash

BRANCH=${1}
REMOTE=${BRANCH:1}

git checkout ${BRANCH}
git pull ${REMOTE} ${BRANCH}