#! /bin/sh

# usage: xargs -n1 ./bbrepos.sh < roster.txt
#        (make sure that roster.txt is in UTF-8, Unix linefeeds,
#         with one student per line!)

# my bitbucket credentials
bb_user=bglavic
bb_pass=BbiagI

# TA's bitbucket username & e-mail
ta_user='dma2'
ta_email='dma2@hawk.iit.edu'

# base directory for shared (template) code
baserepos='/Users/lord_pretzel/Documents/IIT/Teaching/CS525 - Advanced Database Organisation/CodeTemplate/'

# repository name to create (on bitbucket)
repos="cs525-s13-$1"

# student e-mail suffix
esuff='@hawk.iit.edu'

################################################################################

pushd "${baserepos}"


# create bitbucket repos
echo "create repos for \"${1}\":"
echo "curl -u ${bb_user}:${bb_pass} -X POST \"https://api.bitbucket.org/1.0/repositories/\" -d name=${repos} -d scm=git -d is_private=True"
curl -u ${bb_user}:${bb_pass} -X POST "https://api.bitbucket.org/1.0/repositories/" -d name=${repos} -d scm=git -d is_private=True

# push baserepos to new bitbucket repos
echo "push original repository"
git remote add ${1} git@bitbucket.org:$bb_user/$repos.git
git push ${1} master
git fetch ${1}
git checkout -b m${1} ${1}/master 
git config remote.${1}.push m${1}:master
git config branch.m${1}.remote ${1}
git checkout master

# invite student to collaborate via e-mail
echo "invite student"
curl -u $bb_user:$bb_pass -X POST "https://api.bitbucket.org/1.0/invitations/$bb_user/$repos/$1$esuff" -d permission=write

# grant permissions to TA to write (must already have bb account)
echo "grant TA write permission"
curl -u $bb_user:$bb_pass -X PUT "https://api.bitbucket.org/1.0/privileges/$bb_user/$repos/$ta_user" --data write

popd
