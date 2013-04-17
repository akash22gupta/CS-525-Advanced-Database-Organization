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
baserepos='/Users/lord_pretzel/Documents/IIT/Teaching/CS525 - Advanced Database Organisation/2013-spring/CodeTemplate/'

# repository name to create (on bitbucket)
repos="cs525-s13-$1"

# student e-mail suffix
esuff='@hawk.iit.edu'

################################################################################

pushd "${baserepos}"

# create bitbucket repos
echo "create repos for \"${1}\":"

# invite student to collaborate via e-mail
echo "invite student $1"
curl -u $bb_user:$bb_pass -X POST "https://api.bitbucket.org/1.0/invitations/$bb_user/$repos/$1$esuff" -d permission=admin

popd
