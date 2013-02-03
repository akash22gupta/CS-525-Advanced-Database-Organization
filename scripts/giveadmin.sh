#! /bin/sh

# usage: xargs -n1 ./bbrepos.sh < roster.txt
#        (make sure that roster.txt is in UTF-8, Unix linefeeds,
#         with one student per line!)

# my bitbucket credentials
bb_user=bglavic
bb_pass=BbiagI

# repository name to create (on bitbucket)
repos="cs525-s13-$1"

# student e-mail suffix
esuff='@hawk.iit.edu'

################################################################################

pushd "${baserepos}"

# find users for repository
users=`curl -s --user bglavic:BbiagI -X GET "https://api.bitbucket.org/1.0/privileges/bglavic/${repos}" | grep '\"username\": \"' | grep -v 'bglavic' | grep -v 'dma' | sed -e 's/\"username\": \"\([^\"]*\)\".*/\1/g' -e 's/ *//g' | sort | uniq`
echo "users for repos ${repos} are \"${users}\""

# push baserepos to new bitbucket repos
for user in ${users}
do
    echo "grant user ${user} admin permission"
    curl -u ${bb_user}:${bb_pass} -X PUT "https://api.bitbucket.org/1.0/privileges/${bb_user}/${repos}/${user}" --data admin
done

popd
