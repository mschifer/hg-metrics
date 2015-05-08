#!/bin/sh -x
today=`date +%Y-%m-%d`
cd /Users/mschifer
start_dir=`pwd`

cat /dev/null > $start_dir/hg-metrics/branchlist
for branch in beta aurora nightly release
do 
    if [ $branch  == 'nightly' ]
        then 
            wdir=$start_dir/mozilla-central
        else
            wdir=$start_dir/mozilla-$branch
    fi
    echo Updating $branch
    cd $wdir
    echo CURRENT DIR IS $wdir
    echo `pwd`
    lastday=`cat $widr\last-metrics.txt`
    echo $lastday
    version=`awk '{split($0,a,"."); print  a[1]}' $wdir/browser/config/version.txt`
    echo $today >$wdir/last-metrics.txt
    /usr/local/bin/hg pull -u
    /usr/local/bin/hg metrics -f $start_dir/hg-metrics/$branch-$version.json -d "$lastday to $today"
    echo $branch-$version `pwd` >> $start_dir/hg-metrics/branchlist
done

cd $start_dir/hg-metrics
/usr/local/bin/python $start_dir/hg-metrics/parse_data.py -b $start_dir/hg-metrics/branchlist

