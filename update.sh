#!/bin/sh -x
today=`date +%Y-%m-%d`
start_dir=`pwd`

for branch in beta aurora nightly
do 
    if [ $branch  == 'nightly' ]
        then 
            cd ~/mozilla-central
        else
            cd ~/mozilla-$branch
    fi
    lastday=`cat last-metrics.txt`
    echo $lastday
    version=`awk '{split($0,a,"."); print  a[1]}' ./browser/config/version.txt`
    echo $today >last-metrics.txt
    hg pull -u
    hg metrics -f ~/hg-metrics/$branch-$version.json -d "$lastday to $today"
    echo $branch-$version `pwd` > $start_dir/branchlist
done

cd $start_dir
/usr/local/bin/python $start_dir/parse_data.py -b $start_dir/branchlist

