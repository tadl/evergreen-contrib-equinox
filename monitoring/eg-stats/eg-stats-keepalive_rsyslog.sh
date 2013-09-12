count=`ps ax|grep eg-stats-collector-remote-log.pl|grep -v grep| wc -l`
if [ $count -lt 1 ] ; then
        /usr/bin/eg-stats-collector-remote-log.pl &
fi
