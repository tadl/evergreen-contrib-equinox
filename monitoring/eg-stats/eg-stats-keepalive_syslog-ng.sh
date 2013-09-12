count=`ps ax|grep eg-stats-collector-remote-log.pl|grep -v grep| wc -l`
if [ $count -lt 1 ] ; then
	/usr/bin/eg-stats-collector-remote-log.pl --service=open-ils.acq,open-ils.auth,open-ils.search,open-ils.actor,open-ils.booking,open-ils.cat,open-ils.supercat,open-ils.trigger,opensrf.math,opensrf.dbmath,open-ils.penalty,open-ils.circ,open-ils.ingest,open-ils.storage,open-ils.cstore,open-ils.pcrud,opensrf.settings,open-ils.collections,open-ils.reporter,open-ils.reporter-store,open-ils.permacrud,open-ils.fielder,open-ils.vandelay &
	sleep 1
	/etc/init.d/syslog-ng restart
fi
