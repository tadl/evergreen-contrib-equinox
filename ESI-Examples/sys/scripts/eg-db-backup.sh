#!/bin/bash
#    Evergreen database snapshot creation and archiving script
#    Copyright (C) 2008-2010 Equinox Software Inc.
#    Mike Rylander <mrylander@gmail.com>
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
#
# You will need to edit the variables below to configure this script for
# use in your production environment.
#
# This script must be run as the postgres user, and if snapshot shipping
# is enabled then the postgres user should be able to log into the remote
# host as the ARCHIVE_USER over ssh and scp with a passphraseless ssh key.
#


#--------------------- CONFIGURATION BEGIN ------------------------
# Remove the following line once you have adjusted the configuration
# below to match your environment.
echo "Configuration not complete!" && exit 1;


# Where the postgres binaries are installed, particularly psql
PGBIN=/usr/local/bin/


# Where the database cluster lives
PGDATA=/usr/local/pgsql/data;


# How to name the database snapshot files. Adjust to taste.
ARCHIVE_LABEL=`date +MyEvergreen-production-postgres-backup-%FT%T`


# Local snapshot archiving directory
ARCHIVE_DIR="/var/backup/$HOSTNAME/evergreen/database/snapshot/"


# Remote host (IP or resolvable name) to which snapshots should be shipped.
# Leave empty to disable snapshot shipping.
ARCHIVE_HOST=


# User on the remote snapshot-receiving host.
ARCHIVE_USER=

# Snapshot archiving directory on the remote host, if snapshot shipping is
# enabled.
ARCHIVE_DST="/var/backup/$HOSTNAME/evergreen/database/snapshot/"

#---------------------  CONFIGURATION END  ------------------------




ARCHIVE_FILE=$ARCHIVE_LABEL.cpio.gz
# Make sure we're not overwriting an existing backup
if [ -e $ARCHIVE_DIR/$ARCHIVE_FILE ]; then
        echo "Cannot create backup: $ARCHIVE_DIR/$ARCHIVE_FILE exists";
        exit;
fi


# Tell PG we're starting the backup
START_RESULT=`$PGBIN/psql -tc "SELECT pg_start_backup('$ARCHIVE_LABEL') IS NOT NULL;"|grep t`
if [ "_" == "_$START_RESULT" ]; then
        echo "Could not start backup labeled $ARCHIVE_LABEL";
        exit;
fi


# Grab the data we need (just copy it locally) ...
(cd $PGDATA && find . -depth -print | grep -v pg_xlog | cpio -o | gzip > $ARCHIVE_DIR/$ARCHIVE_FILE)


# ... tell PG we're done ...
STOP_RESULT=`$PGBIN/psql -tc "SELECT pg_stop_backup() IS NOT NULL;"|grep t`
if [ "_" == "_$STOP_RESULT" ]; then
        echo "Could not stop backup labeled $ARCHIVE_LABEL";
        exit;
fi

echo "Backup of database on $HOSTNAME complete. Archive label: $ARCHIVE_LABEL"

if [ "_$ARCHIVE_HOST" != "_" ]; then
	# ... then push it over to the backup host
	scp -q $ARCHIVE_DIR/$ARCHIVE_FILE $ARCHIVE_USER@$ARCHIVE_HOST:$ARCHIVE_DST
	SCP_RES=$?
	if [ "$SCP_RES" != "0" ]; then
	        echo "Unable to archive $ARCHIVE_DIR/$ARCHIVE_FILE to $ARCHIVE_USER@$ARCHIVE_HOST:$ARCHIVE_DST!!"
	        exit;
	fi
    echo "Remote backup: $ARCHIVE_HOST:$ARCHIVE_DST"
fi

