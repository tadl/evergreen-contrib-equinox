#!/bin/bash
#!/bin/bash
#    Evergreen WAL archiving script
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
# If WAL file shipping is enabled then the postgres user should be able to
# log into the remote host as the ARCHIVE_USER over ssh and scp with a
# passphraseless ssh key.
#
# In your postgresql.conf, turn on archive_mode (if applicable) and adjust
# the archive_command thusly:
#
#  archive_command = '/location/of/this/script/eg-wal-archive.sh %p %f'
#


#--------------------- CONFIGURATION BEGIN ------------------------
# Remove the following line once you have adjusted the configuration
# below to match your environment.
echo "Configuration not complete!" && exit 1;


# File which, if it exists, pauses WAL archiving
PAUSE_FILE=/tmp/wal-pause

# Local WAL archiving directory
ARCHIVE_DIR="/var/backup/$HOSTNAME/evergreen/database/wal/"


# Remote host (IP or resolvable name) to which WAL files should be shipped.
# Leave empty to disable WAL file shipping.
ARCHIVE_HOST=


# User on the remote WAL-receiving host.
ARCHIVE_USER=

# Snapshot archiving directory on the remote host, if WAL file shipping is
# enabled.
ARCHIVE_DST="/var/backup/$HOSTNAME/evergreen/database/wal/"

#---------------------  CONFIGURATION END  ------------------------




while [ -e $PAUSE_FILE ]; do sleep 1; done

P=$1
F=$2

if [ -e $ARCHIVE_DIR/$F.bz2 ]; then
    echo "Cannot archive: $ARCHIVE_DIR/$F.bz2 already exists"
    logger -p local3.info "Cannot archive: $ARCHIVE_DIR/$F.bz2 already exists"
    exit 0;
fi

cp $P $ARCHIVE_DIR/$F
CP_RES=$?

if [ "$CP_RES" != "0" ]; then
    echo "Cannot archive: unable to copy WAL file $P to $ARCHIVE_DIR/$F, cp exit code = $CP_RES"
    logger -p local3.info "Cannot archive: unable to copy WAL file $P to $ARCHIVE_DIR/$F, cp exit code = $CP_RES"
    exit 1;
fi

/bin/bzip2 $ARCHIVE_DIR/$F
if [ "_$ARCHIVE_HOST" != "_" ]; then
    scp -q $ARCHIVE_DIR/$F.bz2 $ARCHIVE_USER@$ARCHIVE_HOST:$ARCHIVE_DST
fi

exit 0

