#!/bin/bash

LIBRARYNAME="LIBRARYCODE" # library code assigned by collectionHQ
DATE=`date +%Y%m%d`
FILE="$LIBRARYNAME""$DATE".DAT
FTPUSER="user"
FTPPASS="passwd"
FTPSITE="ftp.collectionhq.com"
EMAILFROM="you@example.org"
EMAILTO="thee@example.org"

function get_data_from_sql {
  echo The extract for $DATE has begun. | ./send-email.pl --from "$EMAILFROM" --to "$EMAILTO" --subject "collectionHQ extraction has begun"
  date
  echo Fetching bibs...
  psql -A -t -U evergreen < get_bibs.sql 2>&1 | cut -c8- | perl -ne 'if (m/^[0-9]/) { print STDERR; } else { print; }' > bibs-$DATE.txt
  date
  echo Fetching items...
  psql -A -t -U evergreen < get_items.sql 2>&1 | cut -c8- | perl -ne 'if (m/^[0-9]/) { print STDERR; } else { print; }' > items-$DATE.txt
  date
  echo done.
}

function format_data {
  echo "##HEADER##,##DAT##,##${DATE}##,##${LIBRARYNAME}##,,,##USA##" > $FILE
  cat bibs-$DATE.txt >> $FILE
  cat items-$DATE.txt >> $FILE
  NUMBIBS=`wc -l bibs-$DATE.txt | cut -d' ' -f1`
  NUMITEMS=`wc -l items-$DATE.txt | cut -d' ' -f1`
  echo "##TRAILER##,$NUMBIBS,$NUMITEMS" >> $FILE
}

function upload_data {
  gzip --best $FILE
  ftp -v -n $FTPSITE <<END_SCRIPT
quote USER $FTPUSER
quote PASS $FTPPASS
binary
put $FILE.gz
quit
END_SCRIPT
}

function clean_up {
  bzip2 bibs-$DATE.txt
  bzip2 items-$DATE.txt
  mv bibs-$DATE.txt.bz2 items-$DATE.txt.bz2 $FILE.gz old/
  echo The extract for $DATE has finished. We uploaded data on $NUMBIBS bibs and $NUMITEMS items. | \
    ./send-email.pl --from "$EMAILFROM" --to "$EMAILTO" --subject "collectionHQ extraction has finished"
}

get_data_from_sql && \
      format_data && \
      upload_data && \
         clean_up #.
exit
