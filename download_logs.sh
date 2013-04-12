#!/usr/bin/bash

function usage() {
    echo "$0 <hostname_1> [<hostname_2>] [-p <hostname_3>]

    Downloads pypln log files from all the provided hosts and save them to
    data/<hostname>_broker.log. This assumes pypln was installed using the regular
    deployment strategy.

    For every hostname after the -p option, the script will look for files in
    /srv/pypln/wikipedia/logs instead of /srv/pypln/logs.
    "
}

if [ $# -eq 0 ]
then
    usage
    exit 1
fi

if [ $1 == "--help" ] || [ $1 == "-h" ]
then
    usage
    exit 0
fi

BASE_DIR="/srv/pypln/logs"
for HOST in $*
do
    if [ "$HOST" == "-p" ]
    then
        BASE_DIR="/srv/pypln/wikipedia/logs"
        continue
    fi
    scp pypln@"$HOST":"$BASE_DIR"/pypln-broker.out data/"$HOST"_broker.log
done
