#!/bin/bash

if (( $# < 1 ))
then
    echo USAGE: ./deploy.sh host.com:path/to/public/html
    exit 1
fi

remote_host=${1%/}

parcel build --public-url /cfr-court-opinions index.html
rsync -rt --info=progress2 --mkpath dist/ "$1/cfr-court-opinions"
