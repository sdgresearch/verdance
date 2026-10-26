#!/bin/sh
# Spark's Hadoop layer needs a user name for the current UID. A container run as an
# arbitrary --user (e.g. --user "$(id -u):$(id -g)") has none, so add one first.
if ! whoami >/dev/null 2>&1; then
    echo "user:x:$(id -u):$(id -g):verdance user:${HOME}:/bin/sh" >> /etc/passwd
fi
exec verdance "$@"
