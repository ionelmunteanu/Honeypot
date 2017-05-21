#!bin/bash

cd /

./usr/local/bin/confd -onetime -backend env

./antidote/bin/antidote start

echo "master: $CLUSTER_MASTER_NAME"
echo "hosntame: $HOSTNAME"

if [ "$CLUSTER_MASTER_NAME" = "$HOSTNAME" ]; then 
	echo "MASTER IS WAITING 15 seconds"
	sleep 5
	/antidote/bin/antidote-admin cluster plan
	sleep 5
	/antidote/bin/antidote-admin cluster commit
	sleep 5
	/antidote/bin/antidote-admin cluster plan
	sleep 5
	/antidote/bin/antidote-admin cluster commit
	/antidote/bin/antidote-admin  member-status
else
	echo "NODE IS WAITING 1 seconds"
	sleep 5
	./antidote/bin/antidote-admin cluster join "$CLUSTER_MASTER_NAME@$CLUSTER_MASTER_NAME"
fi	

sleep 5
./antidote/bin/antidote attach