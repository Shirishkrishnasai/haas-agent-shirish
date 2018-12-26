#!/bin/bash
# ssh key copy to all slave nodes
# $USER for username of namenode 
# $HOSTNAME for hostname of namenode 

for ip in `cat /opt/hadoop/etc/hadoop/slaves`; do 
   sshpass -p password ssh-copy-id -i $ip >> log.file
done

var=$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/');

for i in !var; do sshpass -p password ssh-copy-id -i  $USER@$HOSTNAME
done


