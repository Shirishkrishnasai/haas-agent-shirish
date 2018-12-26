#! /bin/bash
#ssh key genenaration
# $1 for namenode user password 

var=$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/');
for i in !var; do sshpass -p  password ssh localhost "ssh-keygen -t rsa -P '' -f /home/hadoop/.ssh/id_rsa;cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys;chmod 0600 /home/hadoop/.ssh/authorized_keys"; done



