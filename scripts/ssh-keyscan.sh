#! /bin/bash
# scan  all nodes sshkeys and adding those keys in known_hosts
ssh-keyscan  localhost >> /home/hadoop/.ssh/known_hosts
ssh-keyscan $HOSTNAME >> /home/hadoop/.ssh/known_hosts
ssh-keyscan 0.0.0.0 >> /home/hadoop/.ssh/known_hosts
ssh-keyscan 127.0.0.1 >> /home/hadoop/.ssh/known_hosts
ssh-keyscan ::1 >> /home/hadoop/.ssh/known_hosts
ssh-keyscan 127.0.1.1 >> /home/hadoop/.ssh/known_hosts

for ip in `cat /etc/hosts`; do 
   ssh-keyscan $ip >> /home/hadoop/.ssh/known_hosts 
done
for ip in `cat /opt/hadoop/etc/hadoop/slaves`; do 
   ssh-keyscan $ip >> /home/hadoop/.ssh/known_hosts 
done

