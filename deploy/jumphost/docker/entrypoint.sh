#!/bin/bash

ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa
ssh-keygen -f /etc/ssh/ssh_host_ecdsa_key -N '' -t ecdsa
ssh-keygen -f /etc/ssh/ssh_host_ed25519_key -N '' -t ed25519

mkdir -p /root/.ssh
touch /root/.ssh/authorized_keys
echo ${NP_JUMP_HOST_ROOT_TOKEN} > /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys

find /home/*/.ssh -type d |while read -r line ; do arr=(${line//// }); adduser -d /home/${arr[1]} ${arr[1]}; chown -R ${arr[1]}:${arr[1]} /home/${arr[1]}; done

/usr/sbin/sshd -D