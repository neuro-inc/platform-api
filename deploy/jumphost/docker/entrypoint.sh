#!/bin/bash

mkdir -p ssh-daemon-sessing

if [[ ! -e /home/ssh-daemon-settings/ssh_host_rsa_key ]]; then
    ssh-keygen -f /home/ssh-daemon-settings/ssh_host_rsa_key -N '' -t rsa
fi

if [[ ! -e /home/ssh-daemon-settings/ssh_host_ecdsa_key ]]; then
    ssh-keygen -f /home/ssh-daemon-settings/ssh_host_ecdsa_key -N '' -t rsa
fi

if [[ ! -e /home/ssh-daemon-settings/ssh_host_ed25519_key ]]; then
    ssh-keygen -f /home/ssh-daemon-settings/ssh_host_ed25519_key -N '' -t rsa
fi

cp /home/ssh-daemon-settings/ssh_host_rsa_key /etc/ssh/ssh_host_rsa_key
cp /home/ssh-daemon-settings/ssh_host_ecdsa_key /etc/ssh/ssh_host_ecdsa_key
cp /home/ssh-daemon-settings/ssh_host_ed25519_key /etc/ssh/ssh_host_ed25519_key


mkdir -p /root/.ssh
touch /root/.ssh/authorized_keys
echo ${NP_JUMP_HOST_ROOT_TOKEN} > /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys

accounts=($(find /home/*/.ssh -type d))
for i in "${accounts[@]}"
do
	names=(${i//// });
	name=${names[1]};
	adduser -d /home/${name} ${name};
	chown -R ${name}:${name} /home/${name};
done

#Add current team member set to bastion host
users=(
  "adavydow"
  "aleksartamonov"
  "alekseysh"
  "alexander-rakhlin"
  "alias2696"
  "angusroven"
  "ajuszkowski"
  "astaff"
  "asvetlov"
  "bennylilom"
  "berenigo"
  "dalazx"
  "dariaposrednikova"
  "didcv"
  "doctoraugust"
  "fedor-s"
  "fedorsavchenko"
  "freestlr"
  "genn"
  "gigigheorghiceanu"
  "golovanovsrg"
  "kykyxdd"
  "lastperson"
  "mgdskr"
  "nadia-wednesday"
  "ne-bo"
  "nebeskey"
  "oanuthriveio"
  "oktai15"
  "pglolo"
  "rauf-kurbanov"
  "ryter"
  "shagren"
  "snikolenko"
  "startapista"
  "stasbel"
  "sudodoki"
  "truskovskiyk"
  "vasiliy28"
  "yagosu"
  "ybehzadi"
  "zubrabubra"
)

for user in ${users[@]}; do
    echo $user
    if [ ! -d "/home/${user}" ]; then
        adduser -d /home/${user} ${user};
        mkdir /home/${user}/.ssh;
        curl https://github.com/${user}.keys >> /home/${user}/.ssh/authorized_keys
        chown -R ${user}:${user} /home/${user};
        chmod 600 /home/${user}/.ssh/authorized_keys
        chmod 700 /home/${user}/.ssh
    fi
done

/usr/sbin/sshd -D