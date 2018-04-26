ANSIBLE_DOCKER_ENV_PATH ?= /tmp/ansible.docker.env
ANSIBLE_VAULT_PASSWORD_PATH ?= /tmp/ansible.vault

.vault_pass:
ifdef ANSIBLE_VAULT_PASSWORD
	@echo $(ANSIBLE_VAULT_PASSWORD) > $(ANSIBLE_VAULT_PASSWORD_PATH)
else
	@cat ~/.vault_pass > $(ANSIBLE_VAULT_PASSWORD_PATH)
endif

ansible.docker.env:
	@env | grep AWS_ > $(ANSIBLE_DOCKER_ENV_PATH)

# in order to get SSH-forwarding working on macOS, follow the
# instructions here:
# https://github.com/uber-common/docker-ssh-agent-forward
ANSIBLE_DOCKER_OPTS := \
    -v ssh-agent:/ssh-agent -e SSH_AUTH_SOCK=/ssh-agent/ssh-agent.sock \
    --env-file $(ANSIBLE_DOCKER_ENV_PATH) \
    -v $(ANSIBLE_VAULT_PASSWORD_PATH):/root/.vault_pass \
    registry.neuromation.io/neuromationorg/ansible:latest

_run_docker_ssh_agent_forward:
	git clone git://github.com/uber-common/docker-ssh-agent-forward
	cd docker-ssh-agent-forward; make; make install
	pinata-ssh-forward

deploy_platformapi_dev: _run_docker_ssh_agent_forward _docker_login ansible.docker.env .vault_pass
	# TODO: pass tag
	docker run --rm $(ANSIBLE_DOCKER_OPTS) \
	    ansible-playbook -l 'tag_env_dev:&tag_role_master' platformapi_deploy.yml
