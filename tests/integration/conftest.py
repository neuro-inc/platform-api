import asyncio
import json
from pathlib import PurePath

import pytest

from platform_api.orchestrator.kube_orchestrator import KubeConfig


@pytest.fixture(scope='session')
def event_loop():
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)

    watcher = asyncio.SafeChildWatcher()
    watcher.attach_loop(loop)
    asyncio.get_event_loop_policy().set_child_watcher(watcher)

    yield loop
    loop.close()


@pytest.fixture(scope='session')
async def kube_config_payload():
    process = await asyncio.create_subprocess_exec(
        'kubectl', 'config', 'view', '-o', 'json',
        stdout=asyncio.subprocess.PIPE)
    output, _ = await process.communicate()
    payload_str = output.decode().rstrip()
    return json.loads(payload_str)


@pytest.fixture(scope='session')
async def kube_config_cluster_payload(kube_config_payload):
    cluster_name = 'minikube'
    clusters = {
        cluster['name']: cluster['cluster']
        for cluster in kube_config_payload['clusters']}
    return clusters[cluster_name]


@pytest.fixture(scope='session')
async def kube_config_user_payload(kube_config_payload):
    user_name = 'minikube'
    users = {
        user['name']: user['user']
        for user in kube_config_payload['users']}
    return users[user_name]


@pytest.fixture(scope='session')
async def kube_config(kube_config_cluster_payload, kube_config_user_payload):
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload
    return KubeConfig(
        storage_mount_path=PurePath('/tmp'),

        endpoint_url=cluster['server'],
        cert_authority_path=cluster['certificate-authority'],
        auth_cert_path=user['client-certificate'],
        auth_cert_key_path=user['client-key']
    )
