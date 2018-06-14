import asyncio
import json
from pathlib import PurePath

import pytest

from platform_api.orchestrator.kube_orchestrator import (
    KubeClient, KubeConfig, VolumeType,)


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

        jobs_ingress_name='platformjobsingress',
        jobs_ingress_domain_name='jobs.platform.neuromation.io',

        endpoint_url=cluster['server'],
        cert_authority_path=cluster['certificate-authority'],
        auth_cert_path=user['client-certificate'],
        auth_cert_key_path=user['client-key']
    )


@pytest.fixture(scope='session')
async def kube_ingress_ip(kube_config_cluster_payload):
    cluster = kube_config_cluster_payload
    from urllib.parse import urlsplit
    return urlsplit(cluster['server']).hostname


class TestKubeClient(KubeClient):
    @property
    def _endpoints_url(self):
        return f'{self._namespace_url}/endpoints'

    def _generate_endpoint_url(self, name):
        return f'{self._endpoints_url}/{name}'

    async def get_endpoint(self, name):
        url = self._generate_endpoint_url(name)
        return await self._request(method='GET', url=url)

    async def request(self, *args, **kwargs):
        return await self._request(*args, **kwargs)


@pytest.fixture(scope='session')
async def kube_client(kube_config):
    config = kube_config
    # TODO (A Danshyn 06/06/18): create a factory method
    client = TestKubeClient(
        base_url=config.endpoint_url,

        cert_authority_path=config.cert_authority_path,

        auth_type=config.auth_type,
        auth_cert_path=config.auth_cert_path,
        auth_cert_key_path=config.auth_cert_key_path,

        namespace=config.namespace,
        conn_timeout_s=config.client_conn_timeout_s,
        read_timeout_s=config.client_read_timeout_s,
        conn_pool_size=config.client_conn_pool_size
    )
    async with client:
        yield client


@pytest.fixture(scope='session')
async def nfs_volume_server(kube_client):
    payload = await kube_client.get_endpoint('platformstoragenfs')
    return payload['subsets'][0]['addresses'][0]['ip']


@pytest.fixture(scope='session')
async def kube_config_nfs(
        kube_config_cluster_payload, kube_config_user_payload,
        nfs_volume_server):
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload
    return KubeConfig(
        storage_mount_path=PurePath('/var/storage'),

        endpoint_url=cluster['server'],
        cert_authority_path=cluster['certificate-authority'],
        auth_cert_path=user['client-certificate'],
        auth_cert_key_path=user['client-key'],

        storage_type=VolumeType.NFS,
        nfs_volume_server=nfs_volume_server,
        nfs_volume_export_path=PurePath('/var/storage'),
    )
