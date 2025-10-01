# todo: the code below doesn't really belongs to a kube client, and should be removed,
#  once we'll have an event-system in place.
#  the reason why it's here, is that we want to avoid a code duplication,
#  because many service might need to create a namespace
import re
from hashlib import sha256

from platform_api.old_kube_client.client import KubeClient
from platform_api.old_kube_client.errors import ResourceExists
from platform_api.old_kube_client.namespace import Namespace, NamespaceApi

KUBE_NAME_LENGTH_MAX = 63
DASH = "-"
KUBE_NAMESPACE_SEP = DASH * 2
KUBE_NAMESPACE_PREFIX = "platform"
KUBE_NAMESPACE_HASH_LENGTH = 24
NO_ORG = "NO_ORG"
RE_DASH_REPLACEABLE = re.compile(r"[\s_:/\\]+")

NAMESPACE_ORG_LABEL = "platform.apolo.us/org"
NAMESPACE_PROJECT_LABEL = "platform.apolo.us/project"


def generate_hash(name: str) -> str:
    return sha256(name.encode("utf-8")).hexdigest()[:KUBE_NAMESPACE_HASH_LENGTH]


def normalize_name(name: str) -> str:
    return re.sub(RE_DASH_REPLACEABLE, DASH, name).lower().strip()


def generate_namespace_name(org_name: str, project_name: str) -> str:
    """
    returns a Kubernetes resource name in the format
    `platform--<org_name>--<project_name>--<hash>`,
    ensuring that the total length does not exceed `KUBE_NAME_LENGTH_MAX` characters.

    - `platform--` prefix is never truncated
    - `<hash>` (a sha256 truncated to 24 chars), is also never truncated
    - if the names are long, we truncate them evenly,
      so at least some parts of both org and proj names will remain
    """
    org_name = normalize_name(org_name)
    project_name = normalize_name(project_name)

    hashable = f"{org_name}{KUBE_NAMESPACE_SEP}{project_name}"
    name_hash = generate_hash(hashable)

    len_reserved = (
        len(KUBE_NAMESPACE_PREFIX)
        + (len(KUBE_NAMESPACE_SEP) * 3)
        + KUBE_NAMESPACE_HASH_LENGTH
    )
    len_free = KUBE_NAME_LENGTH_MAX - len_reserved
    if len(hashable) <= len_free:
        return (
            f"{KUBE_NAMESPACE_PREFIX}"
            f"{KUBE_NAMESPACE_SEP}"
            f"{hashable}"
            f"{KUBE_NAMESPACE_SEP}"
            f"{name_hash}"
        )

    # org and project names do not fit into a full length.
    # let's figure out the full length of org and proj, and calculate a ratio
    # between org and project, so that we'll truncate more chars from the
    # string which actually has more chars
    len_org, len_proj = len(org_name), len(project_name)
    len_org_proj = len_org + len_proj
    exceeds = len_org_proj - len_free

    # ratio calculation. for proj can be derived via an org ratio
    remove_from_org = round((len_org / len_org_proj) * exceeds)
    remove_from_proj = exceeds - remove_from_org

    new_org_name = org_name[: max(1, len_org - remove_from_org)]
    new_project_name = project_name[: max(1, len_proj - remove_from_proj)]

    return (
        f"{KUBE_NAMESPACE_PREFIX}"
        f"{KUBE_NAMESPACE_SEP}"
        f"{new_org_name}"
        f"{KUBE_NAMESPACE_SEP}"
        f"{new_project_name}"
        f"{KUBE_NAMESPACE_SEP}"
        f"{name_hash}"
    )


async def create_namespace(
    kube_client: KubeClient, org_name: str, project_name: str
) -> Namespace:
    """
    Creates a namespace based on a provided org and project names.
    Applies default labels and network policies.
    """
    # normalize names, by replacing illegal characters with dashes, lower-casing, etc.
    org_name = normalize_name(org_name)
    project_name = normalize_name(project_name)

    namespace_name = generate_namespace_name(org_name, project_name)
    namespace_api = NamespaceApi(kube_client)

    # use default labels
    labels = {
        NAMESPACE_ORG_LABEL: org_name,
        NAMESPACE_PROJECT_LABEL: project_name,
    }

    try:
        # let's try to create a namespace
        namespace = await namespace_api.create_namespace(
            name=namespace_name,
            labels=labels,
        )
    except ResourceExists:
        # of get, it if it doesn't exist
        namespace = await namespace_api.get_namespace(name=namespace_name)

    k8s_api_eps_url = kube_client.generate_endpoint_slice_url("default", "kubernetes")
    k8s_api_eps = await kube_client.get(k8s_api_eps_url)

    # now let's create a network policy, which will allow a namespace-only access
    network_policy_url = kube_client.generate_network_policy_url(namespace_name)
    try:
        await kube_client.post(
            network_policy_url,
            json={
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {
                    "name": namespace_name,
                    "namespace": namespace_name,
                },
                "spec": {
                    "podSelector": {},  # all POD's in the namespace
                    "policyTypes": ["Egress"],
                    "egress": [
                        {
                            "to": [
                                {
                                    "namespaceSelector": {"matchLabels": labels},
                                    # allow traffic to all pods in this ns
                                    "podSelector": {},
                                }
                            ]
                        },
                        # allowing pods to connect to public networks only
                        {
                            "to": [
                                {
                                    "ipBlock": {
                                        "cidr": "0.0.0.0/0",
                                        "except": [
                                            "10.0.0.0/8",
                                            "172.16.0.0/12",
                                            "192.168.0.0/16",
                                        ],
                                    }
                                }
                            ]
                        },
                        # allowing labeled pods to make DNS queries in our private
                        # networks, because pods' /etc/resolv.conf files still
                        # point to the internal DNS
                        {
                            "to": [
                                {"ipBlock": {"cidr": "10.0.0.0/8"}},
                                {"ipBlock": {"cidr": "172.16.0.0/12"}},
                                {"ipBlock": {"cidr": "192.168.0.0/16"}},
                            ],
                            "ports": [
                                {"port": 53, "protocol": "UDP"},
                                {"port": 53, "protocol": "TCP"},
                            ],
                        },
                        # allowing traffic to ingress controller
                        {
                            "to": [
                                {
                                    "namespaceSelector": {},
                                    # allow traffic to all pods in this ns
                                    "podSelector": {
                                        "matchLabels": {
                                            "platform.apolo.us/component": "ingress-gateway"  # noqa
                                        }
                                    },
                                }
                            ],
                        },
                        # allowing traffic to K8s API
                        {
                            "to": [
                                {"ipBlock": {"cidr": f"{address}/32"}}
                                for endpoint in k8s_api_eps["endpoints"]
                                for address in endpoint["addresses"]
                            ],
                            "ports": [
                                {"port": int(port["port"]), "protocol": "TCP"}
                                for port in k8s_api_eps["ports"]
                            ],
                        },
                    ],
                },
            },
        )
    except ResourceExists:
        pass

    return namespace
