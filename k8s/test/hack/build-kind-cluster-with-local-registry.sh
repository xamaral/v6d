#!/bin/sh
set -o errexit

kind_name=kind
k8s_version=v1.29.0
kubeconfig_path=/tmp/e2e-k8s.config

if [ ! -z "$1" ] ; then
    kind_name=$1
fi
# create registry container unless it already exists
reg_name='kind-registry'
reg_port='5001'
if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
  docker run \
    -d --restart=always -p "127.0.0.1:${reg_port}:5000" --name "${reg_name}" \
    registry:2
fi

# create a cluster with the local registry enabled in containerd
cat <<EOF | kind create cluster --name=${kind_name} --kubeconfig=${kubeconfig_path} --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:${reg_port}"]
    endpoint = ["http://${reg_name}:5000"]
kubeadmConfigPatches:
- |
  kind: InitConfiguration
  nodeRegistration:
    kubeletExtraArgs:
      cgroup-driver: systemd
- |
  kind: JoinConfiguration
  nodeRegistration:
    kubeletExtraArgs:
      cgroup-driver: systemd
nodes:
- role: control-plane
  image: kindest/node:v1.29.0
- role: worker
  image: kindest/node:v1.29.0
- role: worker
  image: kindest/node:v1.29.0
- role: worker
  image: kindest/node:v1.29.0
EOF

# connect the registry to the cluster network if not already connected
if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${reg_name}")" = 'null' ]; then
  docker network connect "kind" "${reg_name}"
fi

export KUBECONFIG=${kubeconfig_path}
# Document the local registry
# https://github.com/kubernetes/enhancements/tree/master/keps/sig-cluster-lifecycle/generic/1755-communicating-a-local-registry
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${reg_port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF

# preload vineyardd image to local registry
echo "Preloading vineyardd image..."
docker pull vineyardcloudnative/vineyardd:latest
docker tag vineyardcloudnative/vineyardd:latest localhost:${reg_port}/vineyardd:latest
docker push localhost:${reg_port}/vineyardd:latest