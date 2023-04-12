# Running RaftKeeper, A Distributed System Coordinator

## Prerequisite

You should have a cluster with at least three nodes, and each node requires at least 2 CPUs and 4 GiB of memory. 

You must configure your cluster to dynamically provision PersistentVolumes. If your cluster is not configured to do so, you will have to manually provision three 2 GiB volumes before starting.

More information on [ZooKeeper deployment on k8s](https://kubernetes.io/docs/tutorials/stateful-application/zookeeper/).

## Creating a RaftKeeper ensemble

The manifest below contains a [Headless Service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services), a [Service](https://kubernetes.io/docs/concepts/services-networking/service/), a [PodDisruptionBudget](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/#pod-disruption-budgets), and a [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).

```yaml
apiVersion: v1
kind: Service
metadata:
  name: rk-hs
  labels:
    app: rk
spec:
  ports:
    - port: 8102     # DO NOT MODIFY
      name: server     
    - port: 8103     # DO NOT MODIFY
      name: raft
  clusterIP: None
  selector:
    app: rk
---
apiVersion: v1
kind: Service
metadata:
  name: rk-cs
  labels:
    app: rk
spec:
  ports:
    - port: 8101     # DO NOT MODIFY
      name: client
  selector:
    app: rk
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: rk-pdb
spec:
  selector:
    matchLabels:
      app: rk
  maxUnavailable: 1
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rk
spec:
  selector:
    matchLabels:
      app: rk
  serviceName: rk-hs
  replicas: 3
  updateStrategy:
    type: RollingUpdate
  podManagementPolicy: Parallel # OrderedReady
  template:
    metadata:
      labels:
        app: rk
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchExpressions:
                  - key: "app"
                    operator: In
                    values:
                      - rk
              topologyKey: "kubernetes.io/hostname"
      containers:
        - name: kubernetes-raftkeeper
          imagePullPolicy: Always
          image: raftkeeper/raftkeeper-k8s:latest
          resources:
            requests:
              memory: "2Gi"
              cpu: "1"
          ports:
            - containerPort: 8101   # DO NOT MODIFY
              name: client
            - containerPort: 8102   # DO NOT MODIFY
              name: server
            - containerPort: 8103   # DO NOT MODIFY
              name: raft
          command:
            - sh
            - -c
            # Refer to https://github.com/JDRaftKeeper/RaftKeeper/blob/master/programs/server/config.xml for more parameters
            - "bash start-raftkeeper.sh \ 
                    --server 3 \ 
                    --keeper.raft_settings.nuraft_thread_size 2 \ 
                    --keeper.thread_count 2"                  
          livenessProbe:
            exec:
              command:
                - sh
                - -c
                - "bash raftkeeper-ready.sh 8101"
            initialDelaySeconds: 60
            periodSeconds: 30
            timeoutSeconds: 10
          volumeMounts:
            - name: datadir
              mountPath: /opt/raftkeeper/data
            - name: logdir
              mountPath: /opt/raftkeeper/log
  volumeClaimTemplates:
    - metadata:
        name: datadir
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 2Gi
    - metadata:
        name: logdir
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 2Gi
```

1. Use the [`kubectl apply`](https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands/#apply) command to create the manifest.

```shell
kubectl apply -f https://raw.githubusercontent.com/JDRaftKeeper/RaftKeeper/master/k8s/k8s-deployment.yaml
```

2. Use `kubectl get` to watch the StatefulSet controller create the StatefulSet's Pods.

```shell
kubectl get pods -w -l app=rk
```

3. Once all three pods  are Running and Ready, use `CTRL-C` to terminate kubectl. 

``` 
rk-1      1/1       Running   0         24s
rk-0      1/1       Running   0         30s
rk-2      1/1       Running   0         32s
```

The StatefulSet controller creates three Pods, and each Pod has a container with a RaftKeeper server. 

## Sanity testing the ensemble

The most basic sanity test is to write data to one RaftKeeper server and to read the data from another.

The command below executes the `zkCli.sh` script to write `world` to the path `/hello` on the `rk-0` Pod in the ensemble.

```shell
kubectl exec rk-0 -- zkCli.sh -server localhost:8101 create /hello world
```

To get the data from the `rk-1` Pod use the following command.

``` shell
kubectl exec rk-1 -- zkCli.sh get /hello
```

```
WATCHER::

WatchedEvent state:SyncConnected type:None path:null
Created /hello
```

The data that you created on `rk-0` is available on all the servers in the ensemble.

```
WATCHER::

WatchedEvent state:SyncConnected type:None path:null
world
```

If you succesfully get the value `world`, it means RaftKeeper cluster has achieved consensus and is able to serve client requests.

## Testing for durable storage

Use the [`kubectl delete`](https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands/#delete) command to delete the `rk` StatefulSet. Each pod of the the StatefulSet will be terminated soon.

```shell
kubectl delete statefulset rk
```

Watch the termination of the Pods in the StatefulSet.

```shell
kubectl get pods -w -l app=rk
```

When all three pods are fully terminated, use `CTRL-C` to terminate kubectl.

```
rk-2      1/1       Terminating   0         9m
rk-0      1/1       Terminating   0         11m
rk-1      1/1       Terminating   0         10m
```

Reapply the manifest in `k8s-deployment.yaml`.

```shell
kubectl apply -f https://raw.githubusercontent.com/JDRaftKeeper/RaftKeeper/master/k8s/k8s-deployment.yaml
```

This creates the `rk` StatefulSet object, but the other API objects in the manifest are not modified because they already exist.

```
service/rk-hs unchanged
service/rk-cs unchanged
poddisruptionbudget.policy/rk-pdb configured
statefulset.apps/rk configured
```

Watch the StatefulSet controller recreate the StatefulSet's Pods.

```shell
kubectl get pods -w -l app=rk
```

Once all three Pods are Running and Ready, use `CTRL-C` to terminate kubectl.

```
NAME      READY     STATUS    RESTARTS   AGE
rk-0      0/1       Pending   0          0s
rk-0      0/1       ContainerCreating   0s         0s
rk-0      0/1       Running   0         19s
rk-1      0/1       Pending   0         0s
rk-1      0/1       ContainerCreating   0         0s
rk-1      0/1       Running   0         18s
rk-2      0/1       Pending   0         0s
rk-2      0/1       ContainerCreating   0         0s
rk-2      0/1       Running   0         19s
```

Use the command below to get the value you entered during the [sanity test](https://kubernetes.io/docs/tutorials/stateful-application/zookeeper/#sanity-testing-the-ensemble), from the `rk-2` Pod.

```shell
kubectl exec rk-2 zkCli.sh -server localhost:8101 get /hello
```

Even though you terminated and recreated all of the Pods in the `rk` StatefulSet, the ensemble still serves the original value.

```
WATCHER::

WatchedEvent state:SyncConnected type:None path:null
world
```

The `volumeClaimTemplates` field of the `rk` StatefulSet's `spec` specifies a PersistentVolume provisioned for each Pod. The `StatefulSet` controller generates a `PersistentVolumeClaim` for each Pod in the `StatefulSet`.

Use the following command to get the `StatefulSet`'s `PersistentVolumeClaims`.

```shell
kubectl get pvc -l app=rk
```

## Testing for liveness

The Pod `template` for the `rk` `StatefulSet` specifies a liveness probe.

```yaml
livenessProbe:
  exec:
    command:
      - sh
      - -c
      - "bash raftkeeper-ready.sh 8101"
  initialDelaySeconds: 60
  periodSeconds: 30
  timeoutSeconds: 10
```

The probe calls a bash script that uses the RaftKeeper `ruok` four letter word to test the server's health.

```bash
for i in {1..10}; do
    OK=$(echo ruok | nc 127.0.0.1 $1)
    if [ "$OK" == "imok" ]; then
      exit 0
    fi
    sleep 1
done

exit 1
```

Since the probe might try 10 times in a row and sleep for 1s for each probe, make sure `timeoutSeconds` is greater than 10s.

When the liveness probe for the ZooKeeper process fails, Kubernetes will automatically restart the process for you, ensuring that unhealthy processes in the ensemble are restarted.

```
47s         Normal    Pulling     pod/rk-1   Pulling image "raftkeeper/raftkeeper-k8s:latest"
43s         Normal    Created     pod/rk-1   Created container kubernetes-rafkeeper
43s         Normal    Started     pod/rk-1   Started container kubernetes-rafkeeper
77s         Warning   Unhealthy   pod/rk-1   Liveness probe failed: command "sh -c bash raftkeeper-ready.sh 8101" timed out
77s         Normal    Killing     pod/rk-1   Container kubernetes-rafkeeper failed liveness probe, will be restarted
```



## Tolerating Node failure

RaftKeeper needs a quorum of servers to successfully commit mutations to data. For a three server ensemble, two servers must be healthy for writes to succeed. In quorum based systems, members are deployed across failure domains to ensure availability. To avoid an outage, due to the loss of an individual machine, best practices preclude co-locating multiple instances of the application on the same machine.

By default, Kubernetes may co-locate Pods in a `StatefulSet` on the same node. For the three server ensemble you created, if two servers are on the same node, and that node fails, the clients of your RaftKeeper service will experience an outage until at least one of the Pods can be rescheduled.

You should always provision additional capacity to allow the processes of critical systems to be rescheduled in the event of node failures. If you do so, 
then the outage will only last until the Kubernetes scheduler reschedules one of the RaftKeeper servers. However, if you want your service to tolerate node failures with no downtime, you should set `podAntiAffinity`.

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchExpressions:
            - key: "app"
              operator: In
              values:
                - rk
        topologyKey: "kubernetes.io/hostname"
```

The `requiredDuringSchedulingIgnoredDuringExecution` field tells the Kubernetes Scheduler that it should never co-locate two Pods which have `app` label as `rk` in the domain defined by the `topologyKey`.
