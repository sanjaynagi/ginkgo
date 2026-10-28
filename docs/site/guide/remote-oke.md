---
orphan: true
---

# Running On OKE

This page shows one way to run a whole workflow on Oracle Kubernetes Engine
(OKE). It builds on [Remote Execution](remote-execution.md), so read that page
first.

```{note}
This layout has not yet run on OKE itself. The project's tests run it on a
generic Kubernetes cluster with an S3 store.
```

## The Layout

The driver is the `ginkgo run` process. It schedules tasks and submits a
Kubernetes Job for each remote task. Here the driver runs as a Job of its own,
in the same namespace as its workers. The driver and the workers sign their
Oracle Cloud Infrastructure (OCI) requests with Workload Identity. Workload
Identity lets a pod act as its Kubernetes service account, so no API key is
mounted anywhere.

The `deploy/oke/` directory in the Ginkgo repository holds a reference file for
each step below.

## Steps

1. **Identity.** Workload Identity needs an OKE Enhanced cluster. Choose the
   namespace and service account that the driver and its workers run as. Let
   that service account read the source buckets, and read and write the
   artifact store. The driver needs read access too. It checks the version or
   ETag (the object store's content tag) of each remote input to build cache
   keys before it submits anything.

    ```text
    Allow any-user to read objects in compartment <compartment> where all {
      request.principal.type = 'workload',
      request.principal.cluster_id = '<cluster-ocid>',
      request.principal.namespace = '<namespace>',
      request.principal.service_account = '<service-account>' }
    Allow any-user to manage objects in compartment <compartment> where all {
      request.principal.type = 'workload',
      request.principal.cluster_id = '<cluster-ocid>',
      request.principal.namespace = '<namespace>',
      request.principal.service_account = '<service-account>',
      target.bucket.name = '<artifacts-bucket>' }
    ```

2. **Image.** Build the repository's `Dockerfile` and push it to OCI Registry
   (OCIR) as the base image. Then build the workflow's image on top of it with
   `deploy/oke/Dockerfile`. The driver and its workers run this one image, so a
   worker loads a task from the same path the driver did. Create a pull secret
   for OCIR in the namespace.
3. **Permissions.** The driver creates a Job per task, lists each Job's pods,
   reads their logs, and deletes Jobs when a run is cancelled. Apply
   `deploy/oke/driver-role.yaml` and `deploy/oke/driver-role-binding.yaml` to
   grant exactly that.
4. **Config.** `deploy/oke/ginkgo.toml` sets up these pieces:
   - Workload Identity for the driver, in `[remote.oci]`.
   - Workload Identity for the workers, through the executor's `env`.
   - The artifact store.
   - An executor named `oke`.

   Tasks run on OKE when they name it with `@task(executor="oke")`, or when the
   run makes it the default with `--executor oke`.
5. **Run.** `deploy/oke/driver-job.yaml` runs `ginkgo run` in the image. It
   mounts a PersistentVolumeClaim (PVC) at the project's `.ginkgo/` directory.
   Run history and the cache then outlive the pod, so a rerun reuses earlier
   results. `.ginkgo/` holds a SQLite database, so check that the volume
   supports SQLite's file locking. Give each workflow its own `subPath` on the
   volume.

## Cleaning Up A Cancelled Run

Set `GINKGO_PARENT_RUN_ID` in the driver's environment to the caller's run ID.
Every worker Job and pod then carries a `ginkgo/parent-run-id` label with that
value. To delete a cancelled run's workers, select them by that label:

```bash
kubectl delete jobs -l ginkgo/parent-run-id=<run-id>
```

## Settings For OKE

The executor in `deploy/oke/ginkgo.toml` uses these settings beyond the basic
ones:

| Setting | Why OKE needs it |
|---------|------------------|
| `image_pull_secrets` | OCIR is a private registry, so pods need a pull secret. |
| `env` | Passes `GINKGO_REMOTE_OCI_AUTH` and `GINKGO_REMOTE_OCI_REGION` to every worker. |
| `fuse_annotations = {}` | Turns off the default annotation, which is for Google Kubernetes Engine (GKE). |
| `ephemeral_storage` | Gives each pod room for the largest inputs it downloads. |
| `pod_annotations` | For example `karpenter.sh/do-not-disrupt`, if Karpenter manages the nodes. |
