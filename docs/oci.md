# OCI builds

Public OCI images are available at this address.
```text
ghcr.io/crate-workbench/cratedb-xlens
```

Invoke using Docker or Podman.
```shell
docker run --rm --network=host ghcr.io/crate-workbench/cratedb-xlens
```

Build image locally.
```shell
docker build --tag cratedb-xlens:local .
```
