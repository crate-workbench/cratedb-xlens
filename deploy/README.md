# XMover Deployment Guide

This directory contains Docker and Kubernetes deployment configurations for XMover.

## Quick Start

### 1. Build and Push Docker Image

```bash
# Build the image
make build

# Push to private registry
make push

# View available commands
make help
```

### 2. Deploy to Kubernetes

```bash
# Create namespace (optional)
kubectl create namespace xmover

# Create secrets first (connection string)
kubectl create secret generic xmover-config \
  --from-literal=connection-string="<base64-encoded-string>"

# Deploy CronJob for automated execution
kubectl apply -f k8s/cronjob.yaml

```

## Configuration

### Docker Registry Access

The deployment uses the existing `imagepull-cr8` secret for accessing the private registry `cloud.registry.cr8.net`.

Verify the secret exists:

```bash
kubectl get secret imagepull-cr8
```

If you need to create it:

```bash
kubectl create secret docker-registry imagepull-cr8 \
  --docker-server=cloud.registry.cr8.net \
  --docker-username=<your-username> \
  --docker-password=<your-password> \
  --docker-email=<your-email>
```

### CrateDB Connection

Create the connection string secret:

```bash
# Encode your connection string
echo -n "https://user:pass@cluster.cratedb.net:4200/_sql" | base64

# Create secret
kubectl create secret generic xmover-config \
  --from-literal=connection-string="<base64-encoded-string>"
```

## Deployment Options

### 1. CronJob (Automated)

- **File**: `k8s/cronjob.yaml`
- **Schedule**: Every 4 hours (`0 */4 * * *`)
- **Mode**: Dry-run by default (safe)
- **Purpose**: Regular maintenance automation

**Enable Production Mode**:

```yaml
args:
  - "problematic-translogs"
  - "--autoexec"
  # Remove --dry-run for actual execution
  - "--log-format"
  - "json"
```

## Security Features

### Container Security

- Non-root user (UID 1000)
- Read-only root filesystem
- No privileged escalation
- Minimal capabilities

### Network Security

- Private registry access only
- Encrypted CrateDB connections
- Secret-based configuration

### Resource Limits

- Memory: 128Mi-512Mi (CronJob), up to 1Gi (Manual)
- CPU: 100m-500m (CronJob), up to 1000m (Manual)
- Job timeout: 1-2 hours

## Monitoring

### Logs

```bash
# CronJob logs
kubectl logs -l app=xmover,component=autoexec

# Manual job logs
kubectl logs job/xmover-manual-exec -f

# All XMover logs
kubectl logs -l app=xmover --tail=100
```

### Job Status

```bash
# List jobs
kubectl get jobs -l app=xmover

# Check CronJob
kubectl get cronjobs xmover-autoexec

# Describe for details
kubectl describe cronjob xmover-autoexec
```

### Health Checks

```bash
# Test container health
kubectl run xmover-test --image=cloud.registry.cr8.net/xmover:v0.0.1 \
  --restart=Never --command -- xmover --version

# Cleanup test
kubectl delete pod xmover-test
```

## Troubleshooting

### Common Issues

**Image Pull Errors**:

```bash
# Verify registry secret
kubectl get secret imagepull-cr8 -o yaml

# Test registry access
kubectl run test --image=cloud.registry.cr8.net/xmover:v0.0.1 --restart=Never
```

**Connection Failures**:

```bash
# Check connection secret
kubectl get secret xmover-config -o yaml
kubectl get secret xmover-config -o jsonpath='{.data.connection-string}' | base64 -d

# Test connection from pod
kubectl exec -it <pod-name> -- xmover --version
```

**Resource Issues**:

```bash
# Check resource usage
kubectl top pods -l app=xmover

# Adjust resource limits in YAML files
```

### Job Management

**Cancel Running Job**:

```bash
kubectl delete job xmover-manual-exec
```

**Suspend CronJob**:

```bash
kubectl patch cronjob xmover-autoexec -p '{"spec":{"suspend":true}}'
```

**Resume CronJob**:

```bash
kubectl patch cronjob xmover-autoexec -p '{"spec":{"suspend":false}}'
```

## Development

### Local Testing

```bash
# Build and test locally
make build
make test

# Run interactive container
make run

# Development mode with volume mount
make dev
```

### Configuration Updates

```bash
# Update secrets if needed
kubectl create secret generic xmover-config \
  --from-literal=connection-string="<new-base64-encoded-string>" \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Best Practices

1. **Always test with --dry-run first**
2. **Monitor job completion and logs**
3. **Set appropriate resource limits**
4. **Use secrets for sensitive data**
5. **Enable structured logging (JSON format)**
6. **Set reasonable timeouts and retry policies**

## File Structure

```
deploy/
├── README.md              # This file
├── k8s/
│   ├── cronjob.yaml      # Automated CronJob
│   └── job-manual.yaml   # Manual Job execution
├── Dockerfile            # Container definition
├── .dockerignore         # Docker build exclusions
└── Makefile              # Build automation
```
