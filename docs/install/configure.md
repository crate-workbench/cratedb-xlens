# Configure

Create an `.env` file with your CrateDB connection details:

**For localhost CrateDB:**

```bash
CRATE_CONNECTION_STRING=https://localhost:4200
CRATE_USERNAME=crate
# CRATE_PASSWORD=  # Leave empty or unset for default crate user
CRATE_SSL_VERIFY=false
```

**For remote CrateDB:**

```bash
CRATE_CONNECTION_STRING=https://your-cluster.cratedb.net:4200
CRATE_USERNAME=your-username
CRATE_PASSWORD=your-password
CRATE_SSL_VERIFY=true
```
