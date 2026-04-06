# Deployment

The application is deployed as an AWS Serverless Application using Chalice + CloudFormation.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| `uv` | Package manager — [install docs](https://docs.astral.sh/uv/) |
| `aws` CLI | Must be configured with credentials that can deploy CloudFormation |
| `DD_API_KEY` | Datadog API key — required at deploy time |

AWS credentials can be provided via environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) or via `aws configure`.

---

## Deploy

```shell
DD_API_KEY=<your-key> ./devops/deploy.sh
```

The script runs with `set -x` so every step is printed to stdout.

---

## What the deploy script does

### 1. Export runtime dependencies

```shell
uv export --no-hashes --no-dev > mbta-performance/requirements.txt
```

Generates a `requirements.txt` from the lockfile, excluding dev-only packages. Chalice uses this to build the Lambda layer.

### 2. Package with Chalice

```shell
uv run chalice package --stage prod --merge-template .chalice/resources.json cfn/
```

Produces two artifacts in `cfn/`:

- `sam.json` — CloudFormation/SAM template
- `layer-deployment.zip` — Lambda layer containing all Python dependencies

### 3. Shrink the layer zip

Lambda layers have a 250 MB unzipped size limit (79.1 MB compressed enforced by the script). `devops/helpers.sh` strips unnecessary files from the zip to stay under the limit:

- `__pycache__/` and `.pyc` files
- Test suites from numpy, pandas, pyarrow, SQLAlchemy, ddtrace
- Unused boto3 service data directories
- C/C++ source files (`.c`, `.cpp`, `.h`, `.pyx`, `.pxd`)
- Excess timezone data (non-US zones)
- License, author, and notice files

The script prints the before/after sizes and aborts if the result still exceeds 79.1 MB.

### 4. Upload artifacts to S3

```shell
aws cloudformation package \
  --template-file cfn/sam.json \
  --s3-bucket mbta-performance-lambda-deployments \
  --output-template-file cfn/packaged.yaml
```

Uploads the layer zip to the `mbta-performance-lambda-deployments` S3 bucket and rewrites the template with the S3 URI.

### 5. Deploy the CloudFormation stack

```shell
aws cloudformation deploy \
  --template-file cfn/packaged.yaml \
  --stack-name mbta-performance \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset \
  --parameter-overrides DDApiKey=... GitVersion=... DDTags=...
```

Creates or updates the `mbta-performance` CloudFormation stack. `--no-fail-on-empty-changeset` means a deploy with no changes exits cleanly rather than erroring.

---

## CloudFormation parameters

| Parameter | Source | Description |
|---|---|---|
| `DDApiKey` | `DD_API_KEY` env var | Datadog API key, injected into Lambda env |
| `GitVersion` | `git describe --tags --always` | Version string for Datadog tagging |
| `DDTags` | Constructed from git SHA + repo URL | Additional Datadog tags |
