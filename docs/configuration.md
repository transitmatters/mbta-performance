# Configuration Reference

---

## Environment variables

### Runtime (Lambda + local)

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Python logging level. Set to `DEBUG` for verbose output (note: includes noisy s3transfer and botocore logs). |
| `LOCAL_ARCHIVE_PATH` | `./feeds` | Local directory used to cache GTFS SQLite feeds during backfill. Not used by the Lambda functions. |

### AWS credentials (local only)

Lambda functions use the IAM role attached by CloudFormation. When running locally, provide credentials via either method:

```shell
# Option A: environment variables
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Option B: AWS CLI profile
aws configure
```

### Datadog (prod only, set by Chalice/CloudFormation)

These are set automatically in the `prod` stage via `.chalice/config.json` and the CloudFormation template. You do not need to set them locally.

| Variable | Value | Description |
|---|---|---|
| `DD_API_KEY` | (from CloudFormation parameter) | Datadog API key |
| `DD_SITE` | `datadoghq.com` | Datadog ingest endpoint |
| `DD_ENV` | `prod` | Datadog environment tag |
| `DD_SERVICE` | `mbta-performance` | Datadog service name |
| `DD_TRACE_ENABLED` | `true` | Enable Datadog APM tracing |
| `DD_PROFILING_ENABLED` | `true` | Enable Datadog continuous profiling |
| `DD_VERSION` | (git describe output) | Deployed version, for Datadog release tracking |
| `DD_TAGS` | (git SHA + repo URL) | Additional Datadog tags |

---

## AWS resources

| Resource | Name | Description |
|---|---|---|
| S3 bucket | `tm-mbta-performance` | Primary output bucket for processed event CSVs |
| S3 bucket | `tm-gtfs` | GTFS feed cache — SQLite files built from the LAMP GTFS archive |
| S3 bucket | `mbta-performance-lambda-deployments` | Holds Chalice deployment artifacts (layer zips, packaged templates) |
| CloudFormation stack | `mbta-performance` | Owns both Lambda functions and their IAM roles |

---

## Lambda configuration

Both deployed functions share the same settings:

| Setting | Value |
|---|---|
| Runtime | Python 3.12 |
| Memory | 2048 MB |
| Ephemeral storage (`/tmp`) | 2048 MB |
| Timeout | 250 seconds |
| IAM policy file | `.chalice/policy-lamp-ingest.json` |

---

## Chalice stages

Only one stage (`prod`) is defined in `.chalice/config.json`. There is no separate staging environment — test locally, then deploy to prod.
