
# Dubai Real Estate Dataset

## Prerequisites

Before you can initialize and work with this CDK Python app, ensure you have the following installed:

- **Python 3.8 or later**: Required for CDK Python applications
- **Node.js 14.x or later**: Required for the AWS CDK CLI
- **AWS CLI**: For AWS account configuration and credentials
- **AWS CDK CLI**: Install globally with `npm install -g aws-cdk`
- **Git**: For version control

**AWS Configuration:**
- Configure AWS credentials using `aws configure` or set environment variables
- Ensure your AWS account has appropriate permissions for CDK operations
- Bootstrap your AWS environment: `cdk bootstrap aws://ACCOUNT-NUMBER/REGION`

## Configuration

The `config/` folder contains configuration files for the data pipeline:

- **`parameters.json`**: Contains non-sensitive configuration parameters such as:
  - AWS S3 bucket names for data storage
  - File URLs for remote data sources
  - Processing settings (batch sizes, timeouts)
  - Output format specifications

- **`secrets.json`**: Contains sensitive configuration data such as:
  - API keys and authentication tokens
  - Database connection strings
  - Service credentials

**Note**: The `secrets.json` file should never be committed to version control. Add it to `.gitignore` to prevent accidental commits of sensitive data.

## Deployment via GitHub Actions

This project uses automated CDK deployment through GitHub Actions workflows. Deployments are triggered based on git branches and target different AWS regions:

### Environment Mapping

| Branch | Environment | AWS Region | Workflow |
|--------|-------------|------------|----------|
| `main` | Production | us-east-2 | `deploy-production.yml` |
| Feature branches | Development | eu-north-1 | `deploy-development.yml` |

### Required GitHub Secrets

Before deployments can work, add these secrets to your GitHub repository (Settings → Secrets and variables → Actions):

- `AWS_ACCESS_KEY_ID` - Your AWS access key ID
- `AWS_SECRET_ACCESS_KEY` - Your AWS secret access key  
- `AWS_ACCOUNT_ID` - Your 12-digit AWS account ID (also used as bucket suffix)

### How Deployment Works

1. **Code Push**: Push commits to any branch
2. **Workflow Trigger**: GitHub Actions automatically selects the appropriate workflow based on branch
3. **Environment Setup**: Installs Python 3.13, Node.js 24, and AWS CDK
4. **Configuration**: Creates `secrets.json` with bucket suffix from AWS_ACCOUNT_ID
5. **Validation**: Validates all JSON configuration files
6. **CDK Bootstrap**: Ensures CDK is bootstrapped in the target region
7. **CDK Deploy**: Deploys the stack to the appropriate AWS environment

### Manual Deployment

You can also trigger deployments manually:
- Go to Actions tab in GitHub
- Select the desired workflow
- Click "Run workflow" button
- Choose the branch to deploy

### Local Development

For local development and testing:
```bash
# Install dependencies
pip install -r requirements.txt
npm install -g aws-cdk

# Validate configuration
python -c "import json; json.load(open('config/parameters.json'))"

# Deploy locally (ensure AWS credentials are configured)
cdk bootstrap  # One-time setup
cdk deploy
```

## S3 Bucket Management

The CDK stack implements intelligent S3 bucket creation with the following logic:

### Bucket Naming Convention
- **Base name**: Taken from `config/parameters.json` (`bucket_name` field)
- **Account ID**: AWS Account ID from `config/secrets.json` (`bucket_suffix` field)
- **Region**: Deployment region from `AWS_DEFAULT_REGION` environment variable
- **Final name**: `{bucket_name}-{account_id}-{region}` (e.g., `dubai-real-estate-data-123456789012-eu-north-1`)

### Import/Create Logic
The stack uses boto3 to check if the bucket already exists:

```python
# Check if bucket exists
s3_client = boto3.client('s3')
try:
    s3_client.head_bucket(Bucket=bucket_name)
    bucket_exists = True
except ClientError as e:
    if e.response['Error']['Code'] == '404':
        bucket_exists = False
```

**If bucket exists**: Imports the existing bucket using `s3.Bucket.from_bucket_name()`
**If bucket doesn't exist**: Creates a new bucket with:
- Versioning enabled
- Configurable removal policy (DESTROY for dev, RETAIN for prod)
- Auto-delete objects setting from parameters

### Configuration Deployment
After bucket creation, the stack automatically deploys `config/parameters.json` to the S3 bucket under the `config/` prefix, making configuration accessible to other AWS services.

### Regional Bucket Examples
- **Development** (eu-north-1): `dubai-real-estate-data-123456789012-eu-north-1`
- **Production** (us-east-2): `dubai-real-estate-data-123456789012-us-east-2`

This ensures global uniqueness across all AWS accounts and regions while maintaining environment separation.

