
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
| `stage` | Staging | eu-central-2 | `deploy-staging.yml` |
| Feature branches | Development | eu-north-1 | `deploy-development.yml` |

### Required GitHub Secrets

Before deployments can work, add these secrets to your GitHub repository (Settings → Secrets and variables → Actions):

- `AWS_ACCESS_KEY_ID` - Your AWS access key ID
- `AWS_SECRET_ACCESS_KEY` - Your AWS secret access key  
- `AWS_ACCOUNT_ID` - Your 12-digit AWS account ID
- `API_KEYS_JSON` - JSON string containing API keys for external services

### How Deployment Works

1. **Code Push**: Push commits to any branch
2. **Workflow Trigger**: GitHub Actions automatically selects the appropriate workflow based on branch
3. **Environment Setup**: Installs Python 3.13, Node.js 24, and AWS CDK
4. **Lambda Layers**: Builds Lambda layers for Linux (PyMuPDF, Pillow)
5. **Configuration**: Creates `api_keys.json` from GitHub secrets
6. **Validation**: Validates all JSON configuration files
7. **CDK Bootstrap**: Ensures CDK is bootstrapped in the target region
8. **CDK Deploy**: Deploys the stack to the appropriate AWS environment

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

