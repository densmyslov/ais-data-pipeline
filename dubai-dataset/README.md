
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

