import json
import os
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
)
from constructs import Construct

class AgentsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Load configuration files
        with open('config/parameters.json', 'r') as f:
            params = json.load(f)

        # ===============Create Lambda Layer for Polars dependencies================
        polars_layer = _lambda.LayerVersion(self, "PolarsLayer",
            code=_lambda.Code.from_asset("../layers/polars-layer",
                bundling={
                    "image": _lambda.Runtime.PYTHON_3_13.bundling_image,
                    "command": [
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output/python/"
                    ]
                }
            ),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_13],
            description="Layer containing polars for data analysis",
        )

        # ===============Create Lambda Function for DS Explorer Agent================
        ds_explorer_lambda = _lambda.Function(self, "DSExplorerLambda",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="lambda-handler.lambda_handler",
            code=_lambda.Code.from_asset("../agents/ds-explorer"),
            layers=[polars_layer],
            timeout=Duration.minutes(15),
            memory_size=1024,
            environment={
                "LOG_LEVEL": "INFO"
            }
        )