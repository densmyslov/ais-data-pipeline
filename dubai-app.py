#!/usr/bin/env python3
import os
import sys
import aws_cdk as cdk
sys.path.append('dubai-dataset')
from dubai_dataset.dubai_dataset_stack import DubaiDatasetStack

app = cdk.App()
DubaiDatasetStack(app, "DubaiDatasetStack")
app.synth()