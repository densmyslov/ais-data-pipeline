#!/usr/bin/env python3
import os
import aws_cdk as cdk
from agents.agents_stack import AgentsStack

app = cdk.App()
AgentsStack(app, "AgentsStack")
app.synth()