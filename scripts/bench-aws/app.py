#!/usr/bin/env python3
"""CDK app — TRAINS AWS benchmark infrastructure.

Deploy:
    cdk deploy --all --require-approval never

Destroy:
    cdk destroy --all --force

Region: eu-west-3 (CDG Paris), 3 AZs.
"""
import os

import aws_cdk as cdk

from trains_bench.stacks.network import TrainsBenchNetworkStack
from trains_bench.stacks.compute import TrainsBenchComputeStack

app = cdk.App()

# Account is required for the VPC AZ lookup (otherwise CDK uses dummy
# context with only 2 AZs → IndexError in compute.py when MAX_NODES > 2).
# Resolved from CDK_DEFAULT_ACCOUNT, which `cdk deploy` injects from the
# caller's STS identity.
env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region="eu-west-3",
)

network = TrainsBenchNetworkStack(
    app,
    "TrainsBenchNetwork",
    env=env,
    description="TRAINS benchmark - VPC + security groups (eu-west-3)",
)

TrainsBenchComputeStack(
    app,
    "TrainsBenchCompute",
    network=network,
    env=env,
    description="TRAINS benchmark - 15 EC2 nodes + S3 results bucket",
)

app.synth()
