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
from aws_cdk import aws_ec2 as ec2

from trains_bench.stacks.network import TrainsBenchNetworkStack
from trains_bench.stacks.compute import TrainsBenchComputeStack

app = cdk.App()

perf_mode = str(app.node.try_get_context("perfMode") or "false").lower() == "true"
max_run_hours = int(app.node.try_get_context("maxRunHours") or 6)
max_budget_usd = str(app.node.try_get_context("maxBudgetUsd") or "60")

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

compute = TrainsBenchComputeStack(
    app,
    "TrainsBenchCompute",
    network=network,
    max_nodes=1 if perf_mode else 5,
    instance_type=(
        ec2.InstanceType("c7g.2xlarge")
        if perf_mode
        else ec2.InstanceType("t4g.small")
    ),
    availability_zones=["eu-west-3c"] if perf_mode else ["eu-west-3a", "eu-west-3c"],
    perf_mode=perf_mode,
    max_run_hours=max_run_hours,
    env=env,
    description=(
        "TRAINS Arctic data-plane benchmark - one ARM64 performance node + S3"
        if perf_mode
        else "TRAINS benchmark - 5 EC2 nodes + S3 results bucket"
    ),
)

for stack in (network, compute):
    cdk.Tags.of(stack).add("Project", "trains-bench")
    if perf_mode:
        cdk.Tags.of(stack).add("Experiment", "arctic-ec2-a1")
        cdk.Tags.of(stack).add("MaxBudgetUSD", max_budget_usd)

app.synth()
