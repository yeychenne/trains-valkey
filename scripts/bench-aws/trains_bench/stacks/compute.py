"""TRAINS Bench — Compute stack (EC2 fleet + IAM + S3).

Provisions MAX_NODES t3.small instances spread evenly across the three
eu-west-3 AZs.  The coordinator script selects a subset (3, 10, or 15)
for each ring-size benchmark pass.

Every instance:
  - Gets an instance profile with SSMFullAccess + the bench S3 bucket
    read/write permissions (so the coordinator can send commands without
    SSH keys and nodes can upload results).
  - Runs a tiny user-data script that installs nothing — the coordinator
    uploads binaries via `ssm:send-command` + `aws s3 cp`.
"""
from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct

from trains_bench.stacks.network import TrainsBenchNetworkStack

MAX_NODES = 3
# t4g.small = 2 vCPU / 2 GiB ARM Graviton, ~$0.0168/hr in eu-west-3.
# Switched from t3.small (x86_64) on 2026-05-26 to match the arm64-Mac build
# host — avoids a cross-compile dependency and is slightly cheaper.
INSTANCE_TYPE = ec2.InstanceType("t4g.small")
# eu-west-3b returned `InsufficientInstanceCapacity` for t4g.small on 2026-05-26;
# AWS told us to use 3a/3c. Three nodes round-robin across 2 AZs (2 in 3a, 1 in
# 3c) — still multi-AZ enough to exercise cross-AZ ring latency for the chaos
# run; widen to 3 AZs if capacity returns.
AZS = ["eu-west-3a", "eu-west-3c"]


class TrainsBenchComputeStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        network: TrainsBenchNetworkStack,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── S3 access-log bucket (forensics for the bench bucket) ──────────
        # Companion bucket that records every read/write against the bench
        # bucket. Forensics use only — `s3:ServerAccessLog` writes happen
        # automatically as a result of `server_access_logs_bucket` below.
        # Kept separate from the bench bucket so the data plane and the
        # audit plane never share a write principal.
        # PR-SEC-D (R-03 partial): T-tr-11 + T-tr-15 forensics.
        self.bench_logs_bucket = s3.Bucket(
            self,
            "BenchAccessLogs",
            bucket_name=f"trains-bench-logs-{self.account}-{self.region}",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            # ACLs OBJECT_WRITER is the format AWS uses for server access logs;
            # leaving the default `BUCKET_OWNER_PREFERRED` lets the log
            # delivery group write objects that the bucket owner controls.
            lifecycle_rules=[
                # 90 days is enough audit horizon for a bench that lives
                # < 1 day per deploy.
                s3.LifecycleRule(expiration=cdk.Duration.days(90)),
            ],
        )

        # ── S3 bucket (binary upload + results) ──────────────────────────
        # PR-SEC-D (R-03 partial): added `versioned=True` and routed access
        # logs to `BenchAccessLogs`. The TM's full R-03 (deny PutObject on
        # `bin/*` except the coordinator role + Object Lock) requires
        # splitting the single shared `BenchInstanceRole` into separate
        # coordinator + node roles AND giving up `auto_delete_objects=True`
        # (Object Lock retention blocks deletion) — both are v1.1 follow-ups.
        # Versioning alone gives a recovery path against accidental or
        # malicious overwrite of `bin/trains` between coordinator upload and
        # ring-node SSM fetch: the old object version is still retrievable
        # by version-id even after a poisoned overwrite.
        self.bench_bucket = s3.Bucket(
            self,
            "BenchBucket",
            bucket_name=f"trains-bench-{self.account}-{self.region}",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            server_access_logs_bucket=self.bench_logs_bucket,
            server_access_logs_prefix="bench-bucket/",
            lifecycle_rules=[
                # Auto-expire results after 30 days.
                s3.LifecycleRule(expiration=cdk.Duration.days(30)),
                # Cap non-current version retention so versioning doesn't
                # accumulate cost. 7 days is more than enough to recover a
                # poisoned `bin/trains` swap (forensics window > MTTD).
                s3.LifecycleRule(
                    noncurrent_version_expiration=cdk.Duration.days(7),
                ),
            ],
        )

        # ── IAM role for EC2 instances ────────────────────────────────────
        role = iam.Role(
            self,
            "BenchInstanceRole",
            role_name="trains-bench-instance-role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
        )
        self.bench_bucket.grant_read_write(role)

        instance_profile = iam.CfnInstanceProfile(
            self,
            "BenchInstanceProfile",
            instance_profile_name="trains-bench-instance-profile",
            roles=[role.role_name],
        )

        # ── AMI — Amazon Linux 2023 ARM64 (matches arm64 build host) ──────
        ami = ec2.MachineImage.latest_amazon_linux2023(
            cpu_type=ec2.AmazonLinuxCpuType.ARM_64,
        )

        # ── User data — minimal setup; coordinator delivers the binary ─────
        user_data = ec2.UserData.for_linux()
        user_data.add_commands(
            "yum install -y awscli",
            "mkdir -p /opt/trains",
        )

        # ── Provision MAX_NODES instances, round-robin across AZs ──────────
        # Filter subnets to the AZs in our allowlist (e.g. drop eu-west-3b if
        # capacity-constrained). CDK orders `subnets.subnets` in VPC-creation
        # order; we re-index by AZ name so the round-robin only ever picks an
        # allowed AZ.
        all_subnets = network.vpc.select_subnets(
            subnet_type=ec2.SubnetType.PUBLIC
        ).subnets
        subnet_list = [s for s in all_subnets if s.availability_zone in AZS]
        if not subnet_list:
            raise RuntimeError(
                f"No PUBLIC subnets in allowed AZs {AZS}; "
                f"VPC has {[s.availability_zone for s in all_subnets]}"
            )

        self.instance_ids: list[str] = []
        self.private_ips: list[str] = []

        for i in range(MAX_NODES):
            az_idx = i % len(subnet_list)
            subnet = subnet_list[az_idx]

            inst = ec2.Instance(
                self,
                f"Node{i:02d}",
                instance_type=INSTANCE_TYPE,
                machine_image=ami,
                vpc=network.vpc,
                vpc_subnets=ec2.SubnetSelection(subnets=[subnet]),
                security_group=network.ring_sg,
                role=role,
                user_data=user_data,
                instance_name=f"trains-bench-node-{i:02d}",
                require_imdsv2=True,
                # Associate a public IP (needed to reach S3 without NAT GW).
                associate_public_ip_address=True,
            )
            # Attach the pre-created instance profile (CDK instance class
            # uses role directly, but we also need the CfnInstanceProfile
            # dependency for SSM to recognise the instance).
            inst.node.add_dependency(instance_profile)

            cdk.CfnOutput(
                self,
                f"InstanceId{i:02d}",
                value=inst.instance_id,
                export_name=f"TrainsBench-InstanceId-{i:02d}",
            )
            cdk.CfnOutput(
                self,
                f"PrivateIp{i:02d}",
                value=inst.instance_private_ip,
                export_name=f"TrainsBench-PrivateIp-{i:02d}",
            )

        cdk.CfnOutput(
            self,
            "BenchBucketName",
            value=self.bench_bucket.bucket_name,
            export_name="TrainsBench-BucketName",
        )
        cdk.CfnOutput(
            self, "MaxNodes", value=str(MAX_NODES), export_name="TrainsBench-MaxNodes"
        )
