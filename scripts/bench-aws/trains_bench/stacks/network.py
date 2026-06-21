"""TRAINS Bench — Network stack (VPC + Security Groups).

Three AZs in eu-west-3. Public subnets only (EC2 instances need outbound
S3 + SSM access; public IPs avoid NAT Gateway cost for a short-lived bench).

Security group allows all TCP within the SG so ring nodes can reach each
other on port 9000 regardless of their position in the ring.
"""
from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from constructs import Construct

# eu-west-3 has exactly 3 AZs; use all of them for the bench.
TARGET_AZS = ["eu-west-3a", "eu-west-3b", "eu-west-3c"]


class TrainsBenchNetworkStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(
            self,
            "BenchVpc",
            vpc_name="trains-bench-vpc",
            max_azs=3,
            nat_gateways=0,  # public subnets only — no NAT cost
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                    map_public_ip_on_launch=True,
                ),
            ],
        )

        # Ring security group: all TCP within the group (covers port 9000
        # used by every node) + outbound for S3/SSM VPC endpoints or
        # internet egress.
        self.ring_sg = ec2.SecurityGroup(
            self,
            "RingSg",
            security_group_name="trains-ring-sg",
            vpc=self.vpc,
            description="TRAINS ring nodes - inter-node TCP for ring + engine (ASCII only)",
            allow_all_outbound=True,
        )
        # Self-referencing ingress — use `connections.allow_from(self, ...)`
        # instead of `add_ingress_rule(Peer.security_group_id(self.id), ...)`.
        # The latter inlines the rule into the SG resource and creates a
        # circular Ref in CloudFormation; `allow_from` emits a separate
        # AWS::EC2::SecurityGroupIngress with the proper dependency.
        # Ports opened (intra-SG only - never public):
        #   7000   TRAINS ring TLS (proxy to proxy, PR-RD-1..4)
        #   7001   TRAINS state-transfer (rejoin snapshot+tail, PR-RJ-3b/3c).
        #          Without this the passive rejoiner's fetch_state to a survivor
        #          is silently dropped and a restarted node never reconverges —
        #          the E5 t1-rejoin failure seen live on 2026-06-16 before this
        #          port was opened.
        #   9000   legacy trains-cli ring (kept for benchmark sweeps)
        #   6379   Valkey engine (chaos verifier reads survivor state across nodes)
        #   26379  Valkey Sentinel quorum (E4 sweep, added 2026-05-27)
        for port, desc in [
            (7000, "TRAINS-redis ring TLS"),
            (7001, "TRAINS-redis state transfer (rejoin, PR-RJ-3b/3c)"),
            (9000, "trains-cli ring"),
            (6379, "Valkey engine (chaos verifier, intra-SG only)"),
            (26379, "Valkey Sentinel quorum (E4 sweep)"),
        ]:
            self.ring_sg.connections.allow_from(
                self.ring_sg, ec2.Port.tcp(port), desc
            )

        cdk.CfnOutput(self, "VpcId", value=self.vpc.vpc_id)
        cdk.CfnOutput(
            self,
            "RingSgId",
            value=self.ring_sg.security_group_id,
            export_name="TrainsBench-RingSgId",
        )
