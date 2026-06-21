#!/usr/bin/env python3
"""Pre-deploy CDK lints for trains-bench-aws.

Catches the failure classes empirically encountered during the 2026-05-26
morning EC2 chaos deploy attempt (PR-RD-5b, six latent bugs bundled into
one fix). Run as a gate before `cdk synth` so the operator gets a fast,
specific failure with a fix suggestion instead of a 5-10 minute
`cdk deploy` that rolls back at CloudFormation's first resource error.

Lint catalogue:

  no-emdash       em-dash / non-Latin-1 char in `description="..."`
                  → IAM rejects the resource with a 400 regex-mismatch.
                  (PR-RD-5b bug class #4)

  env-account     CDK Stack instantiated without `env=Environment(account=...)`
                  → CDK falls back to a 2-AZ dummy context and downstream
                    `subnet_list[2]` raises IndexError at synth time.
                  (PR-RD-5b bug class #1)

  sg-self-ref     SecurityGroupIngress that references its own group via
                  CDK tokens before the group is fully resolved can produce
                  a CloudFormation circular dependency. Detect the AS-WRITTEN
                  pattern; the operator's mitigation is to attach the rule
                  after construction (CfnSecurityGroupIngress with explicit
                  GroupId).
                  (PR-RD-5b bug class #3)

  required-ports  Verify all ports the bench needs are opened by some
                  ingress rule. Missing = silent failure at runtime
                  (Sentinel can't form quorum without 26379, ring TLS
                  can't connect without 7000, etc.)
                  (PR-RD-5b bug class #6)

Method comes from AgentOrchestrator's `backend/app/skills/builtin/_cdk_lints.py`
(which catches a different set of failure classes for AO's ECS/ECR/agent
stack — secret-refs, container-image, ecr-existence). We adapted the
LintFinding shape and the em-dash regex; the TRAINS-specific lints are net new.

Run with:
    python3 scripts/bench-aws/cdk_lints.py        # lints the bench-aws stacks
    python3 scripts/bench-aws/cdk_lints.py PATH   # lints a specific dir

Exits 0 on clean, 1 on any finding (so it can gate `cdk synth` in CI).
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path


# ── Generic patterns ─────────────────────────────────────────────────────────

# IAM accepts only Latin-1 printable + tab/LF/CR in description fields.
# The IAM regex per AWS docs is: [\t\n\r\x20-\x7E\xA1-\xFF]*
# This pattern detects ANY character outside that set.
_BAD_IAM_CHAR = re.compile("[^\t\n\r\x20-\x7e\xa1-\xff]")

# Matches `description="..."` or `description='...'` (Python CDK kwarg form).
# Stops at the matching quote; the value is whatever's between.
_DESC_LINE = re.compile(r"""description\s*=\s*(['"])(.*?)\1""")

# Matches `cdk.App()` — the entrypoint marker. We only run env-account on
# files that instantiate a CDK app, because that's where env= must be set.
# Stack-defining files (compute.py, network.py) receive env via **kwargs
# from app.py and shouldn't trigger the lint.
_CDK_APP_INSTANTIATION = re.compile(r"\bcdk\.App\s*\(")

# Matches `env=` kwarg with an Environment that has an account=
_ENV_WITH_ACCOUNT = re.compile(r"env\s*=\s*cdk\.Environment\([^)]*account\s*=")

# Matches `add_ingress_rule` on a SecurityGroup where the peer is the SG itself
# (a common shape: `sg.add_ingress_rule(peer=sg.security_group_id, ...)` or
# `Peer.security_group_id(sg.security_group_id)`).
_SG_SELF_REF = re.compile(
    r"add_ingress_rule\s*\([^)]*?peer\s*=\s*[^)]*?security_group_id"
)

# Match port specifications: ec2.Port.tcp(N) or Port(N)
_PORT_TCP = re.compile(r"ec2\.Port\.tcp\s*\(\s*(\d+)\s*\)")
_PORT_RANGE = re.compile(r"ec2\.Port\.tcp_range\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)")
# A port-defining tuple-list iteration pattern:
#   for port, desc in [(7000, "..."), (9000, "..."), ...]:
# We scan tuples of (int, "...") and treat them as port definitions when the
# file otherwise drives ec2.Port.tcp(port) in a loop body.
_TUPLE_PORT = re.compile(r"\(\s*(\d{2,5})\s*,\s*['\"]")
_LOOP_VAR_PORT_TCP = re.compile(r"ec2\.Port\.tcp\s*\(\s*\w+\s*\)")
# Anchor: only run required-ports on files that ACTUALLY define an SG
# (consumes-SG files like compute.py don't open ports themselves).
_SG_DEFINITION = re.compile(r"ec2\.SecurityGroup\s*\(")


# ── Lint primitives ──────────────────────────────────────────────────────────


@dataclass
class LintFinding:
    """One actionable failure-class reported by a lint.

    Attributes:
        rule: Stable lint identifier (e.g. ``"no-emdash"``). Stable so reviewers
            can grep on it to suppress / route findings.
        line: 1-based line number of the offending statement, or 0 for
            file-level findings.
        file: Path of the source file (relative to the lint root or absolute,
            whatever the caller passed in).
        message: One-line human-readable description of what's wrong.
        suggestion: The canonical fix the operator should apply.
    """

    rule: str
    line: int
    file: Path
    message: str
    suggestion: str

    def format(self) -> str:
        loc = f"{self.file}:{self.line}" if self.line else str(self.file)
        return (
            f"  [{self.rule}] {loc}\n"
            f"    {self.message}\n"
            f"    fix: {self.suggestion}"
        )


# ── Individual lints ─────────────────────────────────────────────────────────


def lint_no_emdash(source: str, path: Path) -> list[LintFinding]:
    """Catch em-dash / non-Latin-1 characters in IAM/SG ``description=...``.

    IAM's Role/Policy ``Description`` field validates against the regex
    ``[\\t\\n\\r\\x20-\\x7E\\xA1-\\xFF]*`` and rejects em-dashes (U+2014),
    smart quotes, emoji, etc. SecurityGroup descriptions reject non-ASCII
    too. CloudFormation surfaces this as a 400 validation error at the
    first IAM/EC2-creating resource.

    Only ``description=`` lines are scanned -- other strings (comments,
    bucket names, etc.) are allowed any character.

    Adapted from AO's ``backend/app/skills/builtin/_cdk_lints.py::lint_no_emdash``.
    """
    findings: list[LintFinding] = []
    for lineno, line in enumerate(source.splitlines(), start=1):
        match = _DESC_LINE.search(line)
        if not match:
            continue
        value = match.group(2)
        bad = _BAD_IAM_CHAR.findall(value)
        if not bad:
            continue
        offenders = sorted(set(bad))
        codepoints = ", ".join(f"U+{ord(c):04X}" for c in offenders)
        findings.append(
            LintFinding(
                rule="no-emdash",
                line=lineno,
                file=path,
                message=(
                    f"description=... contains non-Latin-1 characters: "
                    f"{offenders} ({codepoints})"
                ),
                suggestion=(
                    "Replace with hyphens '-' or other Latin-1 characters. "
                    "IAM rejects the resource at create time with 'Member "
                    "must satisfy regular expression pattern...'."
                ),
            )
        )
    return findings


def lint_env_account(source: str, path: Path) -> list[LintFinding]:
    """Flag CDK Stacks instantiated without ``env=Environment(account=...)``.

    Without ``account=`` set, CDK falls back to its dummy 2-AZ context.
    Code that does ``subnet_list[2]`` (e.g., spreading nodes across 3 AZs)
    then raises IndexError at synth time, AFTER several seconds of CDK
    boot. The operator-facing diagnosis is "synth crashed inexplicably";
    the real cause is the missing account.

    The lint is targeted at the CDK app entrypoint (file containing
    ``cdk.App()``) rather than stack-defining files. Stack-defining
    modules receive env via ``**kwargs`` from the entrypoint and aren't
    where the account belongs.
    """
    if not _CDK_APP_INSTANTIATION.search(source):
        return []
    if _ENV_WITH_ACCOUNT.search(source):
        return []
    return [
        LintFinding(
            rule="env-account",
            line=0,
            file=path,
            message=(
                "Stack is instantiated without env=cdk.Environment(account=...). "
                "CDK falls back to a dummy 2-AZ context; subnet_list[2] panics "
                "at synth."
            ),
            suggestion=(
                "Pass env=cdk.Environment(account=os.environ['CDK_DEFAULT_ACCOUNT'], "
                "region=os.environ['CDK_DEFAULT_REGION']) at construct time."
            ),
        )
    ]


def lint_sg_self_ref(source: str, path: Path) -> list[LintFinding]:
    """Flag SecurityGroupIngress with a peer referencing its own group.

    Self-referencing SG rules in a single L2 construct can produce a
    CloudFormation circular dependency: the SG depends on the rule,
    the rule depends on the SG's ``GroupId``. CDK's L2 abstraction
    sometimes hides this; the operator sees ``CIRCULAR_DEPENDENCY``
    at synth.

    Canonical fix: create the SG first, then attach the rule via a
    ``CfnSecurityGroupIngress`` with an explicit ``GroupId`` and
    ``SourceSecurityGroupId`` (both resolved as tokens, not as part
    of the same construct).
    """
    findings: list[LintFinding] = []
    for lineno, line in enumerate(source.splitlines(), start=1):
        if _SG_SELF_REF.search(line):
            findings.append(
                LintFinding(
                    rule="sg-self-ref",
                    line=lineno,
                    file=path,
                    message=(
                        "SG ingress rule references its own security_group_id "
                        "in the same construct; can produce CloudFormation "
                        "CIRCULAR_DEPENDENCY at synth."
                    ),
                    suggestion=(
                        "Create the SG first, then attach with "
                        "ec2.CfnSecurityGroupIngress(self, ..., "
                        "group_id=sg.security_group_id, "
                        "source_security_group_id=sg.security_group_id)."
                    ),
                )
            )
    return findings


# Ports the bench requires open within the ring SG. Numbers chosen to match
# the existing trains-valkey bench:
#   6379  — Valkey RESP backend on each node (loopback or within-SG)
#   7000  — trains-valkey proxy ring TLS listener (PR-RD-5b restored this)
#   9000  — TRAINS protocol legacy port (some operators still set this)
#   26379 — Valkey Sentinel (added 2026-05-27 PM for E4-clean)
REQUIRED_BENCH_PORTS = (6379, 7000, 9000, 26379)


def lint_required_ports(source: str, path: Path) -> list[LintFinding]:
    """Verify all REQUIRED_BENCH_PORTS appear in some ec2.Port.tcp(...) call.

    A missing port is a silent runtime failure: ring TLS can't connect
    without 7000, Sentinel can't form quorum without 26379, etc. CDK
    deploys cleanly with these missing because the SG/CFN is valid.

    We only run this lint on files that actually CREATE a SecurityGroup
    (heuristic: source contains ``ec2.SecurityGroup(``). Files that only
    consume an SG (compute.py via ``security_group=network.ring_sg``)
    are out of scope.

    We extract ports from three patterns:
        ec2.Port.tcp(<INT>)        — direct, common case
        ec2.Port.tcp_range(LO,HI)  — direct, ranges
        for port, desc in [(<INT>, "..."), ...]:  — loop-over-tuples (the
                                    style used by network.py); we treat
                                    every (INT, "...") tuple as a port
                                    when a ``ec2.Port.tcp(VAR)`` call
                                    appears in the same source.
    """
    if not _SG_DEFINITION.search(source):
        return []
    seen: set[int] = set()
    for m in _PORT_TCP.finditer(source):
        seen.add(int(m.group(1)))
    for m in _PORT_RANGE.finditer(source):
        lo, hi = int(m.group(1)), int(m.group(2))
        for p in range(lo, hi + 1):
            seen.add(p)
    # Tuple-driven loop pattern: harvest (INT, "...") tuples when a
    # ec2.Port.tcp(VARIABLE) call exists somewhere in the file.
    if _LOOP_VAR_PORT_TCP.search(source):
        for m in _TUPLE_PORT.finditer(source):
            seen.add(int(m.group(1)))
    missing = [p for p in REQUIRED_BENCH_PORTS if p not in seen]
    if not missing:
        return []
    return [
        LintFinding(
            rule="required-ports",
            line=0,
            file=path,
            message=(
                f"SG-defining file is missing bench-required ports {missing} "
                f"(found: {sorted(seen)})"
            ),
            suggestion=(
                "Add an ingress rule per missing port within the ring SG: "
                "ring_sg.add_ingress_rule(peer=ec2.Peer.security_group_id("
                "ring_sg.security_group_id), connection=ec2.Port.tcp(PORT), "
                "description='ASCII-only description')."
            ),
        )
    ]


# ── Driver ───────────────────────────────────────────────────────────────────


ALL_LINTS = (
    lint_no_emdash,
    lint_env_account,
    lint_sg_self_ref,
    lint_required_ports,
)


def run_all_lints(root: Path) -> list[LintFinding]:
    """Run every lint over every ``*.py`` under ``root``.

    Returns a flat list of findings. Empty list = clean.

    The lint module itself is skipped — its regexes match its own docstring
    examples and would produce false positives.
    """
    findings: list[LintFinding] = []
    self_path = Path(__file__).resolve()
    for path in sorted(root.rglob("*.py")):
        # Skip cdk.out / .venv / __pycache__ etc.
        if any(part in {"cdk.out", ".venv", "__pycache__", "node_modules"} for part in path.parts):
            continue
        # Skip the lint module itself.
        if path.resolve() == self_path:
            continue
        try:
            source = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            # If the file isn't UTF-8, it almost certainly has non-Latin-1 chars too.
            findings.append(
                LintFinding(
                    rule="no-emdash",
                    line=0,
                    file=path,
                    message="file is not UTF-8 decodable (likely contains non-Latin-1 chars)",
                    suggestion="Re-encode the file as UTF-8 with ASCII-only descriptions.",
                )
            )
            continue
        for lint in ALL_LINTS:
            findings.extend(lint(source, path))
    return findings


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
    print(f"cdk_lints: scanning {root}")
    findings = run_all_lints(root)
    if not findings:
        print("cdk_lints: clean")
        return 0
    print(f"cdk_lints: {len(findings)} finding(s):")
    for f in findings:
        print(f.format())
    return 1


if __name__ == "__main__":
    sys.exit(main())
