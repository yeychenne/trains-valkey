#!/usr/bin/env bash
# Provision, execute, collect, and destroy the one-node Arctic EC2-A1 run.

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
AWS_PROFILE="${AWS_PROFILE:-devops-admin}"
AWS_REGION="${AWS_REGION:-eu-west-3}"
MAX_RUN_HOURS="${MAX_RUN_HOURS:-6}"
MAX_BUDGET_USD="${MAX_BUDGET_USD:-60}"
CDK_CLI_VERSION="${CDK_CLI_VERSION:-2.1135.1}"
RESULTS_DIR="${RESULTS_DIR:-$REPO/bench/results/ec2-arctic-a1}"
OUTPUTS="$SCRIPT_DIR/arctic-cdk-outputs.json"
export AWS_PROFILE AWS_REGION CDK_DEFAULT_REGION="$AWS_REGION"

cdk_args=(
    -c perfMode=true
    -c "maxRunHours=$MAX_RUN_HOURS"
    -c "maxBudgetUsd=$MAX_BUDGET_USD"
)

cdk_exec() {
    npx --yes "aws-cdk@$CDK_CLI_VERSION" "$@"
}

activate_cdk_python() {
    test -x "$SCRIPT_DIR/.venv/bin/python" || {
        echo "Missing CDK virtualenv; run '$0 preflight' first." >&2
        return 1
    }
    export PATH="$SCRIPT_DIR/.venv/bin:$PATH"
}

account_id() {
    aws sts get-caller-identity --query Account --output text
}

output_value() {
    local key=$1
    jq -r --arg key "$key" '[.[] | .[$key] // empty][0]' "$OUTPUTS"
}

hourly_price() {
    aws pricing get-products \
        --region us-east-1 \
        --service-code AmazonEC2 \
        --filters \
            Type=TERM_MATCH,Field=instanceType,Value=c7g.2xlarge \
            'Type=TERM_MATCH,Field=location,Value=EU (Paris)' \
            Type=TERM_MATCH,Field=operatingSystem,Value=Linux \
            Type=TERM_MATCH,Field=tenancy,Value=Shared \
            Type=TERM_MATCH,Field=preInstalledSw,Value=NA \
            Type=TERM_MATCH,Field=capacitystatus,Value=Used \
        --query PriceList --output json \
        | jq -r '[.[0] | fromjson | .terms.OnDemand[] | .priceDimensions[] | .pricePerUnit.USD][0]'
}

preflight() {
    echo "=== AWS and source preflight ==="
    CDK_DEFAULT_ACCOUNT="$(account_id)"
    export CDK_DEFAULT_ACCOUNT
    echo "Account: $CDK_DEFAULT_ACCOUNT; region: $AWS_REGION; profile: $AWS_PROFILE"

    test -z "$(git -C "$REPO" status --porcelain)" || {
        echo "Refusing to benchmark a dirty worktree; commit and push first." >&2
        return 1
    }
    local sha remote_sha price maximum
    sha="$(git -C "$REPO" rev-parse HEAD)"
    remote_sha="$(git -C "$REPO" ls-remote origin "refs/heads/$(git -C "$REPO" branch --show-current)" | awk '{print $1}')"
    test "$sha" = "$remote_sha" || {
        echo "HEAD $sha is not the pushed branch tip ($remote_sha)." >&2
        return 1
    }

    aws ec2 describe-instance-type-offerings \
        --location-type availability-zone \
        --filters Name=instance-type,Values=c7g.2xlarge \
        --query "InstanceTypeOfferings[?Location=='eu-west-3c'].InstanceType" \
        --output text | grep -q c7g.2xlarge
    price="$(hourly_price)"
    [[ "$price" =~ ^[0-9]+([.][0-9]+)?$ ]] || {
        echo "Could not resolve a numeric c7g.2xlarge price: '$price'." >&2
        return 1
    }
    maximum="$(awk -v price="$price" -v hours="$MAX_RUN_HOURS" 'BEGIN { printf "%.4f", price * hours }')"
    awk -v maximum="$maximum" -v budget="$MAX_BUDGET_USD" \
        'BEGIN { exit !(maximum < budget) }' || {
        echo "Six-hour compute bound \$$maximum exceeds budget \$$MAX_BUDGET_USD." >&2
        return 1
    }
    echo "On-Demand price: \$$price/hour; compute bound: \$$maximum; budget: \$$MAX_BUDGET_USD"

    python3 "$SCRIPT_DIR/cdk_lints.py"
    cd "$SCRIPT_DIR"
    if [ ! -x .venv/bin/python ]; then
        python3 -m venv .venv
    fi
    .venv/bin/pip install -q -r requirements.txt
    activate_cdk_python
    cdk_exec synth "${cdk_args[@]}" >/dev/null
    echo "Preflight passed for commit $sha"
}

deploy() {
    echo "=== Deploy one c7g.2xlarge ==="
    activate_cdk_python
    export CDK_DEFAULT_ACCOUNT="$(account_id)"
    cd "$SCRIPT_DIR"
    cdk_exec deploy --all --require-approval never "${cdk_args[@]}" --outputs-file "$OUTPUTS"
    test "$(output_value PerfMode)" = true
    test "$(output_value MaxNodes)" = 1

    local bucket
    bucket="$(output_value BenchBucketName)"
    aws s3 cp "$SCRIPT_DIR/arctic-ec2-a1-remote.sh" \
        "s3://$bucket/control/arctic-ec2-a1-remote.sh"
}

wait_for_ssm() {
    local instance_id=$1
    for _ in $(seq 1 60); do
        if [ "$(aws ssm describe-instance-information \
            --filters "Key=InstanceIds,Values=$instance_id" \
            --query 'InstanceInformationList[0].PingStatus' --output text)" = Online ]; then
            return 0
        fi
        sleep 10
    done
    echo "Instance $instance_id did not become SSM-online within 10 minutes." >&2
    return 1
}

run_remote() {
    echo "=== Execute EC2-A1 through SSM ==="
    local bucket instance_id sha parameters command_id status
    bucket="$(output_value BenchBucketName)"
    instance_id="$(output_value InstanceId00)"
    sha="$(git -C "$REPO" rev-parse HEAD)"
    wait_for_ssm "$instance_id"

    parameters="$(jq -nc --arg bucket "$bucket" --arg sha "$sha" --arg region "$AWS_REGION" '{commands:[
      "aws s3 cp s3://" + $bucket + "/control/arctic-ec2-a1-remote.sh /opt/trains/arctic-ec2-a1-remote.sh",
      "chmod 0755 /opt/trains/arctic-ec2-a1-remote.sh",
      "sudo env GIT_SHA=" + $sha + " BUCKET=" + $bucket + " AWS_REGION=" + $region + " bash /opt/trains/arctic-ec2-a1-remote.sh"
    ]}')"
    command_id="$(aws ssm send-command \
        --instance-ids "$instance_id" \
        --document-name AWS-RunShellScript \
        --comment "TRAINS Arctic EC2-A1 $sha" \
        --timeout-seconds 14400 \
        --parameters "$parameters" \
        --query Command.CommandId --output text)"
    echo "SSM command: $command_id"

    while true; do
        status="$(aws ssm get-command-invocation --command-id "$command_id" \
            --instance-id "$instance_id" --query Status --output text 2>/dev/null || echo Pending)"
        echo "$(date +%H:%M:%S) $status"
        case "$status" in
            Success) break ;;
            Failed|Cancelled|TimedOut)
                aws ssm get-command-invocation --command-id "$command_id" \
                    --instance-id "$instance_id" --output json || true
                return 1
                ;;
        esac
        sleep 30
    done
}

collect() {
    echo "=== Collect immutable raw evidence ==="
    local bucket sha destination
    bucket="$(output_value BenchBucketName)"
    sha="$(git -C "$REPO" rev-parse HEAD)"
    destination="$RESULTS_DIR/$sha"
    mkdir -p "$destination"
    aws s3 sync "s3://$bucket/arctic-ec2-a1/$sha" "$destination"
    test "$(jq -r .status "$destination/status.json")" = complete
    python3 "$SCRIPT_DIR/arctic-ec2-a1-analyze.py" "$destination"
    echo "Results: $destination"
}

teardown() {
    echo "=== Destroy all experiment resources ==="
    activate_cdk_python
    export CDK_DEFAULT_ACCOUNT="$(account_id)"
    local account bucket logs
    account="$CDK_DEFAULT_ACCOUNT"
    bucket="trains-bench-${account}-${AWS_REGION}"
    logs="trains-bench-logs-${account}-${AWS_REGION}"
    aws s3 rm "s3://$bucket" --recursive --quiet 2>/dev/null || true
    aws s3 rm "s3://$logs" --recursive --quiet 2>/dev/null || true
    cd "$SCRIPT_DIR"
    cdk_exec destroy --all --force "${cdk_args[@]}" || {
        aws s3 rm "s3://$bucket" --recursive --quiet 2>/dev/null || true
        aws s3 rm "s3://$logs" --recursive --quiet 2>/dev/null || true
        aws cloudformation delete-stack --stack-name TrainsBenchCompute
        aws cloudformation wait stack-delete-complete --stack-name TrainsBenchCompute
        aws cloudformation delete-stack --stack-name TrainsBenchNetwork
        aws cloudformation wait stack-delete-complete --stack-name TrainsBenchNetwork
    }
    test "$(aws ec2 describe-instances \
        --filters Name=tag:Experiment,Values=arctic-ec2-a1 \
                  Name=instance-state-name,Values=pending,running,stopping,stopped \
        --query 'Reservations[].Instances[]' --output json)" = '[]'
    echo "Teardown verified. No Arctic EC2-A1 instances remain."
}

all() {
    preflight
    trap 'teardown' EXIT
    deploy
    run_remote
    collect
    teardown
    trap - EXIT
}

case "${1:-}" in
    preflight) preflight ;;
    deploy) deploy ;;
    run) run_remote ;;
    collect) collect ;;
    teardown) teardown ;;
    all) all ;;
    *) echo "usage: $0 {preflight|deploy|run|collect|teardown|all}" >&2; exit 2 ;;
esac
