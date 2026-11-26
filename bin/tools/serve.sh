#!/bin/bash
# RoboSystems Serve: the whole platform on one EC2 instance in your own AWS
# account, launched from cloudformation/serve.yaml and run from here.
#
#   bin/tools/serve.sh launch [<profile> <domain> <allowed-cidr|me> [stack-name]]
#   bin/tools/serve.sh status
#   bin/tools/serve.sh upgrade [<tag>|latest]
#   bin/tools/serve.sh logs <service> [lines]
#   bin/tools/serve.sh restart [service]
#   bin/tools/serve.sh reboot
#   bin/tools/serve.sh snapshot
#   bin/tools/serve.sh set <KEY> [value]
#   bin/tools/serve.sh tunnel dagster|mailpit
#   bin/tools/serve.sh shell
#
# launch asks for whatever it is not given, with the machine's AWS CLI profiles
# offered as choices, finds the account's public Route 53 hosted zone that holds
# the domain, and remembers the profile, region and stack in .local/serve.json. Every other verb acts on that stack,
# through Systems Manager: nothing on the instance listens for administration.
# The account is always the remembered or chosen profile, never the shell's own
# credentials, so a terminal signed in to another account cannot be the target.
#
# launch reads these optional environment variables:
#   HOSTED_ZONE_ID        default: the account's public zone that holds the domain
#   VPC_ID, SUBNET_ID     default: the region's default VPC and a public subnet of it
#   ROBOSYSTEMS_VERSION   default: the newest release whose image is published
#   INSTANCE_TYPE         default: the template's
#   DATA_VOLUME_SIZE      default: the template's, in GiB
#   SNAPSHOT_ID           start from a snapshot of an earlier data volume
#   ANTHROPIC_API_KEY     configure the server-side operators
#   ASSUME_YES=1          skip the confirmation
#
# The stack is yours to run. See docs/technical/Self-Hosting-on-a-Server.md.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
TEMPLATE="$ROOT/cloudformation/serve.yaml"
TARGET_FILE="$ROOT/.local/serve.json"
COMPOSE="docker compose -f compose.yaml --env-file .env --profile all"

usage() {
  sed -n '5,14s/^#   bin\/tools\/serve.sh/  just serve/p' "$0" >&2
  exit 2
}

die() {
  echo "serve: $*" >&2
  exit 1
}

# ask <prompt> [default]: one line from the terminal; the default on an empty answer.
ask() {
  local answer
  [ -t 0 ] || die "this needs an answer and there is no terminal to ask on"
  if [ -n "${2:-}" ]; then
    read -r -p "$1 [$2]: " answer
    echo "${answer:-$2}"
  else
    read -r -p "$1: " answer
    echo "$answer"
  fi
}

# choose <prompt> <option>...: a numbered list on stderr; prints the option picked.
choose() {
  local prompt=$1 answer i=1 option
  shift
  for option in "$@"; do
    echo "  $i) $option" >&2
    i=$((i + 1))
  done
  answer=$(ask "$prompt")
  case "$answer" in
  '' | *[!0-9]*) die "not a number: $answer" ;;
  esac
  [ "$answer" -ge 1 ] && [ "$answer" -le $# ] || die "no option $answer"
  echo "${!answer}"
}

confirm() {
  [ "${ASSUME_YES:-}" = "1" ] && return 0
  case "$(ask "$1 (y/N)")" in
  y | Y | yes) ;;
  *) die "stopped" ;;
  esac
}

# use_profile <profile>: every AWS call after this runs as it. Sets REGION and ACCOUNT.
use_profile() {
  aws configure list-profiles 2>/dev/null | grep -qx "$1" ||
    die "no AWS CLI profile named '$1' (aws configure sso creates one)"
  export AWS_PROFILE="$1"
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
  REGION=${REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region 2>/dev/null || true)}}}
  [ -n "$REGION" ] || die "profile '$1' sets no region; set AWS_REGION"
  export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION"
  ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null) ||
    die "profile '$1' has no valid session; run: aws sso login --profile $1"
}

# The stack the other verbs act on: what launch remembered. Sets PROFILE,
# REGION, STACK, ACCOUNT, INSTANCE and VOLUME.
load_target() {
  command -v jq >/dev/null 2>&1 || die "jq is required"
  [ -f "$TARGET_FILE" ] ||
    die "no stack is remembered here; launch one with: just serve launch"
  PROFILE=$(jq -r .profile "$TARGET_FILE")
  REGION=$(jq -r .region "$TARGET_FILE")
  STACK=$(jq -r .stack "$TARGET_FILE")
  use_profile "$PROFILE"
  local outputs
  outputs=$(aws cloudformation describe-stacks --stack-name "$STACK" \
    --query 'Stacks[0].Outputs' --output json 2>/dev/null) ||
    die "stack $STACK is not in account $ACCOUNT ($REGION)"
  INSTANCE=$(jq -r '.[] | select(.OutputKey == "InstanceId") | .OutputValue' <<<"$outputs")
  VOLUME=$(jq -r '.[] | select(.OutputKey == "DataVolumeId") | .OutputValue' <<<"$outputs")
  [ -n "$INSTANCE" ] || die "stack $STACK has no instance yet"
}

# on_instance <timeout-seconds> <script>: run it as root on the instance, print
# what it printed, and return its exit status. The wait is this function's own,
# sized from the timeout: the AWS CLI's waiter gives up after 100 seconds.
on_instance() {
  local timeout=$1 script=$2 request command status waited=0
  request=$(mktemp)
  jq -n --arg script "$script" --arg timeout "$timeout" \
    '{commands: [$script], executionTimeout: [$timeout]}' >"$request"
  command=$(aws ssm send-command --instance-ids "$INSTANCE" \
    --document-name AWS-RunShellScript --parameters "file://$request" \
    --query Command.CommandId --output text) || {
    rm -f "$request"
    die "could not reach the instance through Systems Manager"
  }
  rm -f "$request"
  while :; do
    sleep 3
    waited=$((waited + 3))
    status=$(aws ssm get-command-invocation --command-id "$command" \
      --instance-id "$INSTANCE" --query Status --output text 2>/dev/null || echo Pending)
    case "$status" in
    Pending | InProgress | Delayed) ;;
    *) break ;;
    esac
    [ "$waited" -lt $((timeout + 60)) ] ||
      die "no result after $waited seconds; the command may still be running (id $command)"
  done
  aws ssm get-command-invocation --command-id "$command" --instance-id "$INSTANCE" \
    --query StandardOutputContent --output text
  if [ "$status" != "Success" ]; then
    aws ssm get-command-invocation --command-id "$command" --instance-id "$INSTANCE" \
      --query StandardErrorContent --output text >&2
    echo "serve: the command ended as $status" >&2
    return 1
  fi
}

# A compose service name, safe to place in a command.
service_name() {
  case "$1" in
  '' | *[!a-z0-9-]*) die "not a service name: $1" ;;
  esac
  echo "$1"
}

cmd_launch() {
  [ $# -le 4 ] || usage
  local profile=${1:-} domain=${2:-} cidr=${3:-} zone=${HOSTED_ZONE_ID:-} line address vpc subnet version tag
  STACK=${4:-RoboSystemsServe}
  if [ -z "$profile" ] || [ -z "$domain" ] || [ -z "$cidr" ]; then
    [ -t 0 ] || die "launch needs its three arguments when there is no terminal to ask on"
  fi

  local profiles=()
  while IFS= read -r line; do
    [ -z "$line" ] || profiles+=("$line")
  done < <(aws configure list-profiles 2>/dev/null)
  [ ${#profiles[@]} -gt 0 ] || die "no AWS CLI profiles; create one with: aws configure sso"
  if [ -z "$profile" ]; then
    echo "Which AWS account? (AWS CLI profiles on this machine)" >&2
    profile=$(choose "Profile number" "${profiles[@]}")
  fi
  REGION=""
  use_profile "$profile"

  # The account's public zones, "id name" a line; the names lose their final dot.
  local zones=() zone_name="" candidate
  while IFS= read -r line; do
    [ -z "$line" ] || zones+=("$line")
  done < <(aws route53 list-hosted-zones \
    --query 'HostedZones[?Config.PrivateZone==`false`].[Id,Name]' --output text |
    sed -e 's#^/hostedzone/##' -e 's/\.$//' | tr '\t' ' ')
  [ ${#zones[@]} -gt 0 ] ||
    die "account $ACCOUNT has no public Route 53 hosted zone; the stack needs one for its names"

  if [ -z "$domain" ]; then
    echo "Public hosted zones in account $ACCOUNT:" >&2
    for line in "${zones[@]}"; do
      echo "  ${line#* }" >&2
    done
    candidate=""
    [ ${#zones[@]} -ne 1 ] || candidate="serve.${zones[0]#* }"
    domain=$(ask "Base name under one of them (api., app., ledger., investor. and s3. go under it)" "$candidate")
    [ -n "$domain" ] || die "no base name"
  fi
  if [ -z "$zone" ]; then
    # The zone that holds the name: the longest zone name the domain ends in.
    for line in "${zones[@]}"; do
      candidate=${line#* }
      case "$domain" in
      "$candidate" | *".$candidate")
        if [ ${#candidate} -gt ${#zone_name} ]; then
          zone_name=$candidate
          zone=${line%% *}
        fi
        ;;
      esac
    done
    [ -n "$zone" ] ||
      die "no public hosted zone in account $ACCOUNT holds $domain; create one or set HOSTED_ZONE_ID"
  fi

  [ -n "$cidr" ] || cidr=$(ask "Address range allowed to reach it on 443 (\"me\" = this machine)" "me")
  if [ "$cidr" = "me" ]; then
    address=$(curl -sf --max-time 10 https://checkip.amazonaws.com) ||
      die "could not read this machine's public address; pass a CIDR"
    cidr="${address}/32"
  fi

  vpc=${VPC_ID:-}
  if [ -z "$vpc" ]; then
    vpc=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true \
      --query 'Vpcs[0].VpcId' --output text)
    [ "$vpc" != "None" ] || die "the region has no default VPC; set VPC_ID and SUBNET_ID"
  fi
  subnet=${SUBNET_ID:-}
  if [ -z "$subnet" ]; then
    subnet=$(aws ec2 describe-subnets \
      --filters "Name=vpc-id,Values=$vpc" Name=map-public-ip-on-launch,Values=true \
      --query 'sort_by(Subnets, &AvailabilityZone)[0].SubnetId' --output text)
    [ "$subnet" != "None" ] || die "no public subnet in $vpc; set SUBNET_ID"
  fi

  version=${ROBOSYSTEMS_VERSION:-}
  if [ -z "$version" ]; then
    # A release is launchable once its image is published, which can trail the tag.
    git -C "$ROOT" fetch --tags --quiet origin 2>/dev/null || true
    for tag in $(git -C "$ROOT" tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -5); do
      if "$HERE/check-release-image.sh" "robofinsystems/robosystems:$tag" >/dev/null 2>&1; then
        version=$tag
        break
      fi
    done
    [ -n "$version" ] || die "found no release with a published image; set ROBOSYSTEMS_VERSION"
  fi

  local params=(
    "VpcId=$vpc" "PublicSubnetId=$subnet" "AllowedCidr=$cidr"
    "HostedZoneId=$zone" "DomainName=$domain" "RobosystemsVersion=$version"
  )
  [ -z "${INSTANCE_TYPE:-}" ] || params+=("InstanceType=$INSTANCE_TYPE")
  [ -z "${DATA_VOLUME_SIZE:-}" ] || params+=("DataVolumeSize=$DATA_VOLUME_SIZE")
  [ -z "${SNAPSHOT_ID:-}" ] || params+=("DataVolumeSnapshotId=$SNAPSHOT_ID")
  [ -z "${ANTHROPIC_API_KEY:-}" ] || params+=("AnthropicApiKey=$ANTHROPIC_API_KEY")

  cat <<EOF

RoboSystems Serve
  account     $ACCOUNT ($REGION), profile $profile
  stack       $STACK
  release     $version
  names       api. app. ledger. investor. s3. under $domain (zone ${zone_name:-$zone})
  reachable   443 from $cidr
  network     $vpc / $subnet
  operators   $([ -n "${ANTHROPIC_API_KEY:-}" ] && echo "Anthropic API key" || echo "not configured")

This starts an EC2 instance, a data volume and an Elastic IP that bill until
the stack is deleted. You run it: upgrades, backups and the instance are yours.
EOF
  confirm "Launch?"

  mkdir -p "$(dirname "$TARGET_FILE")"
  jq -n --arg profile "$profile" --arg region "$REGION" --arg stack "$STACK" \
    '{profile: $profile, region: $region, stack: $stack}' >"$TARGET_FILE"

  echo "Launching; the stack completes when the API answers, usually within 15 minutes."
  aws cloudformation deploy --stack-name "$STACK" \
    --template-file "$TEMPLATE" --capabilities CAPABILITY_IAM \
    --parameter-overrides "${params[@]}" ||
    die "the stack did not complete; read the boot log with: just serve shell, then /var/log/robosystems-bootstrap.log"

  aws cloudformation describe-stacks --stack-name "$STACK" \
    --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output text
  echo "Next: just serve status. Delete it with: aws cloudformation delete-stack --profile $profile --region $REGION --stack-name $STACK"
}

cmd_status() {
  load_target
  echo "stack $STACK in $ACCOUNT ($REGION), instance $INSTANCE: $(aws ec2 describe-instances \
    --instance-ids "$INSTANCE" --query 'Reservations[0].Instances[0].State.Name' --output text)"
  on_instance 60 "cd /data/robosystems
echo \"release: \$(grep -E '^ROBOSYSTEMS_IMAGE=' .env | cut -d: -f2), API reports \$(curl -sf --max-time 5 http://127.0.0.1:8000/v1/status | jq -r .details.version 2>/dev/null || echo 'no answer')\"
docker ps --all --format 'table {{.Names}}\t{{.Status}}'
echo
df -h /data | tail -1 | awk '{print \"data volume: \" \$3 \" used of \" \$2 \" (\" \$5 \")\"}'
free -m | awk 'NR == 2 {print \"memory: \" \$3 \" MB used of \" \$2 \" MB\"}'"
}

cmd_upgrade() {
  local tag=${1:-latest}
  case "$tag" in
  latest | v[0-9]*.[0-9]*.[0-9]*) ;;
  *) die "not a release: $tag (a tag such as v1.25.0, or latest)" ;;
  esac
  load_target
  echo "Upgrading $STACK to $tag. A snapshot first is: just serve snapshot"
  confirm "Upgrade?"
  on_instance 1800 "robosystems-upgrade $tag 2>&1"
}

cmd_logs() {
  [ $# -ge 1 ] || die "which service? (api, worker, graph-api, dagster-daemon, dagster-webserver, pg, ...)"
  local service lines=${2:-100}
  service=$(service_name "$1")
  case "$lines" in
  '' | *[!0-9]*) die "not a line count: $lines" ;;
  esac
  load_target
  on_instance 60 "cd /data/robosystems && $COMPOSE logs --no-log-prefix --tail $lines $service 2>&1"
}

cmd_restart() {
  local service=""
  [ $# -eq 0 ] || service=$(service_name "$1")
  load_target
  confirm "Restart ${service:-every service} on $STACK?"
  on_instance 600 "cd /data/robosystems && $COMPOSE restart $service 2>&1"
}

cmd_reboot() {
  load_target
  confirm "Reboot instance $INSTANCE ($STACK)? The stack is down until it is back."
  aws ec2 reboot-instances --instance-ids "$INSTANCE"
  echo "Rebooting. The containers start by themselves; check with: just serve status"
}

cmd_snapshot() {
  load_target
  [ -n "$VOLUME" ] || die "stack $STACK reports no data volume"
  aws ec2 create-snapshot --volume-id "$VOLUME" \
    --description "$STACK data volume, by hand $(date -u +%Y-%m-%dT%H:%MZ)" \
    --tag-specifications "ResourceType=snapshot,Tags=[{Key=Name,Value=$STACK-manual}]" \
    --query '[SnapshotId,State]' --output text
  echo "Start a new stack from it with: SNAPSHOT_ID=<id> just serve launch"
}

# The value goes to the stack's settings secret and the instance reads it from
# there, so it is never in a command's text or in Systems Manager's history.
cmd_set() {
  [ $# -ge 1 ] && [ $# -le 2 ] || die "usage: just serve set <KEY> [value]"
  local key=$1 value secret current next
  case "$key" in
  [A-Z]*) ;;
  *) die "not a setting name: $key" ;;
  esac
  case "$key" in
  *[!A-Z0-9_]*) die "not a setting name: $key" ;;
  esac
  load_target
  if [ $# -eq 2 ]; then
    value=$2
  else
    [ -t 0 ] || die "no value given and no terminal to ask on"
    read -r -s -p "Value for $key (hidden): " value
    echo >&2
  fi
  secret=$(aws cloudformation describe-stack-resource --stack-name "$STACK" \
    --logical-resource-id ServeSettingsSecret \
    --query StackResourceDetail.PhysicalResourceId --output text 2>/dev/null) ||
    die "stack $STACK has no settings secret"
  current=$(aws secretsmanager get-secret-value --secret-id "$secret" \
    --query SecretString --output text)
  next=$(mktemp)
  chmod 600 "$next"
  jq --arg key "$key" --arg value "$value" '.[$key] = $value' <<<"$current" >"$next"
  aws secretsmanager put-secret-value --secret-id "$secret" \
    --secret-string "file://$next" >/dev/null
  rm -f "$next"
  echo "Saved $key; applying it on the instance."
  on_instance 600 "robosystems-apply-settings 2>&1"
}

cmd_tunnel() {
  local port
  case "${1:-}" in
  dagster) port=8002 ;;
  mailpit) port=8025 ;;
  *) die "usage: just serve tunnel dagster|mailpit" ;;
  esac
  load_target
  echo "http://127.0.0.1:1$port while this runs; Ctrl-C closes it."
  exec aws ssm start-session --target "$INSTANCE" \
    --document-name AWS-StartPortForwardingSession \
    --parameters "portNumber=$port,localPortNumber=1$port"
}

cmd_shell() {
  load_target
  exec aws ssm start-session --target "$INSTANCE"
}

command -v aws >/dev/null 2>&1 || die "the AWS CLI is required"
[ $# -ge 1 ] || usage
VERB=$1
shift
case "$VERB" in
launch | status | upgrade | logs | restart | reboot | snapshot | set | tunnel | shell) "cmd_$VERB" "$@" ;;
*) usage ;;
esac
