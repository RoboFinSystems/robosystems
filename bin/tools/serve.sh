#!/bin/bash
# RoboSystems Serve: the whole platform on one EC2 instance in your own AWS
# account, launched from cloudformation/serve.yaml and run from here.
#
#   bin/tools/serve.sh launch [<profile> <domain> <allowed-address|me> [stack-name]]
#   bin/tools/serve.sh status
#   bin/tools/serve.sh upgrade [<tag>|latest]
#   bin/tools/serve.sh logs <service> [lines]
#   bin/tools/serve.sh restart [service]
#   bin/tools/serve.sh reboot
#   bin/tools/serve.sh resize small|medium|large
#   bin/tools/serve.sh allow <address|me>
#   bin/tools/serve.sh snapshot
#   bin/tools/serve.sh set <KEY> [value]
#   bin/tools/serve.sh unset <KEY>
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
#   SIZE                  small (16 GB, the default), medium (32 GB) or large (64 GB)
#   INSTANCE_TYPE         an instance type the template allows, instead of SIZE
#   DATA_VOLUME_SIZE      default: the template's, in GiB
#   SNAPSHOT_ID           start from a snapshot of an earlier data volume
#   ANTHROPIC_API_KEY     configure the server-side operators (it reaches AWS in
#                         a parameter file, never on a command line)
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
  sed -n 's/^#   bin\/tools\/serve.sh/  just serve/p' "$0" >&2
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
  local timeout=$1 script=$2 request command status problem waited=0
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
    problem=$(mktemp)
    status=$(aws ssm get-command-invocation --command-id "$command" \
      --instance-id "$INSTANCE" --query Status --output text 2>"$problem") || {
      # The invocation takes a moment to exist; anything else is a real failure
      # (an expired session, a lost permission) and waiting would not fix it.
      if grep -q InvocationDoesNotExist "$problem"; then
        status=Pending
      else
        cat "$problem" >&2
        rm -f "$problem"
        die "could not read the command's result (id $command)"
      fi
    }
    rm -f "$problem"
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

# size_type <size>: the instance type of a size. The container memory caps are
# not chosen here: the instance sets them from its own memory at every boot.
size_type() {
  case "$1" in
  small) echo r7g.large ;;
  medium) echo r7g.xlarge ;;
  large) echo r7g.2xlarge ;;
  *) die "not a size: $1 (small is 16 GB, medium 32 GB, large 64 GB)" ;;
  esac
}

# one_address <address|me>: the /32 that 443 opens to. One address, never a
# range: that is what this shape is, one operator's own deployment.
one_address() {
  local cidr=$1 address
  if [ "$cidr" = "me" ]; then
    address=$(curl -sf --max-time 10 https://checkip.amazonaws.com) ||
      die "could not read this machine's public address; pass an address"
    cidr="${address}/32"
  fi
  case $cidr in
  */32) ;;
  */*) die "443 opens to one address: pass it as <address>/32, not a range ($cidr)" ;;
  *) cidr="${cidr}/32" ;;
  esac
  case ${cidr%/32} in
  *[!0-9.]* | '') die "not an IPv4 address: ${cidr%/32}" ;;
  esac
  echo "$cidr"
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

  [ -n "$cidr" ] || cidr=$(ask "The one address allowed to reach it on 443 (\"me\" = this machine)" "me")
  cidr=$(one_address "$cidr")

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

  # deploy would update a stack of this name in place, and a changed release
  # or setting changes the boot script, which restarts or replaces the instance.
  local existing
  existing=$(aws cloudformation describe-stacks --stack-name "$STACK" \
    --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true)
  case "$existing" in
  '') ;;
  *FAILED | ROLLBACK_COMPLETE)
    die "stack $STACK exists from a launch that failed ($existing); delete it first: aws cloudformation delete-stack --profile $profile --region $REGION --stack-name $STACK"
    ;;
  *)
    die "stack $STACK already exists in account $ACCOUNT ($existing); launch does not update a running stack. To move it to another release: just serve upgrade. For a second stack, give launch another stack name"
    ;;
  esac

  local size=${SIZE:-} instance_type=${INSTANCE_TYPE:-}
  if [ -z "$instance_type" ]; then
    if [ -z "$size" ] && [ -t 0 ]; then
      size=$(ask "Size: small (16 GB), medium (32 GB) or large (64 GB)" "small")
    fi
    size=${size:-small}
    instance_type=$(size_type "$size")
  fi

  local params=(
    "VpcId=$vpc" "PublicSubnetId=$subnet" "AllowedCidr=$cidr"
    "HostedZoneId=$zone" "DomainName=$domain" "RobosystemsVersion=$version"
    "InstanceType=$instance_type"
  )
  [ -z "${DATA_VOLUME_SIZE:-}" ] || params+=("DataVolumeSize=$DATA_VOLUME_SIZE")
  [ -z "${SNAPSHOT_ID:-}" ] || params+=("DataVolumeSnapshotId=$SNAPSHOT_ID")
  [ -z "${ANTHROPIC_API_KEY:-}" ] || params+=("AnthropicApiKey=$ANTHROPIC_API_KEY")

  cat <<EOF

RoboSystems Serve
  account     $ACCOUNT ($REGION), profile $profile
  stack       $STACK
  release     $version
  instance    $instance_type${size:+ ($size)}
  names       api. app. ledger. investor. s3. under $domain (zone ${zone_name:-$zone})
  reachable   443 from $cidr
  network     $vpc / $subnet
  operators   $([ -n "${ANTHROPIC_API_KEY:-}" ] && echo "Anthropic API key" || echo "not configured")

This starts an EC2 instance, a data volume and an Elastic IP that bill until
the stack is deleted. You run it: upgrades, backups and the instance are yours.
EOF
  confirm "Launch?"

  # The parameters travel in a file, so a key among them is in no process list.
  local parameter_file failed_instance
  parameter_file=$(mktemp)
  chmod 600 "$parameter_file"
  printf '%s\n' "${params[@]}" | jq -R . | jq -s . >"$parameter_file"

  echo "Launching; the stack completes when the API answers, usually within 15 minutes."
  # A failed launch is kept, not rolled back, so its boot log can be read.
  if ! aws cloudformation deploy --stack-name "$STACK" \
    --template-file "$TEMPLATE" --capabilities CAPABILITY_IAM --disable-rollback \
    --parameter-overrides "file://$parameter_file"; then
    rm -f "$parameter_file"
    failed_instance=$(aws cloudformation describe-stack-resource --stack-name "$STACK" \
      --logical-resource-id ServeInstance \
      --query StackResourceDetail.PhysicalResourceId --output text 2>/dev/null || true)
    echo "serve: the stack did not complete and was left as it failed." >&2
    [ -z "$failed_instance" ] || [ "$failed_instance" = "None" ] ||
      echo "  The boot log is /var/log/robosystems-bootstrap.log on the instance: aws ssm start-session --profile $profile --region $REGION --target $failed_instance" >&2
    echo "  Delete it before launching again: aws cloudformation delete-stack --profile $profile --region $REGION --stack-name $STACK" >&2
    exit 1
  fi
  rm -f "$parameter_file"

  # Remembered only now, so a launch that fails never replaces the pointer to
  # a stack that works.
  mkdir -p "$(dirname "$TARGET_FILE")"
  jq -n --arg profile "$profile" --arg region "$REGION" --arg stack "$STACK" \
    '{profile: $profile, region: $region, stack: $stack}' >"$TARGET_FILE"

  aws cloudformation describe-stacks --stack-name "$STACK" \
    --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output text
  echo "Next: just serve status. Delete it with: aws cloudformation delete-stack --profile $profile --region $REGION --stack-name $STACK"
}

cmd_status() {
  load_target
  echo "stack $STACK in $ACCOUNT ($REGION), instance $INSTANCE: $(aws ec2 describe-instances \
    --instance-ids "$INSTANCE" \
    --query 'Reservations[0].Instances[0].[InstanceType,State.Name]' --output text | tr '\t' ' ')"
  on_instance 60 "cd /data/robosystems
echo \"release: \$(grep -E '^ROBOSYSTEMS_IMAGE=' .env | cut -d: -f2), API reports \$(curl -sf --max-time 5 http://127.0.0.1:8000/v1/status | jq -er .details.version 2>/dev/null || echo 'no answer')\"
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
  # Systems Manager returns about 24,000 characters of output and drops the rest.
  [ "$lines" -le 200 ] || die "at most 200 lines; for more, read it on the instance: just serve shell"
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

# Moves the one address 443 opens to, when the operator's own address changes.
# It replaces the address and never adds one. Like resize it goes through EC2,
# so the stack keeps reporting the address it launched with.
ALLOWED_RULE="The stack, through Caddy"
cmd_allow() {
  [ $# -eq 1 ] || die "usage: just serve allow <address|me>"
  local cidr group current old address present
  cidr=$(one_address "$1")
  load_target
  group=$(aws cloudformation describe-stack-resource --stack-name "$STACK" \
    --logical-resource-id ServeSecurityGroup \
    --query StackResourceDetail.PhysicalResourceId --output text)
  current=$(aws ec2 describe-security-group-rules --filters "Name=group-id,Values=$group" \
    --query "SecurityGroupRules[?!IsEgress && FromPort==\`443\` && Description=='$ALLOWED_RULE'].[SecurityGroupRuleId,CidrIpv4]" \
    --output text)
  present=$(awk -v cidr="$cidr" '$2 == cidr' <<<"$current")
  if [ -n "$present" ] && [ "$(wc -l <<<"$current")" -eq 1 ]; then
    echo "443 is already open to $cidr and to nothing else."
    return
  fi
  confirm "Open 443 on $STACK to $cidr only, in place of $(awk '{print $2}' <<<"$current" | paste -sd' ' -)?"
  # The new address first, so a failure never leaves the stack closed to both.
  [ -n "$present" ] ||
    aws ec2 authorize-security-group-ingress --group-id "$group" --ip-permissions \
      "IpProtocol=tcp,FromPort=443,ToPort=443,IpRanges=[{CidrIp=$cidr,Description=\"$ALLOWED_RULE\"}]" >/dev/null
  while read -r old address; do
    [ -n "$old" ] && [ "$address" != "$cidr" ] || continue
    aws ec2 revoke-security-group-ingress --group-id "$group" \
      --security-group-rule-ids "$old" >/dev/null
  done <<<"$current"
  echo "443 is open to $cidr only."
}

# The instance type moves through EC2, not the stack: a stack update would
# re-resolve the Amazon Linux image and could replace the instance. The stack
# keeps reporting the type it launched with. The data volume and the address
# stay attached across the stop, and the instance sets its memory caps from
# the memory it finds when it starts.
cmd_resize() {
  [ $# -eq 1 ] || die "usage: just serve resize small|medium|large"
  local target current
  target=$(size_type "$1")
  load_target
  current=$(aws ec2 describe-instances --instance-ids "$INSTANCE" \
    --query 'Reservations[0].Instances[0].InstanceType' --output text)
  [ "$current" != "$target" ] || die "instance $INSTANCE is already $target"
  confirm "Resize $INSTANCE ($STACK) from $current to $target? The stack is down for a few minutes while it stops and starts."
  aws ec2 stop-instances --instance-ids "$INSTANCE" >/dev/null
  echo "Stopping."
  aws ec2 wait instance-stopped --instance-ids "$INSTANCE"
  if ! aws ec2 modify-instance-attribute --instance-id "$INSTANCE" \
    --instance-type "Value=$target"; then
    aws ec2 start-instances --instance-ids "$INSTANCE" >/dev/null
    die "could not change the type; the instance is starting again as $current"
  fi
  aws ec2 start-instances --instance-ids "$INSTANCE" >/dev/null
  echo "Starting as $target."
  aws ec2 wait instance-running --instance-ids "$INSTANCE"
  echo "Running. The containers come back with caps sized for it; check with: just serve status"
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

# A setting name the stack lets an operator change. The secrets the first boot
# generated are refused: the databases on the data volume were created with
# them, so a new value locks the stack out of its own data. The image pins
# belong to upgrade.
setting_name() {
  case "$1" in
  [A-Z]*) ;;
  *) die "not a setting name: $1" ;;
  esac
  case "$1" in
  *[!A-Z0-9_]*) die "not a setting name: $1" ;;
  esac
  case "$1" in
  POSTGRES_PASSWORD | DAGSTER_POSTGRES_PASSWORD | VALKEY_AUTH_TOKEN | JWT_SECRET_KEY | ADMIN_API_KEY | CONNECTION_CREDENTIALS_KEY)
    die "$1 was generated when the stack first booted and its data depends on it; it is not changed here"
    ;;
  *_IMAGE) die "$1 is the release pin; move it with: just serve upgrade" ;;
  esac
}

# settings_secret: the stack's settings secret, by its id.
settings_secret() {
  aws cloudformation describe-stack-resource --stack-name "$STACK" \
    --logical-resource-id ServeSettingsSecret \
    --query StackResourceDetail.PhysicalResourceId --output text 2>/dev/null ||
    die "stack $STACK has no settings secret"
}

# write_settings <secret> <jq-filter> [jq-args...]: the secret's JSON, changed.
write_settings() {
  local secret=$1 filter=$2 current next
  shift 2
  current=$(aws secretsmanager get-secret-value --secret-id "$secret" \
    --query SecretString --output text)
  next=$(mktemp)
  chmod 600 "$next"
  jq "$@" "$filter" <<<"$current" >"$next"
  aws secretsmanager put-secret-value --secret-id "$secret" \
    --secret-string "file://$next" >/dev/null
  rm -f "$next"
}

# The value goes to the stack's settings secret and the instance reads it from
# there, so it is never in a command's text or in Systems Manager's history.
# Given as an argument it is in this machine's shell history; the prompt is not.
# selfhost requires CONNECTION_CREDENTIALS_KEY. A stack that ran as dev
# encrypted its connections with a key derived from the JWT secret, so that is
# the key it pins. It first checks the JWT secret is one selfhost will accept.
# Plain sh, so it runs on a stack of any age.
# A function, not a variable set from $(cat <<EOF): bash 3.2, which macOS
# ships, ends a command substitution at the first case pattern's parenthesis.
pin_credentials_key_script() {
  cat <<'EOF'
cd /data/robosystems
# The value as the application reads it: one pair of surrounding quotes off.
JWT=$(grep -E '^JWT_SECRET_KEY=' .env | cut -d= -f2- | sed -E -e 's/^"(.*)"$/\1/' -e "s/^'(.*)'\$/\1/")
# selfhost refuses to start on these, so refuse here, before anything changes.
case "$JWT" in
'' | your-* | *development* | *dev-jwt*)
  echo "JWT_SECRET_KEY in .env is empty or a placeholder; selfhost would not start" >&2
  exit 1
  ;;
esac
[ "${#JWT}" -ge 32 ] || { echo "JWT_SECRET_KEY is shorter than 32 characters; selfhost would not start" >&2; exit 1; }
if grep -qE '^CONNECTION_CREDENTIALS_KEY=.' .env; then
  echo "credentials key already set"
  exit 0
fi
KEY=$(printf '%s_connection_encryption' "$JWT" | openssl dgst -sha256 -binary | base64 | tr '+/' '-_')
sed -i -E '/^#? ?CONNECTION_CREDENTIALS_KEY=/d' .env
printf 'CONNECTION_CREDENTIALS_KEY=%s\n' "$KEY" >>.env
echo "credentials key pinned"
EOF
}

cmd_set() {
  [ $# -ge 1 ] && [ $# -le 2 ] || die "usage: just serve set <KEY> [value]"
  local key=$1 value secret
  setting_name "$key"
  load_target
  if [ $# -eq 2 ]; then
    value=$2
  else
    [ -t 0 ] || die "no value given and no terminal to ask on"
    read -r -s -p "Value for $key (hidden): " value
    echo >&2
  fi
  case "$value" in
  *$'\n'* | *$'\r'*) die "a value is one line" ;;
  esac
  if [ "$key" = ENVIRONMENT ] && [ "$value" = selfhost ]; then
    echo "Pinning the credentials key the stack has used so far."
    on_instance 60 "$(pin_credentials_key_script)"
  fi
  secret=$(settings_secret)
  write_settings "$secret" '.[$key] = $value' --arg key "$key" --arg value "$value"
  echo "Saved $key; applying it on the instance."
  on_instance 600 "robosystems-apply-settings 2>&1"
}

# Drops a setting from the secret and comments its line out of .env, which
# returns the stack to the release's own default for it.
cmd_unset() {
  [ $# -eq 1 ] || die "usage: just serve unset <KEY>"
  local key=$1 secret
  setting_name "$key"
  load_target
  secret=$(settings_secret)
  write_settings "$secret" 'del(.[$key])' --arg key "$key"
  echo "Removed $key; applying on the instance."
  on_instance 600 "sed -i -E 's/^($key=)/# \\1/' /data/robosystems/.env && robosystems-apply-settings 2>&1"
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
launch | status | upgrade | logs | restart | reboot | resize | allow | snapshot | set | unset | tunnel | shell) "cmd_$VERB" "$@" ;;
*) usage ;;
esac
