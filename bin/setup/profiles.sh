# Sourced by the setup scripts. Two AWS CLI profiles share one SSO session:
#
#   robosystems-admin  AdministratorAccess: bootstrap, secrets, infrastructure
#   robosystems-sso    RoboSystemsOperator: everything else (.envrc points here)
#
# Scripts that need admin resolve their profile here instead of trusting the
# ambient AWS_PROFILE, which .envrc sets to the operator profile.

ROBOSYSTEMS_OPERATOR_PROFILE="${ROBOSYSTEMS_OPERATOR_PROFILE:-robosystems-sso}"

# An explicit argument wins, then ROBOSYSTEMS_ADMIN_PROFILE, then
# robosystems-admin. Before `just bootstrap-sso` has split the profiles, the one
# SSO profile is the admin one, so robosystems-sso is the fallback.
resolve_admin_profile() {
    if [ -n "${1:-}" ]; then
        echo "$1"
    elif [ -n "${ROBOSYSTEMS_ADMIN_PROFILE:-}" ]; then
        echo "$ROBOSYSTEMS_ADMIN_PROFILE"
    elif aws configure list-profiles 2>/dev/null | grep -qx "robosystems-admin"; then
        echo "robosystems-admin"
    elif aws configure list-profiles 2>/dev/null | grep -qx "$ROBOSYSTEMS_OPERATOR_PROFILE"; then
        echo "$ROBOSYSTEMS_OPERATOR_PROFILE"
    else
        echo "robosystems-admin"
    fi
}
