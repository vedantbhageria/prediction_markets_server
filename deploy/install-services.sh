#!/bin/bash
# Move the six programs from tmux sessions to systemd services, so they start
# when the server boots and restart by themselves if they crash.
#
#   sudo bash deploy/install-services.sh            # install and start
#   sudo bash deploy/install-services.sh --force    # even if Kalshi is unreachable
#
# It refuses to run while Kalshi cannot be reached: a program restarted while
# the network refuses new connections loses the ones it had and stays paused
# until the network allows connections again.
set -euo pipefail

USER_NAME=theonealpha
APP=/home/$USER_NAME/prediction_markets_server
PROGRAMS="discover activity live backfill api viewer"

if [ "$(id -u)" -ne 0 ]; then
    echo "run it with sudo: sudo bash deploy/install-services.sh" >&2
    exit 1
fi

if [ "${1:-}" != "--force" ] && ! timeout 8 bash -c "</dev/tcp/external-api.kalshi.com/443" 2>/dev/null; then
    echo "Kalshi is unreachable from here right now (the network is refusing new connections)."
    echo "Restarting now would pause collection until it comes back. Try again later,"
    echo "or add --force to go ahead anyway."
    exit 1
fi

mkdir -p /home/$USER_NAME/logs
chown $USER_NAME: /home/$USER_NAME/logs

echo "stopping the tmux copies (each finishes what it is doing first)..."
for name in discover activity live backfill viewer; do
    pkill -INT -u $USER_NAME -f "^.venv/bin/python $name.py" || true
done
pkill -INT -u $USER_NAME -f "^.venv/bin/uvicorn api.main:app" || true
for i in $(seq 1 90); do
    pgrep -u $USER_NAME -f "^.venv/bin/(python (discover|activity|live|backfill|viewer).py|uvicorn api.main:app)" \
        > /dev/null || break
    sleep 1
done
for name in $PROGRAMS; do
    sudo -u $USER_NAME tmux kill-session -t $name 2>/dev/null || true
done

echo "installing the services..."
for name in $PROGRAMS; do
    cp "$APP/deploy/prediction-markets-$name.service" /etc/systemd/system/
done
systemctl daemon-reload
for name in $PROGRAMS; do
    systemctl enable --now "prediction-markets-$name" > /dev/null 2>&1
done

sleep 20
echo
for name in $PROGRAMS; do
    printf "%-10s %s\n" "$name" "$(systemctl is-active prediction-markets-$name)"
done
echo
echo "logs: tail -f ~/logs/<program>.log    status: systemctl status prediction-markets-<program>"
