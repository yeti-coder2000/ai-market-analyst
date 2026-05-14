#!/usr/bin/env bash
set -e

python -m app.services.daily_report_scheduler &
exec python -m app.runners.multi_group_worker
