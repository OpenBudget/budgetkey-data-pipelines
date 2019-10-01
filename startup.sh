#!/bin/sh
echo 'Starting BudgetKey DPP!'

(ssh adam@tzabar.obudget.org "docker system prune -f -a  && docker pull budgetkey/budgetkey-data-pipelines") || true

/dpp/docker/run.sh server
