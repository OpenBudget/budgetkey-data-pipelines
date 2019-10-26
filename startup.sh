#!/bin/sh
echo 'Starting BudgetKey DPP!'

(ssh adam@tzabar.obudget.org "docker system prune -f -a  && docker pull budgetkey/budgetkey-data-pipelines") || true
(ssh adam@tzabar.obudget.org "docker ps | grep google-chrome | cut -c1-12 | xargs docker stop") || true

/dpp/docker/run.sh server
