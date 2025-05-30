#!/bin/sh
echo 'Starting BudgetKey DPP!'

(ssh adam@tzabar.obudget.org "docker system prune -f -a  && docker pull budgetkey/budgetkey-data-pipelines") || true
(ssh adam@tzabar.obudget.org "docker ps | grep -v wernight/phantomjs | grep -v fetcher | grep -v CONTAINER | cut -c1-12 | xargs docker kill -s 9") || true

/dpp/docker/run.sh server
