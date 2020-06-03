FROM budgetkey/budgetkey-data-pipelines:latest

USER root
RUN apt-get install -y build-essential libxml2-dev libxslt-dev libffi-dev libssl-dev && mkdir -p /testenv
WORKDIR /testenv
RUN cp -r /Makefile /setup.py /tox.ini /VERSION.txt /datapackage_pipelines_budgetkey /testenv && make install
ENTRYPOINT ["sh", "-c", "make clean && make test"]
