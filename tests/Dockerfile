FROM budgetkey/budgetkey-data-pipelines:latest
USER root
RUN apk add --update build-base && mkdir -p /testenv
WORKDIR /testenv
RUN cp -r /Makefile /setup.py /tox.ini /VERSION.txt /datapackage_pipelines_budgetkey /testenv && make install
ENTRYPOINT ["sh", "-c", "make clean && make test"]
