FROM frictionlessdata/datapackage-pipelines:latest

ADD pipelines /pipelines

RUN apk add --update git && pip install -U git+https://github.com/frictionlessdata/tabulator-py
RUN addgroup dpp && adduser -s /bin/bash -D -G dpp dpp && \
    mkdir -p /var/datapackages && chown dpp.dpp /var/datapackages -R && \
    chown dpp.dpp /pipelines -R
USER dpp

EXPOSE 5000

CMD ["server"]
