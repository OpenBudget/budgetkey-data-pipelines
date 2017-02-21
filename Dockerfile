FROM frictionlessdata/datapackage-pipelines:latest

ADD pipelines /pipelines
ADD requirements.txt /

RUN apk add --update libxml2 libxslt git && pip install -U git+https://github.com/frictionlessdata/tabulator-py
RUN addgroup dpp && adduser -s /bin/bash -D -G dpp dpp && \
    mkdir -p /var/datapackages && chown dpp.dpp /var/datapackages -R && \
    chown dpp.dpp /pipelines -R
RUN pip install -r /requirements.txt
USER dpp

EXPOSE 5000

CMD ["server"]
