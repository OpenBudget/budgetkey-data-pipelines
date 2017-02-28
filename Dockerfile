FROM frictionlessdata/datapackage-pipelines:latest

ADD pipelines /pipelines
ADD common /common
ADD processors /processors
ADD requirements.txt /

RUN apk add --update libxml2 libxslt git && \
    pip install -U git+https://github.com/frictionlessdata/tabulator-py
RUN addgroup dpp && adduser -s /bin/bash -D -G dpp dpp && addgroup dpp root && addgroup dpp redis && \
    mkdir -p /var/datapackages && chown dpp.dpp /var/datapackages -R && \
    chown dpp.dpp /pipelines -R  && \
    chown dpp.dpp /var/log/redis -R && \
    chown dpp.dpp /var/lib/redis -R && \
    ls -la /var/lib && \
    chown dpp.dpp /var/run/redis -R && \
    ls -la /var/lib 
RUN pip install -r /requirements.txt
USER dpp

ENV PYTHONPATH=/
ENV DPP_PROCESSOR_PATH=/processors
ENV REDIS_USER=dpp
ENV REDIS_GROUP=dpp

EXPOSE 5000

CMD ["server"]
