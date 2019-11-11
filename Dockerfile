FROM frictionlessdata/datapackage-pipelines:latest

RUN apk add --update --no-cache libxml2 libxslt sudo openssh-client curl jpeg-dev antiword \
    poppler-utils libmagic binutils openjdk7-jre 
RUN addgroup dpp && adduser -s /bin/bash -D -G dpp dpp && addgroup dpp root && addgroup dpp redis && addgroup redis dpp && \
    mkdir -p /var/datapackages && chown dpp.dpp /var/datapackages -R && chmod -R a+r /var/datapackages && \
    mkdir -p /home/dpp/.ssh && chown dpp.dpp /home/dpp/.ssh -R && \
    chown dpp.dpp /var/log/redis -R && \
    chown dpp.dpp /var/lib/redis -R && \
    chown dpp.dpp /var/run/redis -R && \
    chown dpp.dpp /var/run/dpp -R && \
    chmod 700 /home/dpp/.ssh && \
    echo '%root ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/root
RUN apk --update --no-cache --virtual=build-dependencies add build-base libxml2-dev libxslt-dev libffi-dev openssl-dev
RUN pip install numpy && pip install textract==1.5.0 pyquery "rfc3986<1.0" filemagic tabula-py paramiko boto3

ADD ./ /

ADD .dpp-runners.tzabar /datapackage_pipelines_budgetkey/pipelines/
RUN mv /datapackage_pipelines_budgetkey/pipelines/.dpp-runners.tzabar /datapackage_pipelines_budgetkey/pipelines/dpp-runners.yaml

RUN chown dpp.dpp /datapackage_pipelines_budgetkey -R
RUN pip install -e /
RUN pip install -U -r /requirements-dev.txt

RUN echo "http://nl.alpinelinux.org/alpine/edge/main" > /etc/apk/repositories && \
    echo "http://nl.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories && \
    echo "http://nl.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && cat /etc/apk/repositories
RUN apk --update --no-cache add -u libevent && \
    ln -s /usr/lib/libevent-2.1.so.7 /usr/lib/libevent-2.1.so.6 && \
    ls -la /usr/lib/libevent* && \
    apk --update --no-cache add -u chromium-chromedriver && \
    cd /usr/local/bin/ && ln -s /usr/bin/chromedriver

RUN apk del build-dependencies && \
    sudo rm -rf /var/cache/apk/* && \
    ln -s /usr/lib/libmagic.so.1 /usr/lib/libmagic.so

RUN apk --update --no-cache add -u libstdc++ musl

USER dpp

ENV PYTHONPATH=/
ENV LD_LIBRARY_PATH=/usr/lib
ENV DPP_PROCESSOR_PATH=/datapackage_pipelines_budgetkey/processors
ENV DPP_REDIS_HOST=localhost
ENV REDIS_USER=dpp
ENV REDIS_GROUP=dpp
ENV DPP_NUM_WORKERS=8

WORKDIR /datapackage_pipelines_budgetkey/pipelines/

EXPOSE 5000

ENTRYPOINT ["/startup.sh"]
