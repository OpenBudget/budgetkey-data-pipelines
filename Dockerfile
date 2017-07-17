FROM frictionlessdata/datapackage-pipelines:latest

ADD ./ /
ADD dpp-runners.yaml /budgetkey_data_pipelines/pipelines/

RUN apk add --update libxml2 libxslt sudo openssh-client curl jpeg-dev antiword poppler-utils
RUN addgroup dpp && adduser -s /bin/bash -D -G dpp dpp && addgroup dpp root && addgroup dpp redis && \
    mkdir -p /var/datapackages && chown dpp.dpp /var/datapackages -R && \
    mkdir -p /home/dpp/.ssh && chown dpp.dpp /home/dpp/.ssh -R && \
    chown dpp.dpp /var/log/redis -R && \
    chown dpp.dpp /var/lib/redis -R && \
    chown dpp.dpp /var/run/redis -R && \
    chmod 700 /home/dpp/.ssh && \
    chown dpp.dpp /budgetkey_data_pipelines -R && \
    echo '%root ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/root
USER dpp

RUN sudo apk --update --no-cache --virtual=build-dependencies add build-base libxml2-dev libxslt-dev
RUN sudo pip install -e /
RUN sudo apk del build-dependencies && \
    sudo rm -rf /var/cache/apk/*

ENV PYTHONPATH=/
ENV DPP_PROCESSOR_PATH=/budgetkey_data_pipelines/processors
ENV DPP_REDIS_HOST=localhost
ENV REDIS_USER=dpp
ENV REDIS_GROUP=dpp

WORKDIR /budgetkey_data_pipelines/pipelines/

EXPOSE 5000

CMD ["server"]
