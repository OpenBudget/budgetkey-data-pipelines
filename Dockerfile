FROM frictionlessdata/datapackage-pipelines:latest

ADD ./ /
ADD dpp-runners.yaml /budgetkey_data_pipelines/pipelines/

RUN apk add --update libxml2 libxslt git sudo openssh-client fontconfig curl && \
    pip install -U git+https://github.com/frictionlessdata/tabulator-py
RUN mkdir -p /usr/share && \
    cd /usr/share \
    && curl -L https://github.com/Overbryd/docker-phantomjs-alpine/releases/download/2.11/phantomjs-alpine-x86_64.tar.bz2 | tar xj \
    && ln -s /usr/share/phantomjs/phantomjs /usr/bin/phantomjs \
    && phantomjs --version
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
RUN cat /etc/sudoers
RUN sudo pip install -r /requirements.txt && sudo pip install -e /

ENV PYTHONPATH=/
ENV DPP_PROCESSOR_PATH=/budgetkey_data_pipelines/processors
ENV DPP_REDIS_HOST=localhost
ENV REDIS_USER=dpp
ENV REDIS_GROUP=dpp

WORKDIR /budgetkey_data_pipelines/pipelines/

EXPOSE 5000

CMD ["server"]
