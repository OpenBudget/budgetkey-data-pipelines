FROM frictionlessdata/datapackage-pipelines:latest-slim


RUN apt-get update && apt-get install -y sudo curl ssh
RUN adduser --disabled-password --home /home/dpp dpp && adduser dpp dpp && \
    adduser dpp root && adduser dpp redis && adduser redis dpp && \
    mkdir -p /var/datapackages && chown dpp.dpp /var/datapackages -R && chmod -R a+r /var/datapackages && \
    mkdir -p /home/dpp/.ssh && chown dpp.dpp /home/dpp/.ssh -R && \
    chown dpp.dpp /var/redis -R && \
    chown dpp.dpp /var/log/redis -R && \
    chown dpp.dpp /var/run/dpp -R && \
    chmod 700 /home/dpp/.ssh && \
    echo '%root ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/root
RUN mkdir -p /usr/share/man/man1 && apt-get install -y antiword poppler-utils libmagic1 default-jre-headless

RUN pip install numpy && pip install textract==1.5.0 pyquery "rfc3986<1.0" filemagic tabula-py paramiko boto3

ADD ./ /

ADD .dpp-runners.tzabar /datapackage_pipelines_budgetkey/pipelines/
RUN mv /datapackage_pipelines_budgetkey/pipelines/.dpp-runners.tzabar /datapackage_pipelines_budgetkey/pipelines/dpp-runners.yaml

RUN chown dpp.dpp /datapackage_pipelines_budgetkey -R
RUN pip install -e /
RUN pip install -U -r /requirements-dev.txt

RUN apt-get install -y sudo unzip wget libglib2.0-0 && wget https://chromedriver.storage.googleapis.com/88.0.4324.96/chromedriver_linux64.zip && \
    unzip chromedriver_linux64.zip && \
    chmod +x chromedriver && mv chromedriver /usr/local/bin/ 

USER dpp

ENV PYTHONPATH=/
ENV DPP_PROCESSOR_PATH=/datapackage_pipelines_budgetkey/processors
ENV DPP_REDIS_HOST=localhost
ENV REDIS_USER=dpp
ENV REDIS_GROUP=dpp
ENV DPP_NUM_WORKERS=6

WORKDIR /datapackage_pipelines_budgetkey/pipelines/

EXPOSE 5000

ENTRYPOINT ["/startup.sh"]
