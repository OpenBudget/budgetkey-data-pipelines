FROM frictionlessdata/datapackage-pipelines:latest

ADD . /pipelines

EXPOSE 5000

CMD ["server"]
