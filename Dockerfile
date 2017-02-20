FROM frictionlessdata/datapackage-pipelines:latest

ADD pipelines /pipelines

RUN apk add --update git && pip install -U git+https://github.com/frictionlessdata/tabulator-py@fix/gsheet-re

EXPOSE 5000

CMD ["server"]
