FROM apache/airflow:2.9.1

COPY requirements.txt requirements.txt

USER root
RUN apt-get update
RUN apt-get install -y libpq-dev gcc

COPY ./processing /opt/kitfloppa/processing
RUN chown -R airflow /opt/kitfloppa/processing

COPY ./utils /opt/kitfloppa/utils
RUN chown -R airflow /opt/kitfloppa/utils

USER airflow
RUN chmod +x /opt/kitfloppa/processing/rt-stps/bin/batch.sh
RUN chmod +x /opt/kitfloppa/processing/rt-stps/jre/bin/java

RUN pip install /opt/kitfloppa/utils

USER root
RUN rm -rf /opt/kitfloppa/utils
USER airflow

RUN pip install -r requirements.txt