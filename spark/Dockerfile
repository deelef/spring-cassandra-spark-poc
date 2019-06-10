FROM bde2020/spark-worker:2.4.1-hadoop2.7
COPY --chown=502:1000 spark-cassandra-connector_2.11-2.4.1.jar /spark/jars
CMD ["/bin/bash", "/worker.sh"]
