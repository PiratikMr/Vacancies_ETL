FROM bitnami/spark:3.5.5

USER root

RUN echo '*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet' > ${SPARK_HOME}/conf/metrics.properties && \
    echo '*.sink.prometheusServlet.path=/metrics/prometheus' >> ${SPARK_HOME}/conf/metrics.properties