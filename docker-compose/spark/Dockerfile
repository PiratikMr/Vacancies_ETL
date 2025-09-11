FROM bitnami/spark:3.5.5

USER root

RUN cat > ${SPARK_HOME}/conf/metrics.properties << 'EOF'
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
EOF