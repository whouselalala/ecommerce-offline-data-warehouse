FROM apache/spark:3.5.3-python3
USER root
WORKDIR /workspace
COPY . /workspace
RUN chmod -R a+rwX /workspace
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--conf", "spark.driver.memory=1g", "/workspace/warehouse/pipeline.py", "--date", "2026-10-01"]
