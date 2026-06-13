FROM bitnamilegacy/spark:3.5.1

USER root

RUN pip install --no-cache-dir \
    pandas \
    numpy \
    scikit-learn \
    joblib \
    pyarrow \
    cassandra-driver \
    pyyaml

# Create alerts directory and set permissions for non-root user
RUN mkdir -p /app/alerts && chmod 777 /app/alerts

USER 1001
