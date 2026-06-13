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

USER 1001
