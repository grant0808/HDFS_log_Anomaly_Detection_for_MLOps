# Kubernetes Layout

Production deployments are driven by `helm/hdfs-log-anomaly` and `argocd/application.yaml`.
The requested subsystem folders are represented here as ownership boundaries for values,
secrets, and chart overlays:

- kafka
- flink
- fastapi
- postgres
- clickhouse
- redis
- prometheus
- grafana
- mlflow
- airflow
- evidently
- ingress
