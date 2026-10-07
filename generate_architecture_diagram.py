#!/usr/bin/env python3
"""
Generate the final on-prem MLOps architecture diagram.

Install Python package:
    pip install graphviz

The Graphviz executable is also required. If `dot --version` works,
the script can generate SVG and PNG files.
"""

from graphviz import Digraph


def build_diagram():
    dot = Digraph(
        "on_prem_mlops",
        graph_attr={
            "rankdir": "LR",
            "splines": "ortho",
            "nodesep": "0.55",
            "ranksep": "0.9",
            "pad": "0.4",
            "bgcolor": "white",
            "fontname": "Helvetica",
            "label": "Production-Grade On-Prem MLOps Platform",
            "labelloc": "t",
            "fontsize": "24",
        },
        node_attr={
            "shape": "box",
            "style": "rounded",
            "fontname": "Helvetica",
            "fontsize": "11",
            "margin": "0.16,0.10",
        },
        edge_attr={
            "fontname": "Helvetica",
            "fontsize": "9",
            "color": "#555555",
            "arrowsize": "0.7",
        },
    )

    # Sources
    with dot.subgraph(name="sources") as c:
        c.attr(label="Data Sources", color="#777777", style="rounded")
        c.node("sensor", "Sensor Simulator\\nEvery 5 minutes")
        c.node("api", "REST API")
        c.node("csv", "CSV / Historical Data")
        c.node("sqlite", "SQLite / Legacy Data")
        c.node("producer", "Kafka Producer")
        c.edge("sensor", "producer")
        c.edge("api", "producer")

    # Kafka
    with dot.subgraph(name="kafka_cluster") as c:
        c.attr(label="Streaming Ingestion", color="#777777", style="rounded")
        c.node(
            "kafka",
            "KAFKA\\nraw-sensor-readings\\n"
            "Partitions | Offsets | Retention | Replay",
        )
        c.node("consumer", "Consumer Group")
        c.edge("producer", "kafka")
        c.edge("kafka", "consumer")

    # ETL
    with dot.subgraph(name="etl") as c:
        c.attr(label="Streaming ETL / Data Quality", color="#777777", style="rounded")
        c.node("spark", "Spark Structured Streaming")
        c.node("schema", "Schema Validation")
        c.node("quality", "Data Quality\\nRanges + Business Rules")
        c.node("dedup", "Deduplication + Idempotency")
        c.node("event_time", "Event Time\\nWatermarks + Late Events")
        c.node("checkpoint", "Checkpointing + Recovery")
        c.node("dlq", "DLQ / Quarantine\\nOriginal Event + Reason")

        c.edge("consumer", "spark")
        c.edge("spark", "schema")
        c.edge("schema", "quality")
        c.edge("quality", "dedup")
        c.edge("dedup", "event_time")
        c.edge("event_time", "checkpoint")
        c.edge("quality", "dlq", label="invalid / suspicious")

    # Storage
    with dot.subgraph(name="storage") as c:
        c.attr(label="On-Prem Data Lake", color="#777777", style="rounded")
        c.node("raw", "RAW\\nImmutable Events")
        c.node("validated", "VALIDATED")
        c.node("curated", "CURATED\\nML / Analytics Ready")
        c.node("partitioned", "Partitioned Parquet\\nIncremental Storage")

        c.edge("spark", "raw", label="preserve original")
        c.edge("checkpoint", "validated")
        c.edge("validated", "curated")
        c.edge("curated", "partitioned")

    # Airflow
    with dot.subgraph(name="airflow") as c:
        c.attr(label="Airflow Orchestration", color="#777777", style="rounded")
        c.node("airflow", "AIRFLOW")
        c.node("training", "TRAINING DAG\\nCurated Data → Features → PySpark")
        c.node("inference", "INFERENCE DAG\\nCurated Data → Champion → Predictions")
        c.node("monitoring", "MONITORING DAG\\nQuality + Drift + Model Performance")

        c.edge("airflow", "training")
        c.edge("airflow", "inference")
        c.edge("airflow", "monitoring")
        c.edge("training", "inference", label="dependency")
        c.edge("inference", "monitoring", label="dependency")

    # MLflow
    with dot.subgraph(name="mlflow") as c:
        c.attr(label="ML Lifecycle", color="#777777", style="rounded")
        c.node("mlflow_node", "MLflow\\nExperiments + Metrics + Artifacts")
        c.node("registry", "Model Registry\\nVersioning + Promotion")
        c.node("champion", "@champion\\nProduction Model")
        c.node("predictions", "Prediction Store")

        c.edge("mlflow_node", "registry")
        c.edge("registry", "champion")
        c.edge("champion", "predictions")

    # Observability / operations
    with dot.subgraph(name="ops") as c:
        c.attr(label="Observability / Reliability", color="#777777", style="rounded")
        c.node(
            "observability",
            "Observability\\n"
            "Throughput | Latency | Consumer Lag\\n"
            "Data Quality | Failures | Model Metrics",
        )
        c.node("replay", "Replay / Reprocessing / Backfills")
        c.node("failure", "Failure Testing\\nCrash | Retry | Recovery")

    # Historical sources
    dot.edge("csv", "raw", label="historical")
    dot.edge("sqlite", "raw", label="historical")

    # Data to MLOps
    dot.edge("partitioned", "training")
    dot.edge("partitioned", "inference")

    # Training/model lifecycle
    dot.edge("training", "mlflow_node")
    dot.edge("champion", "inference")
    dot.edge("inference", "predictions")
    dot.edge("predictions", "monitoring")
    dot.edge("monitoring", "mlflow_node")

    # Operational observability
    dot.edge("kafka", "observability", style="dashed")
    dot.edge("spark", "observability", style="dashed")
    dot.edge("airflow", "observability", style="dashed")
    dot.edge("mlflow_node", "observability", style="dashed")

    # Replay and recovery
    dot.edge("kafka", "replay", style="dashed")
    dot.edge("raw", "replay", style="dashed")
    dot.edge("replay", "spark", style="dashed")
    dot.edge("failure", "consumer", style="dashed")
    dot.edge("failure", "checkpoint", style="dashed")

    return dot


def main():
    diagram = build_diagram()

    svg = diagram.render(
        filename="on_prem_mlops_architecture",
        format="svg",
        cleanup=True,
    )

    png = diagram.render(
        filename="on_prem_mlops_architecture",
        format="png",
        cleanup=True,
    )

    print(f"Generated: {svg}")
    print(f"Generated: {png}")


if __name__ == "__main__":
    main()
