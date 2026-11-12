
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
METRICS_FILE = BASE_DIR / "monitoring" / "performance_metrics.csv"


def main():

    print("\n" + "=" * 60)
    print("          CRYPTO KAFKA PERFORMANCE REPORT")
    print("=" * 60)

    if not METRICS_FILE.exists():
        print("Performance metrics file not found!")
        return

    df = pd.read_csv(METRICS_FILE)

    if df.empty:
        print("No performance records available.")
        return

    # Convert metric columns safely
    numeric_columns = [
        "total_messages",
        "anomalies",
        "current_throughput",
        "average_throughput",
        "average_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "average_processing_ms"
    ]

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column], errors="coerce"
            )

    # Remove startup records with no processed messages
    if "total_messages" in df.columns:
        valid_df = df[df["total_messages"] > 0].copy()
    else:
        print("Missing total_messages column!")
        return

    if valid_df.empty:
        print("No valid performance measurements yet.")
        return

    print("\nTOTAL SNAPSHOTS:", len(df))
    print("VALID SNAPSHOTS:", len(valid_df))

    print("\nLATEST PERFORMANCE MEASUREMENT")

    latest = valid_df.iloc[-1]

    for column in numeric_columns:
        if column in valid_df.columns:
            print(f"{column}: {latest[column]}")

    print("\nAVERAGE PERFORMANCE")

    for column in numeric_columns:
        if column in valid_df.columns:
            print(
                f"{column}: "
                f"{valid_df[column].mean():.2f}"
            )

    print("\nMAXIMUM THROUGHPUT:",
          f"{valid_df['current_throughput'].max():.2f} msg/sec"
          if "current_throughput" in valid_df else "N/A")

    print("\n" + "=" * 60)
    print("          PERFORMANCE TEST COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    main()
