import argparse
import sys
from pathlib import Path

def upload_to_gcs(bucket_name: str, local_dir: Path) -> None:
    try:
        from google.cloud import storage
    except ImportError:
        print("Error: google-cloud-storage is not installed.", file=sys.stderr)
        print("Please run: pip install google-cloud-storage", file=sys.stderr)
        sys.exit(1)

    model_file = local_dir / "model.pt"
    vocab_file = local_dir / "vocab.json"

    if not model_file.exists():
        print(f"Error: {model_file} does not exist. Please train the model first.", file=sys.stderr)
        sys.exit(1)
    if not vocab_file.exists():
        print(f"Error: {vocab_file} does not exist.", file=sys.stderr)
        sys.exit(1)

    print(f"Initializing GCS Client for bucket: {bucket_name}")
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket_name)
    except Exception as exc:
        print(f"Error accessing bucket '{bucket_name}': {exc}", file=sys.stderr)
        sys.exit(1)

    # Upload model.pt
    print(f"Uploading {model_file.name} to gs://{bucket_name}/models/model.pt...")
    model_blob = bucket.blob("models/model.pt")
    model_blob.upload_from_filename(str(model_file))

    # Upload vocab.json
    print(f"Uploading {vocab_file.name} to gs://{bucket_name}/models/vocab.json...")
    vocab_blob = bucket.blob("models/vocab.json")
    vocab_blob.upload_from_filename(str(vocab_file))

    print("🎉 Upload completed successfully! Model is ready to be loaded by K8s pods.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload trained DeepLog artifacts to GCP Cloud Storage (GCS)")
    parser.add_argument("--bucket", required=True, help="Target GCS bucket name (e.g. hdfs-model-artifacts)")
    parser.add_argument("--local-dir", type=Path, default=Path("artifacts"), help="Local directory containing model.pt and vocab.json")
    args = parser.parse_args()

    upload_to_gcs(args.bucket, args.local_dir)


if __name__ == "__main__":
    main()
