import argparse
import json
from pathlib import Path

import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

from app.model.deeplog import DeepLogLSTM


def build_sequences(events: list[str], window_size: int) -> tuple[list[list[str]], list[str]]:
    sequences: list[list[str]] = []
    labels: list[str] = []
    for idx in range(len(events) - window_size):
        sequences.append(events[idx : idx + window_size])
        labels.append(events[idx + window_size])
    return sequences, labels


def train(input_path: Path, output_dir: Path, window_size: int = 10, epochs: int = 3) -> None:
    events = [line.strip().split()[-1] for line in input_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    vocab_events = sorted(set(events))
    event_to_idx = {event: idx for idx, event in enumerate(vocab_events)}
    sequences, labels = build_sequences(events, window_size)
    if not sequences:
        raise ValueError("Not enough events to train DeepLog")
    x = torch.tensor([[event_to_idx[event] for event in seq] for seq in sequences], dtype=torch.long)
    y = torch.tensor([event_to_idx[event] for event in labels], dtype=torch.long)
    model = DeepLogLSTM(vocab_size=len(event_to_idx))
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    loss_fn = nn.CrossEntropyLoss()
    loader = DataLoader(TensorDataset(x, y), batch_size=32, shuffle=True)
    for _ in range(epochs):
        for batch_x, batch_y in loader:
            optimizer.zero_grad()
            loss = loss_fn(model(batch_x), batch_y)
            loss.backward()
            optimizer.step()
    output_dir.mkdir(parents=True, exist_ok=True)
    torch.save(model.state_dict(), output_dir / "model.pt")
    (output_dir / "vocab.json").write_text(json.dumps({"event_to_idx": event_to_idx}, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/sample/hdfs_events.txt"))
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts"))
    parser.add_argument("--window-size", type=int, default=10)
    parser.add_argument("--epochs", type=int, default=3)
    args = parser.parse_args()
    train(args.input, args.output_dir, args.window_size, args.epochs)


if __name__ == "__main__":
    main()
