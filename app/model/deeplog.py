import json
from pathlib import Path

class DeepLogLSTM:
    def __init__(self, vocab_size: int, embedding_dim: int = 32, hidden_size: int = 64) -> None:
        import torch
        from torch import nn

        class _DeepLogLSTM(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.embedding = nn.Embedding(vocab_size, embedding_dim)
                self.lstm = nn.LSTM(embedding_dim, hidden_size, batch_first=True)
                self.classifier = nn.Linear(hidden_size, vocab_size)

            def forward(self, x: torch.Tensor) -> torch.Tensor:
                embedded = self.embedding(x)
                output, _ = self.lstm(embedded)
                return self.classifier(output[:, -1, :])

        self.module = _DeepLogLSTM()

    def __call__(self, x):
        return self.module(x)

    def load_state_dict(self, state_dict) -> None:
        self.module.load_state_dict(state_dict)

    def state_dict(self):
        return self.module.state_dict()

    def parameters(self):
        return self.module.parameters()

    def eval(self) -> None:
        self.module.eval()

    @staticmethod
    def load_tensor(path: Path):
        import torch

        return torch.load(path, map_location="cpu")

    @staticmethod
    def tensor(values):
        import torch

        return torch.tensor(values, dtype=torch.long)

    @staticmethod
    def softmax(logits):
        import torch

        return torch.softmax(logits, dim=-1)

    @staticmethod
    def topk(probs, k: int):
        import torch

        return torch.topk(probs, k)


class DeepLogModel:
    def __init__(self, model_path: Path, vocab_path: Path, top_k: int = 3) -> None:
        self.model_path = model_path
        self.vocab_path = vocab_path
        self.top_k = top_k
        self.event_to_idx: dict[str, int] = {}
        self.idx_to_event: dict[int, str] = {}
        self.model: DeepLogLSTM | None = None
        self.available = False
        self._load()

    def _load(self) -> None:
        if not self.vocab_path.exists() or not self.model_path.exists():
            return
        vocab = json.loads(self.vocab_path.read_text(encoding="utf-8"))
        self.event_to_idx = {str(k): int(v) for k, v in vocab["event_to_idx"].items()}
        self.idx_to_event = {v: k for k, v in self.event_to_idx.items()}
        self.model = DeepLogLSTM(vocab_size=len(self.event_to_idx))
        self.model.load_state_dict(DeepLogLSTM.load_tensor(self.model_path))
        self.model.eval()
        self.available = True

    def predict_next(self, sequence: list[str]) -> tuple[list[str], dict[str, float]]:
        if not self.available or self.model is None:
            raise RuntimeError("DeepLog artifact is unavailable")
        ids = [self.event_to_idx.get(event, 0) for event in sequence]
        import torch

        with torch.no_grad():
            logits = self.model(DeepLogLSTM.tensor([ids]))
            probs = DeepLogLSTM.softmax(logits).squeeze(0)
            values, indices = DeepLogLSTM.topk(probs, min(self.top_k, probs.numel()))
        predictions = [self.idx_to_event[int(idx)] for idx in indices]
        probabilities = {event: float(prob) for event, prob in zip(predictions, values, strict=True)}
        return predictions, probabilities
