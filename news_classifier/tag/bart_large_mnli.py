from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
import torch
from typing import List, Dict, Tuple, Optional
import contextlib

def load_model(model_name_or_path: str = "facebook/bart-large-mnli"):
    """
    Load tokenizer and model for zero-shot classification (BART MNLI).
    """
    try:
        tokenizer = AutoTokenizer.from_pretrained(model_name_or_path)
        model = AutoModelForSequenceClassification.from_pretrained(model_name_or_path)
        return tokenizer, model
    except Exception as e:
        print(f"Error loading model: {e}")
        return None, None

def get_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")

def _pipeline_from(
    tokenizer: AutoTokenizer,
    model: AutoModelForSequenceClassification,
    device: torch.device,
):
    if device.type == "cuda":
        device_idx = 0 
    else:
        device_idx = -1
    return pipeline(
        "zero-shot-classification",
        model=model,
        tokenizer=tokenizer,
        device=device_idx,
    )

def zero_shot_top_k(
    texts: List[str],
    candidate_labels: List[str],
    k: int = 3,
    multi_label: bool = True,
    tokenizer: Optional[AutoTokenizer] = None,
    model: Optional[AutoModelForSequenceClassification] = None,
    device: Optional[torch.device] = None,
    hypothesis_template: str = "This example is about {}.",
    batch_size: int = 16,
    amp_dtype: Optional[str] = None,
) -> List[List[Tuple[str, float]]]:
    """
    For each input text, return the top-k (label, score) pairs among candidate_labels.
    """
    if tokenizer is None or model is None:
        tokenizer, model = load_model()
    if device is None:
        device = get_device()
    pipe = _pipeline_from(tokenizer, model, device)
    # Prepare autocast if CUDA
    _dtype = None
    if device and device.type == "cuda":
        if amp_dtype in ("fp16", "float16"):
            _dtype = torch.float16
        elif amp_dtype in ("bf16", "bfloat16"):
            _dtype = torch.bfloat16
    autocast_ctx = torch.amp.autocast("cuda", dtype=_dtype) if _dtype is not None else contextlib.nullcontext()

    results: List[List[Tuple[str, float]]] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        with autocast_ctx:
            out = pipe(
                batch,
                candidate_labels=candidate_labels,
                multi_label=multi_label,
                hypothesis_template=hypothesis_template,
            )
        if isinstance(out, dict):
            out = [out]
        for o in out:
            labels = o["labels"]
            scores = o["scores"]
            pairs = list(zip(labels, scores))
            pairs = pairs[:k]
            results.append(pairs)
    return results

def zero_shot_one(
    text: str,
    candidate_labels: List[str],
    k: int = 3,
    multi_label: bool = True,
    tokenizer: Optional[AutoTokenizer] = None,
    model: Optional[AutoModelForSequenceClassification] = None,
    device: Optional[torch.device] = None,
    hypothesis_template: str = "This example is about {}.",
    amp_dtype: Optional[str] = None,
) -> List[Tuple[str, float]]:
    res = zero_shot_top_k(
        [text],
        candidate_labels=candidate_labels,
        k=k,
        multi_label=multi_label,
        tokenizer=tokenizer,
        model=model,
        device=device,
        hypothesis_template=hypothesis_template,
        batch_size=1,
        amp_dtype=amp_dtype,
    )
    return res[0]

if __name__ == "__main__":
    tokenizer, model = load_model("facebook/bart-large-mnli")
    labels_news = [
        "economics, finance and markets",
        "corporate, business, industry and innovation",
        "technology, ai and digital platforms",
        "geopolitics, war, security and international relations",
        "domestic politics, elections and government",
        "energy, commodities and environment",
        "society, human rights and public health",
        "sports, entertainment and culture",
    ]
    text = "Barcelona will change its manager for the Champions League."
    topk = zero_shot_one(text, labels_news, k=10, tokenizer=tokenizer, model=model)
    topk_amp = zero_shot_one(text, labels_news, k=10, tokenizer=tokenizer, model=model, amp_dtype="fp16")
    print(topk)
    print("---")
    print(topk_amp)