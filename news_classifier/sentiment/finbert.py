from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
from typing import List, Dict, Tuple, Optional
import contextlib

def load_model():
    path = "/home/ian/ai_models/finbert"
    try:
        tokenizer = AutoTokenizer.from_pretrained(path)
        model = AutoModelForSequenceClassification.from_pretrained(path)
        return tokenizer, model
    except Exception as e:
        print(f"Error loading model: {e}")
        return None, None

def get_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")

def _ensure_id2label(model) -> Dict[int, str]:
    """
    Ensure we have a mapping from class id to label.
    If not provided by the checkpoint, create a generic mapping.
    """
    id2label = getattr(model.config, "id2label", None)
    if isinstance(id2label, dict) and len(id2label) > 0:
        # Keys may be strings in some checkpoints; normalize to int keys
        return {int(k): v for k, v in id2label.items()}
    num_labels = getattr(model.config, "num_labels", 3)
    # Common FinBERT order is {0: 'negative', 1: 'neutral', 2: 'positive'}
    default_labels = {0: "NEGATIVE", 1: "NEUTRAL", 2: "POSITIVE"}
    # If num_labels is unexpected, generate generic names
    if num_labels != 3:
        return {i: f"LABEL_{i}" for i in range(num_labels)}
    return default_labels

def predict_proba(
    texts: List[str],
    tokenizer: AutoTokenizer,
    model: AutoModelForSequenceClassification,
    device: Optional[torch.device] = None,
    max_length: int = 128,
    batch_size: int = 64,
    amp_dtype: Optional[str] = None,
) -> List[Dict[str, float]]:
    """
    Returns class probabilities for each input text as a dict: {label: prob}.
    Processes texts in batches to avoid GPU OOM.
    """
    if tokenizer is None or model is None:
        raise ValueError("Tokenizer and model must be loaded before prediction.")
    if device is None:
        device = get_device()

    model = model.to(device)
    model.eval()
    
    id2label = _ensure_id2label(model)
    results: List[Dict[str, float]] = []
    # Determine autocast dtype policy
    _dtype = None
    if device.type == "cuda":
        if amp_dtype in ("fp16", "float16"):
            _dtype = torch.float16
        elif amp_dtype in ("bf16", "bfloat16"):
            _dtype = torch.bfloat16
    autocast_ctx = torch.amp.autocast("cuda", dtype=_dtype) if _dtype is not None else contextlib.nullcontext()

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        enc = tokenizer(
            batch,
            padding=True,
            truncation=True,
            max_length=max_length,
            return_tensors="pt",
        )
        enc = {k: v.to(device) for k, v in enc.items()}
        with torch.no_grad():
            with autocast_ctx:
                out = model(**enc)
                logits = out.logits  # [batch, num_labels]
                probs = torch.softmax(logits, dim=-1)  # [batch, num_labels]
        for row in probs.cpu():
            d = {id2label[i]: float(row[i]) for i in range(row.numel())}
            results.append(d)
        if device.type == "cuda":
            torch.cuda.empty_cache()
    return results

def classify(
    texts: List[str],
    tokenizer: AutoTokenizer,
    model: AutoModelForSequenceClassification,
    device: Optional[torch.device] = None,
    max_length: int = 128,
    batch_size: int = 64,
    amp_dtype: Optional[str] = None,
    only_probs: bool = False,
) -> List[Tuple[str, float, Dict[str, float]]]:
    """
    Classify texts and return:
      - top_label (str)
      - top_score (float)
      - full_probs ({label: prob})
    """
    probs_list = predict_proba(
        texts=texts,
        tokenizer=tokenizer,
        model=model,
        device=device,
        max_length=max_length,
        batch_size=batch_size,
        amp_dtype=amp_dtype,
    )
    if only_probs:
        return probs_list
    results: List[Tuple[str, float, Dict[str, float]]] = []
    for prob_dict in probs_list:
        top_label, top_score = max(prob_dict.items(), key=lambda kv: kv[1])
        results.append((top_label, top_score, prob_dict))
    return results

def classify_one(
    text: str,
    tokenizer: AutoTokenizer,
    model: AutoModelForSequenceClassification,
    device: Optional[torch.device] = None,
    max_length: int = 128,
    batch_size: int = 64,
    amp_dtype: Optional[str] = None,
) -> Tuple[str, float, Dict[str, float]]:
    """
    Convenience wrapper for single-text classification.
    """
    return classify(
        [text],
        tokenizer,
        model,
        device=device,
        max_length=max_length,
        batch_size=batch_size,
        amp_dtype=amp_dtype,
    )[0]

if __name__ == "__main__":
    tokenizer, model = load_model()
    text = "I love cookies"
    result = classify_one(text, tokenizer, model)
    print(result)
    text = "I hate when I can not eat my boyfriend's cookies"
    result = classify_one(text, tokenizer, model)
    print(result)
    text = "Can I perforate your cookies with a knife?"
    result = classify_one(text, tokenizer, model)
    print(result)