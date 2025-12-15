## BayesianNewsSentiment

Custom Python + PostgreSQL pipeline to collect news from official Telegram media channels, score each message with NLP models (FinBERT for sentiment; BART-large-MNLI for topic tagging), and export an aggregated dataset used by the paper: *From Noisy News Sentiment Scores to Interpretable Temporal Dynamics:A Bayesian State-Space Model*.

### Overview
- **Ingestion (Telegram)**: fetch messages from selected set of Telegram channels into a relational DB (`messages`).
- **Scoring (AI/NLP)**:
  - **Sentiment**: FinBERT → `positive`, `neutral`, `negative` per message → table `message_sentiment`.
  - **Tagging**: BART-large-MNLI zero-shot → relevance scores per predefined category → table `message_tag`.
- **Dataset**: export a CSV joining messages + sentiment + tags for weekly/category analysis used in the paper.

The R paper uses the exported CSV and produces figures and inference via a Bayesian state-space model.

---

## Requirements
- Python 3.10+
- PostgreSQL 13+ (tested locally)
- Telegram API credentials (via Telethon)
- Recommended: a virtual environment

Install Python deps:

```bash
cd /home/user/Projects/BayesianNewsSentiment
pip install -r requirements.txt
```

---

## Database
Default DSN in scripts: `postgresql://user@localhost:5432/telegram_news`. Adjust in the code or export a compatible DSN and change references as needed.

Tables created/used by the pipeline:
- `messages(channel TEXT, id BIGINT, date_unix BIGINT, text TEXT, …)`
- `message_sentiment(channel TEXT, id BIGINT, positive REAL, neutral REAL, negative REAL, …)`
- `message_tag(channel TEXT, id BIGINT, <one column per normalized category label>, …)`

Table creation is handled by helper functions inside the modules (see `news_classifier/telegram_news/database.py`, `news_classifier/sentiment/database.py`, `news_classifier/tag/database.py`).

---

## Telegram News Ingestion
Fetch messages from channels into `messages` using Telethon.

1) Create `.env` in `news_classifier/telegram_news/` with your Telegram API credentials:

```ini
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_PHONE=+34123456789        # optional, can be empty
TELEGRAM_SESSION_NAME=telegram_news # optional
```

2) List channels in `news_classifier/telegram_news/channels.txt` (one per line, or full t.me URLs).

3) Run the ingestor:

```bash
python -m news_classifier.telegram_news.main --channels news_classifier/telegram_news/channels.txt --limit 1000
```

This will:
- Ensure `messages` exists.
- For each channel, fetch new messages since the last saved id.
- Apply a keyword filter (see `news_classifier/telegram_news/keywords_filter.py`).
- Insert rows into `messages`.

---

## Messages AI Scoring

### Sentiment (FinBERT)
Builds and writes `positive`, `neutral`, `negative` to `message_sentiment`.

```bash
python -m news_classifier.sentiment.main
```

Inputs: messages without sentiment yet.  
Outputs: rows in `message_sentiment` keyed by `(channel, id)`.

### Tagging (BART-large-MNLI)
Zero-shot classification over predefined categories; writes per-category relevance scores to `message_tag`.

```bash
python -m news_classifier.tag.main
```

- Categories are defined in `news_classifier/tag/main.py` (`LABELS`). They are normalized to snake_case for DB columns.

---

## Dataset Export (for the paper)
Join `messages` + `message_sentiment` + `message_tag` and export CSV:

```bash
python dataset.py
# or
python -m news_classifier.create_dataset
```

Adjust date filters/labels in the script(s) as needed. The resulting `dataset.csv` is used in the paper.

---

## License
MIT. See the `LICENSE` file for details.