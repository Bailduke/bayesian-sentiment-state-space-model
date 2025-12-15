from typing import List
import pandas as pd
import psycopg2
import logging
import news_classifier.sentiment.finbert as finbert
from news_classifier.sentiment.database import ensure_sentiment_table, insert_sentiment_rows
from news_classifier.utils import timeit, get_db_news

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@timeit
def build_sentiment_dataframe(news: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a sentiment DataFrame with columns:
      ['channel', 'id', 'positive', 'neutral', 'negative']
    from the input news DataFrame. Expects 'text', 'channel' and 'id' columns.
    """
    if news.empty:
        return pd.DataFrame()
    
    required = {'text', 'channel', 'id'}
    missing = required - set(news.columns)
    if missing:
        raise ValueError(f"Input DataFrame must contain columns: {sorted(missing)}")
    texts = news['text'].astype(str).tolist()
    tokenizer, model = finbert.load_model()
    probs_list = finbert.classify(texts, tokenizer, model, only_probs=True)
    probs_df = pd.DataFrame(probs_list)
    # Normalize keys to lowercase and ensure all expected columns exist
    probs_df.columns = [str(c).lower() for c in probs_df.columns]
    for col in ('positive', 'neutral', 'negative'):
        if col not in probs_df.columns:
            probs_df[col] = 0.0
    probs_df = probs_df[['positive', 'neutral', 'negative']].reset_index(drop=True)
    out = pd.concat(
        [news[['channel', 'id']].reset_index(drop=True), probs_df],
        axis=1
    )
    return out

def main() -> None:
    db_path = "postgresql://ian@localhost:5432/telegram_news"
    # Get news to process
    conn = psycopg2.connect(db_path)
    ensure_sentiment_table(conn)
    df = get_db_news(conn, table="message_sentiment")
    conn.close()
    logger.info(f"Got {len(df)} news rows")
    
    # process sentiment
    df_sentiment = build_sentiment_dataframe(df)
    logger.info(f"Built {len(df_sentiment)} sentiment rows")

    # Insert sentiment rows to the sentiment table
    conn = psycopg2.connect(db_path)
    n_inserted = insert_sentiment_rows(conn, df_sentiment)
    conn.close()
    logger.info(f"Sentiment rows inserted: {n_inserted}")

if __name__ == "__main__":
    # Example: load from DB, enrich, and write back to the same table (requires appropriate schema)
    main()