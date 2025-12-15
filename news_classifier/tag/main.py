from news_classifier.utils import timeit, get_db_news
import logging
import pandas as pd
import psycopg2
from news_classifier.tag.database import ensure_tag_table, insert_tag_rows
from news_classifier.tag.bart_large_mnli import load_model, get_device, zero_shot_top_k

logger = logging.getLogger(__name__)

LABELS = [
    "economics, finance and markets",
    "corporate, business, industry and innovation",
    "technology, ai and digital platforms",
    "geopolitics, war, security and international relations",
    "domestic politics, elections and government",
    "energy, commodities and environment",
    "society, human rights and public health",
    "sports, entertainment and culture",
]

def _norm(label: str) -> str:
    return label.replace(",", "").replace(" ", "_").lower()

@timeit
def build_tag_dataframe(news: pd.DataFrame, amp_dtype: str | None = "bf16") -> pd.DataFrame:
    """
    Build a DataFrame with scores per label (one column per label) for each message.
    Output columns: ['channel','id'] + normalized label columns
    """
    if news.empty:
        return pd.DataFrame()
    if not {"channel", "id", "text"}.issubset(news.columns):
        raise ValueError("Input DataFrame must have 'channel','id','text' columns")

    tokenizer, model = load_model()
    device = get_device()
    # Get all scores by setting k=len(LABELS)
    pairs_list = zero_shot_top_k(
        news["text"].astype(str).tolist(),
        candidate_labels=LABELS,
        k=len(LABELS),
        multi_label=True,
        tokenizer=tokenizer,
        model=model,
        device=device,
        amp_dtype=amp_dtype,
        batch_size=8,
    )
    # Build scores matrix
    norm_cols = [_norm(l) for l in LABELS]
    scores_df = pd.DataFrame(0.0, index=range(len(pairs_list)), columns=norm_cols, dtype=float)
    for i, pairs in enumerate(pairs_list):
        for label, score in pairs:
            scores_df.at[i, _norm(label)] = float(score)
    out = pd.concat([news[["channel", "id"]].reset_index(drop=True), scores_df], axis=1)
    return out

def main() -> None:
    db_dsn = "postgresql://ian@localhost:5432/telegram_news"
    conn = psycopg2.connect(db_dsn)
    ensure_tag_table(conn)
    channels = [
    "https://t.me/cnbc_tv18",
    "https://t.me/BBCWorld",
    "https://t.me/nytimes",
    "https://t.me/ReutersWorldChannel",
    "https://t.me/washingtonpost",
]
    # Get messages without tags yet # 1st gen 2024: 1704063600 #1st may 2024: 1714521600
    df_news = get_db_news(conn, table="message_tag", min_unix_time=1704063600, channels=channels)
    conn.close()
    logger.info(f"Fetched {len(df_news)} news rows to tag")
    df_tags = build_tag_dataframe(df_news)
    logger.info(f"Built tags for {len(df_tags)} rows")
    if df_tags.empty:
        return
    conn = psycopg2.connect(db_dsn)
    n = insert_tag_rows(conn, df_tags)
    conn.close()
    logger.info(f"Inserted/updated {n} tag rows")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

