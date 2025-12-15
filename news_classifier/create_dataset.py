from psycopg2.extensions import connection as PGConnection
import psycopg2
import pandas as pd

def create_dataset(conn: PGConnection, min_unix_time):
    query = """
    SELECT m.channel, m.id, m.date_unix, 
    s.positive, s.neutral, s.negative, 
    t.macroeconomics_and_central_banks, t.geopolitics_war_and_international_security, t.domestic_politics_and_government_policy, t.financial_markets_assets_and_trading, t.technology_and_corporate_innovation, t.social_issues_protests_and_public_health, t.crypto_and_blockchain, t.entertainment_sports_and_lifestyle FROM (
        SELECT * FROM messages WHERE date_unix >= %s
    ) m 
    LEFT JOIN message_sentiment s ON m.channel = s.channel AND m.id = s.id
    LEFT JOIN message_tag t ON m.channel = t.channel AND m.id = t.id;
    """
    cursor = conn.cursor()
    cursor.execute(query, (min_unix_time,))
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    return pd.DataFrame(rows, columns=columns)

if __name__ == "__main__":
    db_dsn = "postgresql://ian@localhost:5432/telegram_news"
    conn = psycopg2.connect(db_dsn)
    df = create_dataset(conn, 1733007600)
    conn.close()
    df.to_csv("dataset.csv", index=False)
    print(df.head())