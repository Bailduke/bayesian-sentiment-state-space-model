from psycopg2.extensions import connection as PGConnection
import psycopg2
import pandas as pd

def last_news(conn: PGConnection, min_unix_time):
    query = """
    SELECT 

    m.channel, m.id, m.date_unix, 
    s.positive, s.neutral, s.negative, 
    t.economics_finance_and_markets,
    t.corporate_business_industry_and_innovation,
    t.technology_ai_and_digital_platforms,
    t.geopolitics_war_security_and_international_relations,
    t.domestic_politics_elections_and_government,
    t.energy_commodities_and_environment,
    t.society_human_rights_and_public_health,
    t.sports_entertainment_and_culture
    FROM (
        SELECT * FROM messages WHERE date_unix >= %s
    ) m 
    LEFT JOIN message_sentiment s ON m.channel = s.channel AND m.id = s.id
    RIGHT JOIN message_tag t ON m.channel = t.channel AND m.id = t.id;
    """
    cursor = conn.cursor()
    cursor.execute(query, (min_unix_time,))
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    return pd.DataFrame(rows, columns=columns)

def interval_grouping(df_news, t_increment):
    ...

if __name__ == "__main__":
    t_increment = 60*60*24*7 # 1 week in seconds
    db_dsn = "postgresql://ian@localhost:5432/telegram_news"
    conn = psycopg2.connect(db_dsn)
    df_news = last_news(conn, 1704063600)
    #df = interval_grouping(df_news, t_increment)
    conn.close()
    df_news.to_csv("dataset_news.csv", index=False)
    print(df_news.head())