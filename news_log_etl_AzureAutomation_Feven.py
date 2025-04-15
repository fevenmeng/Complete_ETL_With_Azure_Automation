import requests
import pandas as pd
import psycopg2
from datetime import datetime

# PostgreSQL connection details
db_params = {
   

    "dbname": "postgres",
    "user": "Your_UserName",
    "password": "Your_Password",
    "host": "Your_HostAddress",
    "port": "5432"
}

# Your NewsAPI key
api_key="Your_api_key"

# NewsAPI categories
categories = [
    "business", "entertainment", "general",
    "health", "science", "sports", "technology"
]

# Timestamp for this script run
current_time = datetime.now()
fetch_time = current_time
fetch_date = current_time.date()
fetch_hour = current_time.hour
weekday_name = current_time.strftime('%A')

conn = None
cursor = None
total_inserted = 0  # Track inserted rows

try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO news_log_feven (
        source_id, source_name, author, title, description, url, published_at,
        category, fetch_time, fetch_date, fetch_hour, weekday_name
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (url) DO NOTHING;
    """

    for category in categories:
        print(f"\n📡 Fetching articles for category: {category}")
        page = 1  # Reset page for each category

        while True:
            url = (
                f"https://newsapi.org/v2/top-headlines?country=us&category={category}&apiKey={api_key}&pageSize=100&page={page}"
            )
            response = requests.get(url)

            # Check for 401 Unauthorized status
            if response.status_code == 401:
                print(f"❌ Unauthorized API key for category {category}. Check the API key.")
                break

            # Check for 426 Upgrade Required
            if response.status_code == 426:
                print(f"⚠️ Upgrade required for {category}. Server responded with 426.")
                break

            if response.status_code != 200:
                print(f"❌ Failed to fetch data for {category}: {response.status_code}")
                break

            data = response.json()
            articles = data.get("articles", [])
            if not articles:
                print(f"✅ No more articles found for {category}.")
                break

            df = pd.DataFrame(articles)
            df.drop_duplicates(subset='url', inplace=True)

            df['category'] = category
            df['fetch_time'] = fetch_time
            df['fetch_date'] = fetch_date
            df['fetch_hour'] = fetch_hour
            df['weekday_name'] = weekday_name

            print(f"📥 Inserting {len(df)} articles (Page {page})")

            for _, row in df.iterrows():
                url = row.get("url", "")
                cursor.execute("SELECT 1 FROM news_log_feven WHERE url = %s", (url,))
                if cursor.fetchone():
                    continue

                source = row.get("source", {})
                cursor.execute(insert_query, (
                    source.get("id", ""),
                    source.get("name", ""),
                    row.get("author", ""),
                    row.get("title", ""),
                    row.get("description", ""),
                    url,
                    row.get("publishedAt", None),
                    category,
                    fetch_time,
                    fetch_date,
                    fetch_hour,
                    weekday_name
                ))

                total_inserted += 1

            conn.commit()
            page += 1

    if total_inserted > 0:
        print(f"\n✅ Successfully inserted {total_inserted} new articles.")
    else:
        print("\n⚠️ No new articles were inserted.")

except Exception as e:
    print("❌ Database error:", e)

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()


