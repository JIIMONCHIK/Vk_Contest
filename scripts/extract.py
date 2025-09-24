import os
import requests
import psycopg2
import logging
from time import sleep
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

API_URL = os.getenv('API_URL', 'https://jsonplaceholder.typicode.com/posts')
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'db'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

# logger.info(f"DB connection config: host={DB_CONFIG['host']}, db={DB_CONFIG['database']}, user={DB_CONFIG['user']}")

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_posts(max_retries=3, delay=5):
    for attempt in range(max_retries):
        try:
            logger.info(f"Попытка {attempt + 1} получения данных из API")
            response = requests.get(API_URL, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Ошибка сети: {e}")
            if attempt < max_retries - 1:
                sleep(delay)
            else:
                raise


def insert_posts(posts):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Вставка с игнорированием дубликатов
            insert_query = """
            INSERT INTO raw_users_by_posts (post_id, user_id, title, body, extracted_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (post_id) DO NOTHING
            """
            
            data = [
                (post['id'], post['userId'], post['title'], 
                 post['body'], datetime.now())
                for post in posts
            ]
            
            cur.executemany(insert_query, data)
            conn.commit()
            logger.info(f"Успешно обработано {len(posts)} постов")
            
    except Exception as e:
        logger.error(f"Ошибка при вставке данных: {e}")
        conn.rollback()
    finally:
        conn.close()


def main():
    try:
        posts = fetch_posts()
        insert_posts(posts)
        logger.info("Extract завершен успешно")
    except Exception as e:
        logger.error(f"Extract завершен с ошибкой: {e}")


if __name__ == "__main__":
    main()