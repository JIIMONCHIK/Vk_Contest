import os
import psycopg2
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def update_top_users():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Очистка и обновление витрины
            update_query = """
            TRUNCATE TABLE top_users_by_posts;
            
            INSERT INTO top_users_by_posts (user_id, posts_cnt, calculated_at)
            SELECT 
                user_id,
                COUNT(*) as posts_cnt,
                %s as calculated_at
            FROM raw_users_by_posts
            GROUP BY user_id
            ORDER BY posts_cnt DESC;
            """
            
            cur.execute(update_query, (datetime.now(),))
            conn.commit()
            
            # Получение статистики для логов
            cur.execute("SELECT COUNT(*) FROM top_users_by_posts")
            count = cur.fetchone()[0]
            logger.info(f"Обновлена витрина: {count} пользователей")
            
    except Exception as e:
        logger.error(f"Ошибка при обновлении витрины: {e}")
        conn.rollback()
    finally:
        conn.close()


def main():
    try:
        update_top_users()
        logger.info("Transform завершен успешно")
    except Exception as e:
        logger.error(f"Transform завершен с ошибкой: {e}")


if __name__ == "__main__":
    main()