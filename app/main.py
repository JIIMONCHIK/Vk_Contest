from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import psycopg2
import os
import logging
from datetime import datetime

app = FastAPI(title="Top Users Dashboard")

templates = Jinja2Templates(directory="app/templates")

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_dashboard_stats():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Топ пользователей
            cur.execute("""
                SELECT user_id, posts_cnt, calculated_at 
                FROM top_users_by_posts 
                ORDER BY posts_cnt DESC 
                LIMIT 20
            """)
            users = [
                {
                    'user_id': row[0],
                    'posts_cnt': row[1],
                    'calculated_at': row[2].strftime('%Y-%m-%d %H:%M:%S')
                }
                for row in cur.fetchall()
            ]
            
            # Общая статистика
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT user_id) as total_users,
                    SUM(posts_cnt) as total_posts,
                    AVG(posts_cnt) as avg_posts,
                    MAX(calculated_at) as last_calculated
                FROM top_users_by_posts
            """)
            stats = cur.fetchone()
            
            return {
                'users': users,
                'total_users': stats[0] or 0,
                'total_posts': stats[1] or 0,
                'avg_posts': round(stats[2] or 0, 1),
                'last_calculated': stats[3].strftime('%Y-%m-%d %H:%M:%S') if stats[3] else 'Нет данных'
            }
            
    except Exception as e:
        logging.error(f"Ошибка при получении статистики: {e}")
        return {
            'users': [],
            'total_users': 0,
            'total_posts': 0,
            'avg_posts': 0,
            'last_calculated': 'Ошибка загрузки'
        }
    finally:
        conn.close()


# HTML дашборд с топом пользователей
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    stats = get_dashboard_stats()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "users": stats['users'],
            "total_users": stats['total_users'],
            "total_posts": stats['total_posts'],
            "avg_posts": stats['avg_posts'],
            "last_calculated": stats['last_calculated']
        }
    )


# JSON API для получения топа пользователей
@app.get("/top")
async def get_top_users(limit: int = 10):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id, posts_cnt, calculated_at 
                FROM top_users_by_posts 
                ORDER BY posts_cnt DESC 
                LIMIT %s
            """, (limit,))
            results = [
                {
                    'user_id': row[0],
                    'posts_cnt': row[1],
                    'calculated_at': row[2].isoformat()
                }
                for row in cur.fetchall()
            ]
        return {
            "total": len(results),
            "users": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM top_users_by_posts")
            count = cur.fetchone()[0]
        return {
            "status": "healthy",
            "database": "connected",
            "users_count": count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)