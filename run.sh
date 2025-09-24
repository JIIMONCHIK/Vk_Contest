#!/bin/bash

echo "Запуск проекта..."

# Сборка и запуск контейнеров
docker-compose down
docker-compose build
docker-compose up -d

echo "Ожидание запуска сервисов..."
sleep 5

echo "Дашборд доступен по адресу: http://localhost:8000"
echo "API доступно по адресу: http://localhost:8000/top"