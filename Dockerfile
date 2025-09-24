FROM python:3.9

RUN apt-get update && apt-get install -y cron postgresql-client && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN pip install -r app/requirements.txt

RUN touch /var/log/cron.log
RUN echo "*/5 * * * * root /usr/local/bin/python /app/scripts/extract.py >> /var/log/cron.log 2>&1" >> /etc/cron.d/extract-cron
RUN echo "10 * * * * root /usr/local/bin/python /app/scripts/transform.py >> /var/log/cron.log 2>&1" >> /etc/cron.d/extract-cron
RUN crontab /etc/cron.d/extract-cron

COPY scripts/init.sql /docker-entrypoint-initdb.d/

RUN mkdir -p /app/app/templates
COPY app/templates /app/app/templates/

CMD ["sh", "-c", "cron && tail -f /var/log/cron.log"]