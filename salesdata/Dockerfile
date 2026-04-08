FROM python:3.11-slim

WORKDIR /app

# Install dependencies first — this layer is cached as long as requirements.txt doesn't change.
# Only app.py / static / etc. changes won't trigger a re-install.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

EXPOSE 8080

CMD ["sh", "-c", "gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers 2 --timeout 120"]
