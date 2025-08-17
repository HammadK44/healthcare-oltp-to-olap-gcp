FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY _endpoint.py .
ENV PORT=8080
CMD ["uvicorn", "_endpoint:app", "--host", "0.0.0.0", "--port", "8080"]