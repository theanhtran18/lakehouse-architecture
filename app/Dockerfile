FROM python:3.9-slim

WORKDIR /home/app

# Cài backend (FastAPI + Uvicorn) + frontend build (nếu có)
RUN pip install --no-cache-dir fastapi uvicorn requests

COPY ./ /home/app

EXPOSE 8051

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8051", "--reload"]
