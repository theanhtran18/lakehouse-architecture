FROM python:3.9-slim

WORKDIR /app

# Cài Flask + MLflow + scikit-learn
RUN pip install --no-cache-dir flask mlflow scikit-learn pandas xgboost

COPY ./ /app

EXPOSE 5001

CMD ["python", "serve.py"]
