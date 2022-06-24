FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

WORKDIR /app

COPY [ "src/Pipfile", "src/Pipfile.lock", "./"]

RUN pip install --no-cache-dir pipenv && \
        pipenv install --deploy --system --clear

COPY [ "src/predict.py", "./"]

ENTRYPOINT ["python", "predict.py"] 
CMD [""]
