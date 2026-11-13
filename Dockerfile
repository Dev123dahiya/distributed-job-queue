FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY jobqueue ./jobqueue
RUN pip install --no-cache-dir .
RUN useradd --create-home appuser && mkdir /data && chown appuser /data
USER appuser
VOLUME /data
ENTRYPOINT ["python", "-m", "jobqueue"]
CMD ["demo", "--db", "/data/jobs.db"]
