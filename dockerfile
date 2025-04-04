FROM ghcr.io/astral-sh/uv:bookworm-slim

ADD . /app

WORKDIR /app
RUN uv sync --frozen

CMD ["uv", "run", "box_scores.py"]
