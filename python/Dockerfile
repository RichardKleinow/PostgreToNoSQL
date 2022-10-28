FROM python:latest
RUN pip install --upgrade pip

RUN useradd -m user
USER user
WORKDIR /home/user

COPY --chown=user:user ./requirements.txt requirements.txt
COPY --chown=user:user ./src/transfer.py transfer.py

RUN pip install --user -r requirements.txt

ENV PATH="/home/user/.local/bin:${PATH}"
ENV PYTHONUNBUFFERED 1

CMD ["python", "transfer.py"]