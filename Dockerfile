FROM python:3.10.1-slim
ADD . /coruscant
WORKDIR /coruscant
RUN pip install -r requirements.txt
CMD python coruscant/app.py