FROM python:3.9
ADD Newton.py .
RUN apt-get update
RUN apt-get install default-jdk -y
RUN pip install tweepy pyspark textblob findspark
CMD ["python", "./Newton.py"]
