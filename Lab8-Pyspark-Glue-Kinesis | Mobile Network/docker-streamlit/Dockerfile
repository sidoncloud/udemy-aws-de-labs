# Use an official Python runtime as a parent image
FROM python:3.9
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt

EXPOSE 5002
CMD ["streamlit","run","app.py"]