# Use official Python parent image, version 3.9
FROM python:3.9

# Set working directory to /app
WORKDIR /app

# Copy
COPY etl_project/ /app/etl_project/
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Run ETL pipeline script when container launches
CMD ["python", "etl_project/api_connection.py"]