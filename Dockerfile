# Use official Python parent image, version 3.9
FROM python:3.9

# Set working directory to /app
WORKDIR /app

# Copy
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Run ETL pipeline script when container launches
CMD ["python", "-m", "etl_project.pipeline"]