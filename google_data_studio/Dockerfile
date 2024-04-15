# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /dashboard
COPY google_data_studio /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run update_data_studio_report.py when the container launches
CMD ["python", "scripts/update_data_studio_report.py"]
