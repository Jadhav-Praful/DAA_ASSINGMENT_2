# Use a valid Python version as the base image
FROM python:3.11-slim-buster

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container's working directory
COPY requirements.txt .

# Install the specified Python packages without storing cache to keep the image lean
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container
COPY . .

# Specify the command to run when the container starts
CMD ["python", "seating_arrangement.py"]

