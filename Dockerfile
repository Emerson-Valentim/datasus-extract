# Use the official Python image from the Docker Hub
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN apt install libffi-dev

# Copy the rest of the application code into the container
COPY . .

# Keep the container running
CMD ["tail", "-f", "/dev/null"]