# # Use official Python image as the base image
# FROM python:3.10-slim-bullseye

# # Set environment variables
# ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1

# # Set the working directory
# WORKDIR /app

# # Install OS deps (if needed)
# RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
#     build-essential \
#     gcc \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

# # Install Python deps
# COPY requirements.txt .

# # Install dependencies
# RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# # Copy Source code
# COPY . .

# # Copy model and preprocessor files into the container
# # Copy model and preprocessor files into the container
# COPY final_models/model.pkl final_models/model.pkl
# COPY final_models/preprocessor.pkl final_models/preprocessor.pkl

# # Expose Streamlit default port

# EXPOSE 8502

# # Run the app

# CMD ["streamlit", "run", "app.py", "--server.port=8502", "--server.address=0.0.0.0"]