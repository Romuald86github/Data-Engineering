# Use the official postgres image as a parent image
FROM postgres:13

# Environment variables
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=password
ENV POSTGRES_DB=mydatabase

# Copy the SQL initialization script
COPY ./init.sql /docker-entrypoint-initdb.d/
