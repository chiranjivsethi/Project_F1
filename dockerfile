# Use the official PostgreSQL image from Docker Hub with a specific version
FROM postgres:14.2

# Set environment variables for PostgreSQL
ENV POSTGRES_USER=jane
ENV POSTGRES_PASSWORD=doe
ENV POSTGRES_DB=f1

# Copy the init.sql file to the Docker container
COPY init.sql /docker-entrypoint-initdb.d/

# Expose the PostgreSQL port
EXPOSE 5432

# Start PostgreSQL server when the container starts
CMD ["postgres"]
