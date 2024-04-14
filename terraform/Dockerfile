# Use an official Terraform runtime as a parent image
FROM hashicorp/terraform:latest

# Set the working directory in the container
WORKDIR /terraform

# Copy the Terraform files into the container
COPY terraform /terraform

# Define command to run Terraform
CMD ["terraform", "--version"]
