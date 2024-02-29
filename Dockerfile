# Start from the Ubuntu 21.04 image
FROM ubuntu:21.04

# Install necessary tools
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    # Add any other tools you need here
