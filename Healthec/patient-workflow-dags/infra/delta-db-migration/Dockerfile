FROM trinodb/trino

# RUN useradd -ms /bin/bash trino && echo "trino ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget

# Install yq
RUN wget https://github.com/mikefarah/yq/releases/download/v4.13.4/yq_linux_amd64 -O /usr/local/bin/yq && \
    chmod +x /usr/local/bin/yq

# Copy the shell script and YAML file into the container
COPY config .
COPY migrations .
COPY migration.sh .

CMD ["bash", "migration.sh"]
