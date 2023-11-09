# Use Ubuntu as the base image
FROM ubuntu:latest

# Install dependencies
RUN apt-get update && apt-get install -y \
  wget \
  software-properties-common \
  gnupg \
  git

# Add the Corretto APT repository
RUN wget -O- https://apt.corretto.aws/corretto.key | gpg --dearmor > /usr/share/keyrings/corretto-archive-keyring.gpg && \
  gpg --no-default-keyring --keyring /usr/share/keyrings/corretto-archive-keyring.gpg --fingerprint && \
  echo "deb [signed-by=/usr/share/keyrings/corretto-archive-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list

# Install Java Corretto 21 JDK
RUN apt-get update && apt-get install -y java-21-amazon-corretto-jdk

# Add the Prebuilt-MPR repository
RUN wget -qO - https://proget.makedeb.org/debian-feeds/prebuilt-mpr.pub | gpg --dearmor > /usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg && \
  echo "deb [arch=all,$(dpkg --print-architecture) signed-by=/usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg] https://proget.makedeb.org prebuilt-mpr $(lsb_release -cs)" | tee /etc/apt/sources.list.d/prebuilt-mpr.list

# Install `just` command runner
RUN apt-get update && apt-get install -y just

# Clean up APT caches to reduce image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-21-amazon-corretto

# Verify installations
RUN java -version && just --version

# Install java-ann-bench once to fill caches
RUN git clone https://github.com/kevindrosendahl/java-ann-bench.git /java-ann-bench
WORKDIR /java-ann-bench
RUN just init
RUN ./gradlew build

# Reset the working directory
WORKDIR /java-ann-bench

CMD ["/bin/bash"]
