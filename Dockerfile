# Use Ubuntu as the base image
FROM java-ann-bench-base

# Install java-ann-bench once to fill caches
RUN mkdir /java-ann-bench
COPY . /java-ann-bench
#RUN git clone https://github.com/kevindrosendahl/java-ann-bench.git /java-ann-bench
WORKDIR /java-ann-bench
#RUN just init
#RUN ./gradlew build

# Reset the working directory
WORKDIR /java-ann-bench

CMD ["/bin/bash"]
