FROM python:3.9

WORKDIR /app

# 
COPY ./requirements.txt /app/requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# 
COPY ./code/ /app/

#
COPY  ./data.bin /app/

# Install OpenJDK-17
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean;
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

# 
CMD ["fastapi", "run", "main.py", "--port", "80"]
