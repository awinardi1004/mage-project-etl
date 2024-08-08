FROM mageai/mageai:latest

ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}

# Salin dan instal dependensi Python
COPY requirements.txt ${USER_CODE_PATH}requirements.txt
RUN pip3 install -r ${USER_CODE_PATH}requirements.txt

# Instal dbt
RUN pip3 install dbt
