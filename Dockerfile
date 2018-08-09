FROM daocloud.io/xingetouzi/python3-cron:latest

WORKDIR /app
ENV WORK=/app
ENV PYTHONPATH=/app:$PYTHONPATH
COPY requirements.txt ./
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY . ./
VOLUME [ "/logs" ]
CMD ["/bin/bash", "command.sh"]