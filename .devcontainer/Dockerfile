# 使用你拉取的 GCC 15 镜像
FROM gcc:15.2.0

# 接收从 devcontainer.json 传来的参数
ARG HTTP_PROXY
ARG HTTPS_PROXY
ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY
# 防止 apt 安装时弹出时区选择等交互界面
ENV DEBIAN_FRONTEND=noninteractive

# 运行脚本并立即清理缓存
# 这样可以保证 install.sh 产生的临时数据不会留在镜像层里
# RUN chmod +x /tmp/install.sh && \
#     /tmp/install.sh && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/* /tmp/install.sh

# 设置工作目录
WORKDIR /workspace