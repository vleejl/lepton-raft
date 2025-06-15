#!/bin/bash

# 检查是否提供了参数
if [ $# -ne 1 ]; then
    echo "Usage: $0 'CamelCaseString'"
    exit 1
fi

# 使用 sed 进行转换
echo "$1" | sed -E -e 's/([a-z0-9])([A-Z])/\1_\2/g' -e 's/([A-Z]+)([A-Z][a-z])/\1_\2/g' -e 's/^_//' | tr '[:upper:]' '[:lower:]'