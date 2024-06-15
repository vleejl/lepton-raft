#!/bin/bash

# 检查是否提供了文件名参数
if [ -z "$1" ]; then
  echo "Usage: $0 <file_name>"
  exit 1
fi

# 获取文件名并转换为大写格式（用于宏定义）
FILE_NAME=$(basename "$1" .h)
UPPER_FILE_NAME=$(echo "$FILE_NAME" | tr 'a-z' 'A-Z')

# 创建头文件内容
cat <<EOL > "$1"
#ifndef _LEPTON_${UPPER_FILE_NAME}_H_
#define _LEPTON_${UPPER_FILE_NAME}_H_

namespace lepton {

}  // namespace lepton

#endif  // _LEPTON_${UPPER_FILE_NAME}_H_
EOL

echo "Header file '$1' created successfully!"
