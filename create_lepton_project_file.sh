#!/bin/bash

# 检查是否提供了文件名参数
if [ -z "$1" ]; then
  echo "Usage: $0 <file_name>"
  exit 1
fi

# 获取文件名和后缀名
FILE_NAME=$(basename "$1")
FILE_NAME_NO_EXT="${FILE_NAME%.*}"
EXT="${FILE_NAME##*.}"

# 转换为大写格式（用于宏定义）
UPPER_FILE_NAME=$(echo "$FILE_NAME_NO_EXT" | tr 'a-z' 'A-Z')

# 根据文件扩展名生成不同的文件
if [ "$EXT" == "h" ]; then
  # 创建头文件内容
  cat <<EOL > "$1"
#ifndef _LEPTON_${UPPER_FILE_NAME}_H_
#define _LEPTON_${UPPER_FILE_NAME}_H_

namespace lepton {

}  // namespace lepton

#endif  // _LEPTON_${UPPER_FILE_NAME}_H_
EOL
  echo "Header file '$1' created successfully!"

elif [ "$EXT" == "cpp" ]; then
  # 创建 cpp 文件内容
  cat <<EOL > "$1"
#include "${FILE_NAME_NO_EXT}.h"

namespace lepton {

// Add your implementations here

}  // namespace lepton
EOL
  echo "CPP file '$1' created successfully!"

else
  echo "Error: Unsupported file extension. Please provide a .h or .cpp file."
  exit 1
fi
