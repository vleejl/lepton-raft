import os
import re

# ================= 配置区域 =================
# 1. 对应的引用前缀
PREFIX = "storage/wal/"

# 2. 你头文件实际存放的物理目录（用于建立索引）
HEADER_SEARCH_DIR = "./include/" + PREFIX 

# 3. 扫描哪些文件进行修改
EXTENSIONS = {'.h', '.hpp', '.cpp', '.cc', '.cxx'}
# ===========================================

def get_header_whitelist(search_dir):
    """扫描目录，获取所有存在的头文件名集合"""
    headers = set()
    for root, _, files in os.walk(search_dir):
        for f in files:
            if f.endswith(('.h', '.hpp')):
                headers.add(f)
    return headers

def update_includes():
    # 第一步：获取该目录下真实存在的头文件列表
    whitelist = get_header_whitelist(HEADER_SEARCH_DIR)
    print(f"找到 {len(whitelist)} 个目标头文件，准备进行精准匹配...")

    # 第二步：构建正则，只匹配文件名在白名单中的 #include
    # 动态构建正则，例如: #include\s+"(types\.h|utils\.h|node\.h)"
    if not whitelist:
        print("错误：指定目录下没有找到任何头文件！")
        return

    # 将文件名转义并拼接成正则的 OR 模式
    pattern_str = r'#include\s+"(' + '|'.join(map(re.escape, whitelist)) + r')"'
    include_pattern = re.compile(pattern_str)

    count = 0
    # 第三步：遍历当前项目所有文件进行替换
    for root, _, files in os.walk("."):
        for file in files:
            if os.path.splitext(file)[1] in EXTENSIONS:
                file_path = os.path.join(root, file)
                
                # 略过搜索目录自身（可选，通常建议也处理，以防内部互相引用没加路径）
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # 执行精准替换
                new_content = include_pattern.sub(f'#include "{PREFIX}\\1"', content)

                if new_content != content:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    print(f"已精准更新: {file_path}")
                    count += 1
    
    print(f"\n处理完成！共修改了 {count} 个文件。")

if __name__ == "__main__":
    update_includes()