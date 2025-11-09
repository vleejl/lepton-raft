#!/bin/bash
set -euo pipefail

clear

# ----------------------------
# 默认配置
# ----------------------------
MODE="debug"
SANITIZER="none"
TOOLCHAIN="gcc" # 默认工具链
JOBS="" # 默认不指定并发线程数
VERBOSE=false # 默认不显示详细输出

# ----------------------------
# 帮助信息
# ----------------------------
usage() {
    echo "Usage: $0 [options]"
    echo
    echo "Options:"
    echo "  -m, --mode <debug|release>     Build mode (default: debug)"
    echo "  -s, --sanitizer <asan|tsan|msan|none>"
    echo "                                 Sanitizer type (default: none)"
    echo "  -t, --toolchain <gcc|clang>   Compiler toolchain (default: gcc)"
    echo "  -j, --jobs <N>                 Number of parallel jobs (default: auto)"
    echo "  -c, --clean                    Clean build directory before build"
    echo "  -v, --verbose                  Show verbose build output"
    echo "  -h, --help                     Show this help message"
    echo
    echo "Examples:"
    echo "  $0 -m debug -s asan -t clang   # Debug build with AddressSanitizer using Clang"
    echo "  $0 -m release -j 8 -t gcc -v   # Release build with 8 threads using GCC with verbose output"
    echo "  $0 -m release                  # Release build with auto-detected thread count"
}

# ----------------------------
# 参数解析
# ----------------------------
CLEAN=false
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--mode)
            MODE="$2"
            shift 2
            ;;
        -s|--sanitizer)
            SANITIZER="$2"
            shift 2
            ;;
        -t|--toolchain)
            TOOLCHAIN="$2"
            shift 2
            ;;
        -j|--jobs)
            JOBS="$2"
            shift 2
            ;;
        -c|--clean)
            CLEAN=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# ----------------------------
# 验证工具链选项
# ----------------------------
if [[ "$TOOLCHAIN" != "gcc" && "$TOOLCHAIN" != "clang" ]]; then
    echo "Invalid toolchain: $TOOLCHAIN. Must be 'gcc' or 'clang'"
    usage
    exit 1
fi

# ----------------------------
# 前置任务
# ----------------------------
./format_all.sh -e build,third_party

if $CLEAN; then
    rm -rf build
    xmake clean
fi

# ----------------------------
# 构建配置
# ----------------------------
XMAKE_ARGS=("-m" "$MODE" "--toolchain=$TOOLCHAIN" "-c")

case "$SANITIZER" in
    asan)
        XMAKE_ARGS+=("--asan=y")
        ;;
    tsan)
        XMAKE_ARGS+=("--tsan=y")
        ;;
    msan)
        XMAKE_ARGS+=("--msan=y")
        ;;
    none)
        ;;
    *)
        echo "Invalid sanitizer: $SANITIZER"
        usage
        exit 1
        ;;
esac

# ----------------------------
# 复制对应的 launch.json 文件
# ----------------------------
setup_vscode_launch() {
    local vscode_dir=".vscode"
    local launch_file="$vscode_dir/launch.json"
    
    # 检查 .vscode 目录是否存在
    if [[ ! -d "$vscode_dir" ]]; then
        echo "[WARNING] .vscode directory not found, skipping launch.json setup"
        return 0
    fi
    
    # 确定源文件
    local source_file=""
    case "$(uname -s)" in
        Darwin)
            source_file="$vscode_dir/launch.mac.json"
            echo "[INFO] Detected macOS, using $source_file"
            ;;
        Linux)
            source_file="$vscode_dir/launch.linux.json"
            echo "[INFO] Detected Linux, using $source_file"
            ;;
        *)
            echo "[ERROR] Unsupported operating system: $(uname -s)"
            return 1
            ;;
    esac
    
    # 检查源文件是否存在
    if [[ ! -f "$source_file" ]]; then
        echo "[ERROR] Source file $source_file not found"
        return 1
    fi
    
    # 删除已存在的 launch.json
    if [[ -f "$launch_file" ]]; then
        echo "[INFO] Removing existing $launch_file"
        rm -f "$launch_file"
    fi
    
    # 复制文件
    echo "[INFO] Copying $source_file to $launch_file"
    cp "$source_file" "$launch_file"
    
    if [[ $? -eq 0 ]]; then
        echo "[SUCCESS] VS Code launch.json configured for $(uname -s)"
    else
        echo "[ERROR] Failed to copy $source_file to $launch_file"
        return 1
    fi
}

# 设置 VS Code launch.json
setup_vscode_launch

# ----------------------------
# 编译
# ----------------------------
echo "[INFO] Running xmake with args: ${XMAKE_ARGS[*]}"
xmake f "${XMAKE_ARGS[@]}"

BUILD_CMD=("build")
if [[ -n "$JOBS" ]]; then
    BUILD_CMD+=("-j" "$JOBS")
    echo "[INFO] Building with $JOBS parallel jobs using $TOOLCHAIN"
else
    echo "[INFO] Building with auto-detected thread count using $TOOLCHAIN"
fi

if $VERBOSE; then
    BUILD_CMD+=("-v")
    echo "[INFO] Verbose output enabled"
fi

xmake "${BUILD_CMD[@]}"

# ----------------------------
# 性能分析（可选）
# ----------------------------
# XMAKE_PROFILE=perf:tag xmake -r