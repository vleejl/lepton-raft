#!/bin/bash
set -euo pipefail

clear

# ----------------------------
# 默认配置
# ----------------------------
MODE="debug"
SANITIZER="none"

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
    echo "  -c, --clean                    Clean build directory before build"
    echo "  -h, --help                     Show this help message"
    echo
    echo "Examples:"
    echo "  $0 -m debug -s asan   # Debug build with AddressSanitizer"
    echo "  $0 -m release         # Release build"
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
        -c|--clean)
            CLEAN=true
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
XMAKE_ARGS=("-m" "$MODE")

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
# 编译
# ----------------------------
echo "[INFO] Running xmake with args: ${XMAKE_ARGS[*]}"
xmake f "${XMAKE_ARGS[@]}"
xmake build

# ----------------------------
# 性能分析（可选）
# ----------------------------
# XMAKE_PROFILE=perf:tag xmake -r
