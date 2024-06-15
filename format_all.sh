#!/bin/bash

# Default values
SRC_DIR="."  # Default search directory (current directory)
EXCLUDE_DIRS=()  # Array to hold directories to exclude
CLANG_FORMAT_STYLE="$SRC_DIR/.clang-format"

# Helper function to display usage
usage() {
    echo "Usage: $0 [-d <directory>] [-e <exclude_dir1,exclude_dir2,...>] [-c <clang-format-file>]"
    echo "  -d <directory>       Directory to search for source files (default: current directory)."
    echo "  -e <exclude_dirs>    Comma-separated list of directories to exclude."
    echo "  -c <clang-format>    Path to the .clang-format file (default: \$SRC_DIR/.clang-format)."
    exit 1
}

# Parse command-line options
while getopts "d:e:c:" opt; do
    case ${opt} in
        d)
            SRC_DIR="$OPTARG"
            ;;
        e)
            IFS=',' read -r -a EXCLUDE_DIRS <<< "$OPTARG"
            ;;
        c)
            CLANG_FORMAT_STYLE="$OPTARG"
            ;;
        *)
            usage
            ;;
    esac
done

# Check if .clang-format file exists
if [ ! -f "$CLANG_FORMAT_STYLE" ]; then
    echo "Error: .clang-format file not found at $CLANG_FORMAT_STYLE."
    exit 1
fi

# Build the exclude pattern for `find`
EXCLUDE_PATTERN=""
for dir in "${EXCLUDE_DIRS[@]}"; do
    EXCLUDE_PATTERN+="! -path \"$SRC_DIR/$dir/*\" "
done

# Run `find` with exclude pattern and format files
eval "find \"$SRC_DIR\" -type f \( -name \"*.cpp\" -o -name \"*.h\" \) $EXCLUDE_PATTERN -exec clang-format -i -style=file {} +"

echo "All files in $SRC_DIR (excluding: ${EXCLUDE_DIRS[*]}) have been formatted according to $CLANG_FORMAT_STYLE."
