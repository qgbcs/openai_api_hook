#!/bin/bash
# 启动代理快捷脚本（Linux/Mac/WSL）
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
python3 "$SCRIPT_DIR/local_proxy.py" "$@"
