#!/bin/bash
###################################################################
# Homalos 数据中心启动脚本 (Linux/Mac)
###################################################################

echo ""
echo "========================================================================"
echo "  Homalos 数据中心启动脚本"
echo "========================================================================"
echo ""

# 检查虚拟环境
if [ ! -d ".venv" ]; then
    echo "[错误] 虚拟环境不存在，请先创建虚拟环境"
    echo "运行: python -m venv .venv"
    exit 1
fi

# 激活虚拟环境
echo "[1/3] 激活虚拟环境..."
source .venv/bin/activate

# 检查端口占用
echo ""
echo "[2/3] 检查端口占用..."
DEFAULT_PORT=8001

if lsof -Pi :$DEFAULT_PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "[警告] 端口 $DEFAULT_PORT 已被占用"
    echo ""
    echo "可选操作："
    echo "  1. 使用其他端口启动 （推荐）"
    echo "  2. 关闭占用端口的进程"
    echo "  3. 取消启动"
    echo ""
    read -p "请选择 (1/2/3): " choice
    
    case $choice in
        1)
            read -p "请输入新端口号 (例如 8002): " custom_port
            export API_PORT=$custom_port
            echo "[信息] 将使用端口 $custom_port 启动"
            ;;
        2)
            lsof -Pi :$DEFAULT_PORT -sTCP:LISTEN
            read -p "请输入要关闭的进程ID (PID): " pid
            kill -9 $pid
            echo "[信息] 进程已关闭，将使用默认端口 $DEFAULT_PORT 启动"
            ;;
        3)
            echo "[信息] 已取消启动"
            exit 0
            ;;
        *)
            echo "[错误] 无效选择"
            exit 1
            ;;
    esac
else
    echo "[信息] 端口 $DEFAULT_PORT 可用"
fi

# 启动数据中心
echo ""
echo "[3/3] 启动数据中心..."
echo "========================================================================"
echo ""
python start_datacenter.py

