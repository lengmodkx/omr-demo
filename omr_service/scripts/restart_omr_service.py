"""一键重启 omr-service。

杀掉占用端口 20884 的旧进程，启动新进程（用最新代码）。

使用：
    python -m omr_service.scripts.restart_omr_service
    python -m omr_service.scripts.restart_omr_service --port 20884
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import psutil  # type: ignore


def find_pids_by_port(port: int) -> list:
    """找到所有占用指定端口的进程 PID（Windows 兼容）。"""
    pids = set()
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            for conn in proc.net_connections(kind="inet"):
                if conn.laddr.port == port and conn.status == "LISTEN":
                    pids.add(proc.pid)
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    return sorted(pids)


def kill_pids(pids: list) -> None:
    for pid in pids:
        try:
            proc = psutil.Process(pid)
            print(f"killing pid={pid} name={proc.name()} cmd={' '.join(proc.cmdline()[:3])}")
            proc.kill()
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            print(f"⚠️  无权限杀 pid={pid}，请用管理员权限运行本脚本")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=20884, help="omr-service 端口")
    parser.add_argument("--skip-kill", action="store_true", help="跳过杀进程")
    parser.add_argument("--skip-start", action="store_true", help="跳过启动")
    parser.add_argument("--wait-after-kill", type=int, default=3, help="杀进程后等几秒再启动")
    args = parser.parse_args()

    if not args.skip_kill:
        pids = find_pids_by_port(args.port)
        if not pids:
            print(f"端口 {args.port} 没有进程在监听，无需 kill")
        else:
            kill_pids(pids)
            print(f"等待 {args.wait_after_kill}s 让端口释放...")
            time.sleep(args.wait_after_kill)
            still = find_pids_by_port(args.port)
            if still:
                print(f"⚠️  端口 {args.port} 仍被占用: {still}")
                return 1

    if args.skip_start:
        print("--skip-start 启用，不启动新进程")
        return 0

    # 启动新进程
    print("启动 omr-service...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "omr_service.main"],
        cwd=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        stdout=None,
        stderr=None,
    )
    print(f"started pid={proc.pid}")
    return 0


if __name__ == "__main__":
    sys.exit(main())