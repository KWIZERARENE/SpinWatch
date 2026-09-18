"""
SpinWatch - Automatic HDFS Synchronization Script
Uploads all raw telemetry Parquet files, ML predictions, and PySpark insights
directly into HDFS cluster directories (hdfs://localhost:9000/data/machines/).

Windows fix: hdfs.cmd is a batch file that internally re-expands %* before
passing args to Java, stripping outer quotes in the process. This means paths
containing '=' (e.g. dt=2026-09-18) get split into two tokens: 'dt' and
'2026-09-18', causing 'No such file or directory' errors.

Solution: use shell=True with a fully-quoted command string so the double-quotes
survive the batch file's %* re-expansion and reach Java intact.
"""

import os
import glob
import subprocess

HDFS_CMD = r"C:\hadoop\bin\hdfs.cmd"
HDFS_BASE = "hdfs://localhost:9000"


def hdfs(*args, check=False):
    """Run an hdfs dfs command via shell=True with every argument explicitly
    double-quoted.

    Why shell=True + manual quoting?
    hdfs.cmd is a Windows batch file. Internally it collects all arguments
    with %* and forwards them verbatim to the Java process. During that
    re-expansion, cmd.exe strips the outer quotes that Python's list2cmdline
    placed around each token, so a path like:
        hdfs://localhost:9000/data/machines/raw/dt=2026-09-18
    arrives at Java as two separate tokens ('dt' and '2026-09-18'),
    producing the 'No such file or directory' error.

    By building the command as a single pre-quoted shell string we ensure
    the double-quotes are part of the raw command line text that cmd.exe
    passes to hdfs.cmd via %*, keeping each path intact all the way to Java.
    """
    parts = [f'"{HDFS_CMD}"', 'dfs'] + [f'"{a}"' for a in args]
    cmd = ' '.join(parts)
    return subprocess.run(cmd, check=check, shell=True)


def hdfs_available():
    """Quick connectivity check — returns True if the NameNode is reachable."""
    cmd = f'"{HDFS_CMD}" dfs "-test" "-d" "/"'
    result = subprocess.run(cmd, capture_output=True, timeout=10, shell=True)
    return result.returncode == 0


def upload_to_hdfs():
    print("[*] Synchronizing SpinWatch analytical data with HDFS cluster (hdfs://localhost:9000)...")

    # 0. Fast-fail if HDFS is not running so errors are immediately obvious
    try:
        if not hdfs_available():
            print("[!] HDFS NameNode is not reachable at localhost:9000.")
            print("    Start HDFS first with start_spinwatch_infra.bat, then re-run.")
            return
    except Exception:
        print("[!] Could not reach HDFS. Skipping upload (HDFS may not be running).")
        return

    # 1. Create HDFS Target Directories
    dirs = [
        f"{HDFS_BASE}/data/machines/raw",
        f"{HDFS_BASE}/data/machines/predictions",
        f"{HDFS_BASE}/data/machines/insights",
    ]
    for d in dirs:
        hdfs("-mkdir", "-p", d)

    # 2. Upload Raw Telemetry Parquet & JSON Files
    # IMPORTANT: glob returns Windows backslash paths; abspath keeps them consistent.
    # The dt=YYYY-MM-DD directory name must be passed as a single list element so
    # the shell never interprets '=' as a key=value separator.
    raw_files = [os.path.abspath(f) for f in glob.glob("data/machines/raw/*/*.*")]
    for fpath in raw_files:
        dt_dir = os.path.basename(os.path.dirname(fpath))   # e.g. "dt=2026-09-18"
        target_hdfs_dir = f"{HDFS_BASE}/data/machines/raw/{dt_dir}"
        # mkdir first — pass full path as a single arg, no shell expansion
        hdfs("-mkdir", "-p", target_hdfs_dir)
        print(f"[*] Uploading: {fpath} -> {target_hdfs_dir}")
        hdfs("-put", "-f", fpath, target_hdfs_dir)

    # 3. Upload ML Predictions JSON
    pred_path = os.path.abspath("data/machines/predictions/latest_predictions.json")
    if os.path.exists(pred_path):
        print(f"[*] Uploading ML predictions: {pred_path}")
        hdfs("-put", "-f", pred_path, f"{HDFS_BASE}/data/machines/predictions/")

    # 4. Upload PySpark Heat Insights JSON
    insight_path = os.path.abspath("data/machines/insights/latest_insights.json")
    if os.path.exists(insight_path):
        print(f"[*] Uploading PySpark insights: {insight_path}")
        hdfs("-put", "-f", insight_path, f"{HDFS_BASE}/data/machines/insights/")

    # 5. Verify HDFS directory contents
    print("\n================ HDFS DIRECTORY VERIFICATION ================")
    for d in dirs:
        print(f"\nContents of {d}:")
        hdfs("-ls", d)
    print("=============================================================\n")


if __name__ == "__main__":
    upload_to_hdfs()
