import subprocess
import sys

if __name__ == "__main__":
    cmd = [sys.executable, "-m", "streamlit", "run", "app.py"]
    subprocess.call(cmd)
