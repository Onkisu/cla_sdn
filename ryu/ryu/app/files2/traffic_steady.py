import time, subprocess

DST="10.0.0.2"
pid=subprocess.check_output(["pgrep","-f","h1"]).decode().strip()

while True:
    subprocess.run([
        "mnexec","-a",pid,
        "ITGSend","-T","UDP",
        "-a",DST,"-c","160","-C","50",
        "-t","1000","-l","/dev/null"
    ])
    time.sleep(1)
