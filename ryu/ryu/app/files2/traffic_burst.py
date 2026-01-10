import time, subprocess
DST="10.0.0.2"
pid=subprocess.check_output(["pgrep","-f","h3"]).decode().strip()

while True:
    for r in range(80,260,10):
        subprocess.run([
            "mnexec","-a",pid,
            "ITGSend","-T","UDP",
            "-a",DST,"-c","160","-C",str(r),
            "-t","1000","-l","/dev/null"
        ])
        time.sleep(1)
    time.sleep(60)
