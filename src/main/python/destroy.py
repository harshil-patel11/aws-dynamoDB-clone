import subprocess

def kill_java_processes(identifier):
    try:
        # Find processes by command line including the identifier
        cmd = f"pgrep -f {identifier}"
        pids = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')

        if pids:
            print(f"Killing processes: {' '.join(pids)}")
            for pid in pids:
                subprocess.run(["kill", pid])
        else:
            print("No processes found with identifier", identifier)
    except subprocess.CalledProcessError:
        print("No processes found with identifier", identifier)

if __name__ == "__main__":
    # The unique identifier for your Java application
    identifier = "CPEN431_2024_PROJECT_G10-1.0-SNAPSHOT-jar-with-dependencies.jar"
    kill_java_processes(identifier)