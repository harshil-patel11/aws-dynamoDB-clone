import sys
import subprocess

def main():
    # Check if the filename is provided as a command-line argument
    if len(sys.argv) != 6:
        print("Usage: python setup.py filename")
        exit(1)

    jar_file = sys.argv[1]
    server_file = sys.argv[2]
    output_dir = sys.argv[3]
    num_nodes = sys.argv[4]
    exp_time = sys.argv[5]

    
    # Check if the specified file exists
    try:
        with open(server_file, "r") as file:
            lines = file.readlines()
    except FileNotFoundError:
        print("Error: File not found")
        exit(1)

    # Start Java server instances for each address in the file
    for idx, address in enumerate(lines, 1):
        ip, port = address.strip().split(":")
        output_file = output_dir + f"output_{(int(port)%100)//2}.txt"  # Output file for this subprocess
        print(f"Starting Java server instance on port {port}")
        with open(output_file, "w") as output:
        	subprocess.Popen(["java", "-Xmx512m", "-jar", jar_file, ip, port, server_file, num_nodes, exp_time],stdout=output)

        
if __name__ == "__main__":
    main()
    print("All Java server instances started successfully")
    