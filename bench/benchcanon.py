import os
import shlex
import subprocess
from pathlib import Path
from statistics import mean
import time

root_path = Path(__file__).resolve().parent.parent.absolute()
lms_path = os.path.join(root_path, "lms-clean")

if __name__ == "__main__":
    print("---C Local File System---")
    filenames = ["1G.txt", "10G.txt", "50G.txt"]
    classes = ["wc", "ws", "cf"]
    includeFlags = "-I {0}/src/main/resources/headers/ -I {1}/src/main/resources/headers/".format(root_path, lms_path)
    for scalaclass in classes:
        with open(os.path.join(root_path, "bench", "benchcanon{0}.csv".format(scalaclass)), "w") as f:
            f.write("file,mmap\n")
            print("Benchmarking {0}".format(scalaclass))
            subprocess.run(shlex.split("gcc -O3 canon{0}mmap.c {1} -o benchcanon{0}mmap".format(scalaclass, includeFlags)))
            for filename in filenames:
                f.write(filename)
                print("For {0}".format(filename))
                for i in range(5):
                    print("{0}th run".format(i), end="")
                    subprocess.run("/usr/local/sbin/dropcaches", shell=True)
                    time.sleep(120)
                    output = subprocess.run(shlex.split("./benchcanon{0}mmap /scratch1/das160/{1}".format(scalaclass, filename)), capture_output=True)
                    output = output.stdout.decode("utf-8").split("\n")[0]
                    print("= {0}".format(output))
                    f.write(","+str(output))
                f.write("\n")
