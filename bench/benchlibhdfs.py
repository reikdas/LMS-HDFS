import os
import shlex
import subprocess
from pathlib import Path
from statistics import mean
import time

root_path = Path(__file__).resolve().parent.parent.absolute()
lms_path = os.path.join(root_path, "lms-clean")
hdfs_home = os.getenv('HADOOP_HDFS_HOME')

if __name__ == "__main__":
    print("---C---")
    # filenames = ["1G.txt", "10G.txt", "50G.txt", "100G.txt", "200G.txt"]
    filenames = ["1G.txt", "10G.txt", "50G.txt"]
    classes = ["cf", "ws", "wc"]
    includeFlags = "-I {0}/src/main/resources/headers/ -I {1}/src/main/resources/headers/ -I {2}/include -L {2}/lib/native -lhdfs".format(root_path, lms_path, hdfs_home)
    for scalaclass in classes:
        with open(os.path.join(root_path, "bench", "benchlibhdfs{0}.csv".format(scalaclass)), "w") as f:
            f.write("file,mmap\n")
            print("Benchmarking {0}".format(scalaclass))
            subprocess.run(shlex.split("gcc -O3 libhdfs{0}.c {1} -o benchlibhdfs{0}mmap".format(scalaclass, includeFlags)))
            for filename in filenames:
                f.write(filename)
                print("For {0}".format(filename))
                subprocess.run(shlex.split("./benchlibhdfs{0}mmap /{1}".format(scalaclass, filename)), capture_output=True)
                times = []
                for i in range(5):
                    print("{0}th run".format(i), end="")
                    # subprocess.run("/usr/local/sbin/dropcaches", shell=True)
                    # time.sleep(120)
                    output = subprocess.run(shlex.split("./benchlibhdfs{0}mmap /{1}".format(scalaclass, filename)), capture_output=True)
                    output = output.stdout.decode("utf-8").split("\n")[0]
                    print(" = " + output)
                    f.write("," + output)
                f.write("\n")
