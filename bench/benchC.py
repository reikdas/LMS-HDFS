import os
import shlex
import subprocess
from pathlib import Path
from statistics import mean

root_path = Path(__file__).resolve().parent.parent.absolute()
lms_path = "/homes/das160/lms-clean" # FIXME: Get from build.sbt

if __name__ == "__main__":
    filenames = ["1G.txt", "10G.txt", "50G.txt", "100G.txt", "200G.txt"]
    classes = ["wc", "cf", "ws"]
    includeFlags = "-I {0}/src/main/resources/headers/ -I {1}/src/main/resources/headers/".format(root_path, lms_path)
    for scalaclass in classes:
        with open(os.path.join(root_path, "bench", "benchC{0}.csv".format(scalaclass)), "w") as f:
            f.write("file,mmap,read\n")
            print("Benchmarking {0}".format(scalaclass))
            subprocess.run(shlex.split("gcc -O3 canon{0}mmap.c {1} -o benchcanon{0}mmap".format(scalaclass, includeFlags)))
            subprocess.run(shlex.split("gcc -O3 canon{0}read.c {1} -o benchcanon{0}read".format(scalaclass, includeFlags)))
            for filename in filenames:
                f.write(filename)
                print("For {0}".format(filename))
                for rformat in ["mmap", "read"]:
                    subprocess.run(shlex.split("./benchcanon{0}{1} /scratch1/das160/{2}".format(scalaclass, rformat, filename)))
                    times = []
                    for i in range(5):
                        print("{0}th run".format(i))
                        output = subprocess.run(shlex.split("./benchcanon{0}{1} /scratch1/das160/{2}".format(scalaclass, rformat, filename)), capture_output=True)
                        output = output.stdout.decode("utf-8")
                        times.append(float(output))
                    print(rformat + " = " + str(mean(times)))
                    f.write("," + str(mean(times)))
                f.write("\n")
