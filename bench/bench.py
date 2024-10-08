import os
import shlex
import subprocess
from pathlib import Path

lmshdfs_path = Path(__file__).resolve().parent.parent.absolute()
lms_path = os.path.join(lmshdfs_path, "lms-clean")
mpi_path = "/usr/lib/x86_64-linux-gnu/openmpi/include/"

def find_between(s, start, end):
    return (s.split(start))[1].split(end)[0]

if __name__ == "__main__":
    print("---DDLOADER---")
    conf = {
            "/1G.txt": [1, 2, 4, 8],
            "/10G.txt": [1, 2, 4, 8, 16, 32, 64],
            "/50G.txt": [1, 2, 4, 8, 16, 32, 64],
            }
    classes = ["CharFreq", "Whitespace", "WordCount"]
    includeFlags = "-I {0}/src/main/resources/headers/ -I {1}/src/main/resources/headers/".format(lmshdfs_path, lms_path)
    for scalaclass in classes:
        print("Benchmarking {0}".format(scalaclass))
        for key, values in conf.items():
            print("For {0}".format(key))
            with open(os.path.join(lmshdfs_path, "bench", "benchDD{0}{1}.csv".format(scalaclass, key[1:-4])), "w") as f:
                f.write("Procs,mmap\n")
                compiled = 0
                for value in values:
                    print("Num threads = {0}".format(value))
                    f.write(str(value))
                    if compiled == 0:
                        compiled = 1
                        subprocess.run(shlex.split("sbt \"runMain {0} --loadFile={1} --writeFile=benchmmap.c --bench --mmap --multiproc --print\"".format(scalaclass, key)), cwd=lmshdfs_path)
                        err = subprocess.run(shlex.split("mpicc -O3 benchmmap.c {0} -o benchmmap".format(includeFlags)), cwd=lmshdfs_path)
                        err.check_returncode()
                        print(f"mpicc -O3 benchmmap.c {includeFlags} -o benchmmap")
                        execcmd = "mpirun -np {0} --mca btl ^openib --map-by numa --bind-to core benchmmap 0".format(value)
                    elif compiled == 1:
                        execcmd = "mpirun -np {0} --mca btl ^openib --map-by numa --bind-to core benchmmap 0".format(value)
                    else:
                        raise Exception("Huh?")
                    print(execcmd)
                    subprocess.run("/usr/local/sbin/dropcaches", shell=True)
                    subprocess.run(shlex.split(execcmd), cwd=lmshdfs_path) # Get the program in cache
                    subprocess.run(shlex.split(execcmd), cwd=lmshdfs_path) # Get the program in cache
                    for i in range(5):
                        print("{0}th run".format(i),end="")
                        output = subprocess.run(shlex.split(execcmd), capture_output=True, cwd=lmshdfs_path)
                        output = output.stdout.decode("utf-8")
                        times = []
                        if value == 1:
                            line = output.split("\n")[0]
                            times.append(float(find_between(line, "spent", "time").strip()))
                        else:
                            for line in output.split("\n"):
                                if ("Proc" in line and "spent" in line and "time." in line):
                                    times.append(float(find_between(line, "spent", "time").strip()))
                        print(" = ", max(times))
                        f.write("," + str(max(times)))
                    f.write("\n")
