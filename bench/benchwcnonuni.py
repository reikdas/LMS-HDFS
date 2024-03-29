import os
import shlex
import subprocess
from pathlib import Path
from statistics import mean

lmshdfs_path = Path(__file__).resolve().parent.parent.absolute()
lms_path = os.path.join(lmshdfs_path, "lms-clean")

def find_between(s, start, end):
    return (s.split(start))[1].split(end)[0]

if __name__ == "__main__":
    nprocs = [1, 2, 3, 4, 5, 6]
    includeFlags = "-I {0}/src/main/resources/headers/ -I {1}/src/main/resources/headers/".format(lmshdfs_path, lms_path)
    subprocess.run(shlex.split("sbt \"runMain WordCount --loadFile=/text.txt --writeFile=benchmmap.c --bench --mmap\""), cwd=lmshdfs_path)
    subprocess.run(shlex.split("mpicc -O3 benchmmap.c {0} -o benchmmap".format(includeFlags)), cwd=lmshdfs_path)
    with open(os.path.join(lmshdfs_path, "bench", "benchwcnonuni.csv"), "w") as f:
        f.write("Procs,mmap\n")
        for value in nprocs:
            print("Num threads = {0}".format(value))
            f.write(str(value))
            subprocess.run(shlex.split("mpirun -np {0} --mca btl ^openib -map-by numa benchmmap 0".format(value)), cwd=lmshdfs_path) # Get the program in cache
            maxtimes = []
            for i in range(5):
                print("{0}th run".format(i))
                output = subprocess.run(shlex.split("mpirun -np {0} --mca btl ^openib -map-by numa benchmmap 0".format(value)), capture_output=True, cwd=lmshdfs_path)
                output = output.stdout.decode("utf-8")
                times = []
                for line in output.split("\n")[:-1]:
                    times.append(float(find_between(line, "spent", "time").strip()))
                maxtimes.append(max(times))
            print(str(mean(maxtimes)))
            f.write("," + str(mean(maxtimes)))
            f.write("\n")
