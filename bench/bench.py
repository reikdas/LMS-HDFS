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
    conf = {
            "/1G.txt": [1, 2, 4, 8],
            "/10G.txt": [1, 2, 4, 8, 16, 32, 64, 79],
            "/50G.txt": [1, 2, 4, 8, 16, 32, 64, 96],
            "/100G.txt": [1, 2, 4, 8, 16, 32, 64, 96],
            "/200G.txt": [1, 2, 4, 8, 16, 32, 64, 96]
            }
    classes = ["WordCount", "CharFreq", "Whitespace"]
    includeFlags = "-I {0}/src/main/resources/headers/ -I {1}/src/main/resources/headers/".format(lmshdfs_path, lms_path)
    for scalaclass in classes:
        print("Benchmarking {0}".format(scalaclass))
        for key, values in conf.items():
            print("For {0}".format(key))
            subprocess.run(shlex.split("sbt \"runMain {0} --loadFile={1} --writeFile=benchmmap.c --bench --mmap\"".format(scalaclass, key)), cwd=lmshdfs_path)
            subprocess.run(shlex.split("sbt \"runMain {0} --loadFile={1} --writeFile=benchread.c --bench\"".format(scalaclass, key)), cwd=lmshdfs_path)
            subprocess.run(shlex.split("mpicc -O3 benchmmap.c {0} -o benchmmap".format(includeFlags)), cwd=lmshdfs_path)
            subprocess.run(shlex.split("mpicc -O3 benchread.c {0} -o benchread".format(includeFlags)), cwd=lmshdfs_path)
            with open(os.path.join(lmshdfs_path, "bench", "bench{0}{1}.csv".format(scalaclass, key[1:-4])), "w") as f:
                f.write("Procs,mmap,read\n")
                for value in values:
                    print("Num threads = {0}".format(value))
                    f.write(str(value))
                    for rformat in ["mmap", "read"]:
                        subprocess.run(shlex.split("mpirun -np {0} --mca btl ^openib -map-by numa bench{1} 0".format(value, rformat)), cwd=lmshdfs_path) # Get the program in cache
                        maxtimes = []
                        for i in range(5):
                            print("{0}th run".format(i))
                            output = subprocess.run(shlex.split("mpirun -np {0} --mca btl ^openib -map-by numa bench{1} 0".format(value, rformat)), capture_output=True, cwd=lmshdfs_path)
                            output = output.stdout.decode("utf-8")
                            times = []
                            for line in output.split("\n")[:-1]:
                                times.append(float(find_between(line, "spent", "time").strip()))
                            maxtimes.append(max(times))
                        print(rformat + " = " + str(mean(maxtimes)))
                        f.write("," + str(mean(maxtimes)))
                    f.write("\n")
