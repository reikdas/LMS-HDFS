import subprocess
import shlex
from pathlib import Path
from statistics import mean

lmshdfs_path = Path(__file__).parent.absolute()
lms_path = "/home/reikdas/Research/lms-clean" # FIXME: Get from build.sbt

def find_between(s, start, end):
    return (s.split(start))[1].split(end)[0]

if __name__ == "__main__":
    conf = {
            "/1G.txt": [1, 2, 4, 8],
            "/10G.txt": [1, 2, 4, 8, 16, 32, 64, 79],
            }
    classes = ["WordCount", "CharFreq", "Whitespace"]
    includeFlags = "-I {0}/src/main/resources/headers/ -I {1}/src/main/resources/headers/".format(lmshdfs_path, lms_path)
    for scalaclass in classes:
        print("Benchmarking {0}".format(scalaclass))
        for key, values in conf.items():
            with open("bench{0}{1}.csv".format(scalaclass, key[1:-4]), "w") as f:
                print("For {0}".format(key))
                subprocess.run(shlex.split("sbt \"runMain {0} --loadFile={1} --writeFile=bench.c --bench\"".format(scalaclass, key)))
                subprocess.run(shlex.split("mpicc -O3 bench.c {0} -o bench".format(includeFlags)))
                for value in values:
                    print("Num threads = {0}".format(value))
                    subprocess.run(shlex.split("mpirun -np {0} --mca btl ^openib -map-by numa bench 0".format(value))) # Get the program in cache
                    maxtimes = []
                    for i in range(5):
                        print("{0}th run".format(i))
                        output = subprocess.run(shlex.split("mpirun -np {0} --mca btl ^openib -map-by numa bench 0".format(value)), capture_output=True)
                        output = output.stdout.decode("utf-8")
                        times = []
                        for line in output.split("\n")[:-1]:
                            times.append(float(find_between(line, "spent", "time").strip()))
                        maxtimes.append(max(times))
                    print(str(value) + "," + str(mean(maxtimes)) + "\n")
                    f.write(str(value) + "," + str(mean(maxtimes)) + "\n")
