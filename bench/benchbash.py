import os
import shlex
import subprocess
from pathlib import Path
from statistics import mean

root_path = Path(__file__).resolve().parent.parent.absolute()
lms_path = os.path.join(root_path, "lms-clean")

if __name__ == "__main__":
    filenames = ["1G.txt", "10G.txt", "50G.txt", "100G.txt", "200G.txt"]
    classes = ["wc", "cf", "ws"]
    for scalaclass in classes:
        with open(os.path.join(root_path, "bench", "benchbash{0}.csv".format(scalaclass)), "w") as f:
            f.write("file,mmap\n")
            print("Benchmarking {0}".format(scalaclass))
            for filename in filenames:
                f.write(filename)
                print("For {0}".format(filename))
                times = []
                for i in range(5):
                    print("{0}th run".format(i))
                    output = subprocess.run(shlex.split("time ./bash{0}.sh /{1}".format(scalaclass, filename)), capture_output=True)
                    output = output.stderr.decode("utf-8")
                    second_space_idx =  output.find(" ", output.find(" ") + 1)
                    end_idx = output.find("elapsed")
                    output = output[second_space_idx:end_idx]
                    mins = float(output[:output.find(":")])
                    secs = float(output[output.find(":") + 1:])
                    t = (mins * 60) + secs
                    times.append(t)
                print(str(mean(times)))
                f.write("," + str(mean(times)))
                f.write("\n")