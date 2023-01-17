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
    includeFlags = "-I {0}/src/main/resources/headers/ -I {1}/src/main/resources/headers/".format(root_path, lms_path)
    for scalaclass in classes:
        with open(os.path.join(root_path, "bench", "benchC{0}.csv".format(scalaclass)), "w") as f:
            #f.write("file,mmap,read\n")
            f.write("file,mmap\n")
            print("Benchmarking {0}".format(scalaclass))
            subprocess.run(shlex.split("gcc -O3 canon{0}mmap.c {1} -o benchcanon{0}mmap".format(scalaclass, includeFlags)))
            #subprocess.run(shlex.split("gcc -O3 canon{0}read.c {1} -o benchcanon{0}read".format(scalaclass, includeFlags)))
            for filename in filenames:
                f.write(filename)
                print("For {0}".format(filename))
                #for rformat in ["mmap", "read"]:
                for rformat in ["mmap"]:
                    testout = subprocess.run(shlex.split("./benchcanon{0}{1} /scratch1/das160/{2}".format(scalaclass, rformat, filename)), capture_output=True)
                    # testout = testout.stdout.decode("utf-8")
                    # testout = testout[testout.index("\n") + 1:]
                    # with open("testout", "w") as testf:
                    #     testf.write(testout)
                    # subprocess.run(shlex.split("sort testout -o testout"))
                    # diffout = subprocess.run(shlex.split("diff testout /scratch1/das160/{0}wc.txt".format(filename[:-4])), capture_output=True)
                    # assert(diffout.stdout.decode("utf-8") == "")
                    times = []
                    for i in range(5):
                        print("{0}th run".format(i))
                        output = subprocess.run(shlex.split("./benchcanon{0}{1} /scratch1/das160/{2}".format(scalaclass, rformat, filename)), capture_output=True)
                        output = output.stdout.decode("utf-8").split("\n")[0]
                        times.append(float(output))
                    print(rformat + " = " + str(mean(times)))
                    f.write("," + str(mean(times)))
                f.write("\n")
