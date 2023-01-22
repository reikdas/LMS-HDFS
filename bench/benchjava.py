import os
import shlex
import subprocess
from pathlib import Path
from statistics import mean

root_path = Path(__file__).resolve().parent.parent.absolute()
lms_path = os.path.join(root_path, "lms-clean")
hdfs_home = os.getenv('HADOOP_HDFS_HOME')

if __name__ == "__main__":
    filenames = ["1G.txt", "10G.txt", "50G.txt", "100G.txt", "200G.txt"]
    classes = ["wc", "cf", "ws"]
    for scalaclass in classes:
        with open(os.path.join(root_path, "bench", "benchJava{0}.csv".format(scalaclass)), "w") as f:
            f.write("file,mmap\n")
            print("Benchmarking {0}".format(scalaclass))
            subprocess.run(shlex.split("hadoop com.sun.tools.javac.Main HDFS{0}.java".format(scalaclass)))
            subprocess.run(shlex.split("jar cf bench.jar HDFS{0}.class".format(scalaclass)))
            for filename in filenames:
                f.write(filename)
                print("For {0}".format(filename))
                subprocess.run(shlex.split("hadoop jar bench.jar HDFS{0} /{1}".format(scalaclass, filename)), capture_output=True)
                times = []
                for i in range(5):
                    print("{0}th run".format(i), end="")
                    output = subprocess.run(shlex.split("hadoop jar bench.jar HDFS{0} /{1}".format(scalaclass, filename)), capture_output=True)
                    output = output.stdout.decode("utf-8").split("\n")[0]
                    times.append(float(output))
                    print(" = " + output)
                print(str(mean(times)))
                f.write("," + str(mean(times)))
                f.write("\n")
