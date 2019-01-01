import os, re, time, sys, gzip, io
from pprint import pprint


start_time = time.time()
pwd = os.getcwd()


class SdlFiles:
    def __init__(self, path=pwd, file_name="default_name.txt", unix_creation_time="490104000"):
        self.path = path
        self.name = file_name
        self.created = unix_creation_time


file_objects = []
initialized_files = 1
how_many_files = []
regex_file = re.compile(r".*cm\\trace\\ccm\\sdl\\(SDL.*).gz")

for rootdir, subdirs, files in os.walk(pwd):
    for file in files:
        file_path = os.path.join(rootdir, file)
        match = re.search(regex_file, file_path)
        if match:
            how_many_files.append(match.group(1))
            create_time = str(os.path.getmtime(file_path))
            real_name = match.group(1)
            new_file = SdlFiles(file_path, real_name, create_time)
            print("Number of initialized files: {}".format(initialized_files))
            file_objects.append(new_file)
            initialized_files += 1


created_processes = {}
stopped_processes = []
files_checked = 0
sld_process = re.compile(r"\|(\S*)")

for file in file_objects:
    logs = gzip.open(file.path, 'rb')
    f = io.BufferedReader(logs)
    for line in f:
        line_str = str(line)
        if (len(line_str) > 145) and (len(line_str) < 373):
            if "|Created" in line_str:
                created_match = re.search(sld_process, line_str[109:])
                if created_match:
                    created_processes.update({created_match.group(1): file.path})
            elif "|Stopping |" in line_str:
                stopped_match = re.search(sld_process, line_str[109:])
                if stopped_match:
                    stopped_processes.append(stopped_match.group(1))
    logs.close()
    files_checked += 1
    print("Percentage complete with reading files: {0:.3f}".format((files_checked / len(how_many_files)) * 100))


for process in stopped_processes:
    check_dict = created_processes.get(process)
    if check_dict:
        del created_processes[process]


f = open("script-results.txt", "w+")


for k, v in created_processes.items():
    print("{} was started in the file below:\n    {} \n".format(k, v))
    f.write("{} was started in the file below:\n   {} \n\n".format(k, v))


def how_long():
    duration = time.time() - start_time
    duration = duration / 60
    return round(duration, 1)


runtime = how_long()


f.write("----------\n----------\n")
f.write("It took {} minutes for the script to complete.".format(runtime))
f.close()


print()
print("The processes above were created; however, we didn't see them stop.\n")
print("It took {} minutes for the script to complete.\n".format(runtime))
print("The output from the script is also stored in a file "
      "named 'script-results.txt' in the directory below:\n")
print("    ", pwd)