import os, re, time, sys, gzip, io
from multiprocessing import Process, Manager, Pool
from pprint import pprint
import gc

sld_process = re.compile(r"\|(\S*)")  # used to get only the process name and none of the other stuff around it.


class SdlFiles:  # all files will be instantiated as a file object
    def __init__(self, path="", file_name="default_name.txt", unix_creation_time="490104000"):
        self.path = path
        self.name = file_name
        self.created = unix_creation_time


def get_sdl_files(passed_dir):
    """
    This function finds all the CCM SDL files
    :param passed_dir: This is the CWD of where the python script is ran from
    """
    for rootdir, subdirs, files in os.walk(passed_dir):  # Checking the dir/subdirs for SDL files
        for file in files:
            file_path = os.path.join(rootdir, file)  # full file path stored as 'file_path'
            match = re.search(regex_file, file_path)  # checking if the file is CCM SDL
            if match:  # if the file is CCM SDL, then execute the code below
                create_time = str(os.path.getmtime(file_path))  # find out when the file was created, this way we read the files in the correct order
                real_name = match.group(1)  # the name of the file without the path (i.e. 'SDL003_100_000491.txt')
                new_file = SdlFiles(file_path, real_name, create_time)  # instantiate the file object for the current file being handled
                print("Number of initialized files: {}".format(len(file_objects) + 1))  # notify the consumer how many files have been initialized.
                file_objects.append(new_file)  # append the new object to the list 'file_objects'


def check_processes(quarter_to_check, passed_list_to_update, passed_int, created_proc, stopped_proc):
    """
    This function opens the compressed gz files, reads them with io.BufferedReader to increase speed, then checks for lines where processes are created or stopping.

    When a match is found, the

    :param quarter_to_check: This defines the 25% of logs to review (all logs from start to stop for 25% segmented work)
    :param passed_list_to_update: each process has it's own list to update
    :param passed_int: this is used to check the overall progress and might be deleted
    :param created_proc:
    :param stopped_proc:
    """

    sub_pid = os.getpid()

    created_proc_local = dict()  # this will be updated locally then passed back to the parent later to update 'created_processes'
    stopped_proc_local = []  # this will be updated locally then passed back to the parent later to update 'stopped_processes'
    files_checked_local = []  # this will be updated locally then passed back to the parent later to update whatever list was passed for updating (files_checked_1, files_checked_2, etc.)

    for file_obj in quarter_to_check:

        logs = gzip.open(file_obj.path, 'rb')  # open the gz file
        fil = io.BufferedReader(logs)  # read the file using buffer
        for line in fil:  # iterate through each line of the file
            line_str = str(line)  # convert the line to a string rather than bytes or whatever
            #if (len(line_str) > 145) and (len(line_str) < 373):  # lines that have processes starting/stopping fall between 145 to 373 characters (with some room for variation in length)
            if "|Created" in line_str:  # check if a process is created
                created_match = re.search(sld_process, line_str[109:])  # check a section of the line and get the process id from it. Line sliced to minimize regex searching.
                if created_match:
                    created_proc_local.update({created_match.group(1): file_obj.path})  # add entry to dict 'created_processes' that includes the process and the path to the file where the process was created.
            elif "|Stopping |" in line_str:  # check if a process is stopping
                stopped_match = re.search(sld_process, line_str[109:])  # check a section of the line and get the process id from it. Line sliced to minimize regex searching.
                if stopped_match:
                    stopped_proc_local.append(stopped_match.group(1))  # add the name of the stopped process to the list named 'stopped_processes'. This is used later to remove entries from dict 'created_processes'.
        logs.close()  # close the file before moving forward
        files_checked_local.append("1")  # since we checked a file we are updating the number of files we checked

        print(f"Still working on it, process {sub_pid} just completed file {file_obj.name}.")  # using this for now instead of the one that shows % complete. going to be hard to get % with multiprocessing
        #print("Percentage complete with reading files: {0:.3f}".format((total_files_checked / passed_int) * 100))  # notify the consumer about progress
        gc.collect()  # release unreferenced memory, huge help with managing memory consumption

    created_proc.update(created_proc_local)
    stopped_proc.extend(stopped_proc_local)
    passed_list_to_update.extend(files_checked_local)


if __name__ == '__main__':  # execute the code below only if it is the main process (prevents subprocesses from executing code when using multiprocessing)

    start_time = time.time()  # will be used to determine the full run time
    pwd = os.getcwd()  # used later to search the current directory, and it's subdirectories, for CCM SDL files.

    file_objects = []  # when the files are instantiated, their place in memory is added to this list. This len of this list is used throughout this script for progress tracking.
    regex_file = re.compile(r".*cm\\trace\\ccm\\sdl\\(SDL.*).gz")  # This regex string helps find CCM SDL files only
    get_sdl_files(pwd)

    manager = Manager()

    files_checked_1 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    files_checked_2 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    files_checked_3 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    files_checked_4 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    total_files_checked = len(files_checked_1) + len(files_checked_2) + len(files_checked_3) + len(files_checked_4)

    created_processes = manager.dict()  # this dict will house all the processes that are created
    stopped_processes = manager.list()  # this list will house all the processes that are stopped

    """
    I want to be able to divide the work into quarters and allow sub-processes to handle the segmented work.
    """
    first_quarter = len(file_objects) / 4
    second_quarter = first_quarter * 2
    third_quarter = first_quarter * 3

    p1 = Process(target=check_processes, args=(file_objects[0:round(first_quarter)], files_checked_1, len(file_objects), created_processes, stopped_processes))
    p2 = Process(target=check_processes, args=(file_objects[round(first_quarter):round(second_quarter)], files_checked_2, len(file_objects), created_processes, stopped_processes))
    p3 = Process(target=check_processes, args=(file_objects[round(second_quarter):round(third_quarter)], files_checked_3, len(file_objects), created_processes, stopped_processes))
    p4 = Process(target=check_processes, args=(file_objects[round(third_quarter)::], files_checked_4, len(file_objects), created_processes, stopped_processes))

    # We already instantiated the processes; however, we now need to start them
    p1.start()
    p2.start()
    p3.start()
    p4.start()

    while total_files_checked != len(file_objects):  # don't execute more code until this criteria is met
        total_files_checked = len(files_checked_1) + len(files_checked_2) + len(files_checked_3) + len(files_checked_4)
        """
        print()
        print(total_files_checked)
        print()
        print(len(file_objects))
        """
        # print(len(created_processes))
        gc.collect()  # release unreferenced memory, huge help with managing memory consumption
        continue

    print()  # put some whitespace in for ease of viewing
    print("Reading the files is complete. Please wait while we check the processes.")
    print()  # put some whitespace in for ease of viewing

    def del_proc(proc):
        print(f"Checking for {proc}.")
        try:
            del created_processes[proc]
        except:
            pass

    [del_proc(proc) for proc in set(stopped_processes)]

    f = open("multiprocessing-script-results.txt", "w+")

    to_write = [f"{k} was started in the file below:\n   {v} \n\n" for k, v in created_processes.items()]
        #print("{} was started in the file below:\n    {} \n".format(k, v))

    f.writelines(to_write)


    def how_long():
        duration = time.time() - start_time
        duration = duration / 60
        return round(duration, 1)

    runtime = how_long()

    f.write("----------\n----------\n")
    f.write("It took {} minutes for the script to complete.".format(runtime))
    f.close()

    print()
    print("It took {} minutes for the script to complete.\n".format(runtime))
    print("The output from the script is stored in a file "
          "named 'multiprocessing-script-results.txt' in the directory below:\n")
    print("    ", pwd)
