import os, re, time, sys, gzip, io, gc
from multiprocessing import Process, Manager, Pool

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
    :param created_proc: this is the global 'created_processes' that will be updated to include 'created_proc_local'
    :param stopped_proc: this is the global 'stopped_processes' that will be updated to include 'stopped_proc_local'
    """

    sub_pid = os.getpid()  # running this within the function that is called by multiprocessing.Process so that I can get the pid of the subpocesses

    created_proc_local = dict()  # this will be updated locally then passed back to the parent later to update 'created_processes'
    stopped_proc_local = []  # this will be updated locally then passed back to the parent later to update 'stopped_processes'
    files_checked_local = []  # this will be updated locally then passed back to the parent later to update whatever list was passed for updating (files_checked_1, files_checked_2, etc.)

    for file_obj in quarter_to_check:  # iterate through the objects passed to the function check_processes()

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

    created_proc.update(created_proc_local)  # update the global dictionary to include the local dictionary
    stopped_proc.extend(stopped_proc_local)  # update the global list to include the local list
    passed_list_to_update.extend(files_checked_local) # update the global list to include the local list


if __name__ == '__main__':  # execute the code below only if it is the main process (prevents subprocesses from executing code when using multiprocessing)

    start_time = time.time()  # will be used to determine the full run time
    pwd = os.getcwd()  # used later to search the current directory, and it's subdirectories, for CCM SDL files.

    file_objects = []  # when the files are instantiated, their place in memory is added to this list. The len of this list is used throughout this script for progress tracking.
    regex_file = re.compile(r".*cm\\trace\\ccm\\sdl\\(SDL.*).gz")  # This regex string helps find CCM SDL files only
    get_sdl_files(pwd)  # executing the function to actually find all the .gz SDL files

    manager = Manager()  # initialize the multiprocessing.Manager - Manager allows us to get information from the subprocesses back to the parent.

    files_checked_1 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    files_checked_2 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    files_checked_3 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    files_checked_4 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    total_files_checked = len(files_checked_1) + len(files_checked_2) + len(files_checked_3) + len(files_checked_4)  # this will later be used to see if all files were reviewed

    created_processes = manager.dict()  # this dict will house all the processes that are created
    stopped_processes = manager.list()  # this list will house all the processes that are stopped

    """
    I want to be able to divide the work into quarters and allow sub-processes to handle the segmented work.
    
    So I will take the total number of files and divide it by 4 and store the value as 'first_quarter', then I will create the second_quarter and third_quarter based off first_quarter.
    
    Using the variables first_quarter/second_quarter/third_quarter I will use list slicing to tell the function 'check_processes' which parts of the list it should be concerned with.
    
    Fourth quarter isn't needed as I can just say go from the end of the 3rd quarter to the end of the list.
    """
    first_quarter = len(file_objects) / 4
    second_quarter = first_quarter * 2
    third_quarter = first_quarter * 3

    """
    This is where the magic really happens. The 4 processes are initialized and stored as p1/p2/p3/p4. The function called is check_processes() and the sliced quarters of 'file_objects' are sent with other elements as well.
    """
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
        total_files_checked = len(files_checked_1) + len(files_checked_2) + len(files_checked_3) + len(files_checked_4)  # Check the math each loop as 'total_files_checked' will be updated by subprocesses
        gc.collect()  # release unreferenced memory, huge help with managing memory consumption
        continue  # keep running through the loop until the criteria is met

    print()  # put some whitespace in for ease of viewing
    print("Reading the files is complete. Please wait while we check the processes.")
    print()  # put some whitespace in for ease of viewing

    def del_proc(passed_proc):
        """
        This function is only here to delete processes from created_processes
        :param proc: This is the process to try deleting
        """
        print(f"Checking for {passed_proc}.")
        try:
            del created_processes[passed_proc]
        except:
            pass

    [del_proc(proc) for proc in set(stopped_processes)]  # we are looping through the stopped_processes and sending them to del_proc() to delete it from created_processes

    f = open("multiprocessing-script-results.txt", "w+")  # this is where the results of the script will be documented. All processes that were started, without stopping, will be written to this file.

    to_write = [f"{k} was started in the file below:\n   {v} \n\n" for k, v in created_processes.items()]  # So that I only have to perform 1 write operation I will store everything in a variable named 'to_write'
    f.writelines(to_write)  # this is where we actually write to the file


    def how_long():
        """
        This function identifies the current time, then subtracts the start time, then returns the diff so we can document the program's total runtime.

        It then returns the total runtime
        """
        duration = time.time() - start_time
        duration = duration / 60
        return round(duration, 1)

    runtime = how_long()

    f.write("----------\n----------\n")
    f.write(f"It took {runtime} minutes for the script to complete.")  # Document the runtime in the file.
    f.close()  # close the file

    # The rest tells the consumer about the runtime in the terminal and the location of the file where the script output is stored
    print()
    print(f"It took {runtime} minutes for the script to complete.\n")
    print("The output from the script is stored in a file named 'multiprocessing-script-results.txt' in the directory below:\n")
    print("    ", pwd)
