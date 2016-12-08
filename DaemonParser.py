# Objectives
# Parse the daemon logs:
# output: show sync slice, oplog slice and snapshots work history and statistics

import sys, getopt, os.path, os

SYNC_SLICE = "Loading Sync Slice"
OPLOG_SLICE = "OplogSlice"
SNAPSHOT = "file save info"

dic_sync_slice = {}
dic_oplog_slice = {}
dic_snapshot = {}


def func_parse_line(line):
    if line.find(SYNC_SLICE) > 0:
        func_map_job(line, SYNC_SLICE)
    elif line.find(OPLOG_SLICE) > 0:
        func_map_job(line, OPLOG_SLICE)
    elif line.find(SNAPSHOT) > 0:
        func_map_job(line, SNAPSHOT)


def func_map_job(line, type):
    line_array = line.split(" ")
    shard = (line_array[8].split("."))[3]
    #   group_id = (line_array[8].split("."))[2]
    full_date_time = (line_array[0])
    date = (full_date_time.split("T"))[0]
    hour = ((full_date_time.split("T"))[1]).split(":")[0]
    #   docs = (line_array[21]).strip("\r\n")
    #   daemon = (line_array[1] + line_array[2]).strip("[:")
    #   namespace = (line_array[14])
    #   seq = ((line_array[17]).split("#"))[1].strip(".")
    key = shard + " " + date + "T" + hour

    if type == SYNC_SLICE:
        if dic_sync_slice.get(key):
            dic_sync_slice[key] += 1
        else:
            dic_sync_slice[key] = 1

    if type == OPLOG_SLICE:
        last_oplog = (line_array[14].split(":"))[0]
        current_oplog = (line_array[16].split(":"))[0]
        optime_covered = int(current_oplog) - int(last_oplog)

        if dic_oplog_slice.get(key):
            dic_oplog_slice[key][0] += 1
            dic_oplog_slice[key][1] += optime_covered
        else:
            dic_oplog_slice[key] = [1, optime_covered]

    if type == SNAPSHOT:
        filesize = line_array[37]
        if dic_snapshot.get(key):
            dic_snapshot[key] += int(filesize)
        else:
            dic_snapshot[key] = int(filesize)


def print_usage():
    print("DaemonParser.py -i <file>")
    print ("DaemonParser.py -h for help")
    pass


def parse_command_line_args(argv):
    inputfile = ''
    try:
        opts, args = getopt.getopt(argv, "hi:", ['help', 'input='])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    if not opts:
        print_usage()
        sys.exit(2)
    else:
        for opt, arg in opts:
            if opt == '-h':
                print_usage()
                sys.exit()
            elif opt in ("-i", "--input"):
                inputfile = arg
        return inputfile


inputfile = parse_command_line_args(sys.argv[1:])
if os.path.exists(inputfile):
    pass
f = open(inputfile, 'r')
dic_sync_slice = {}
dic_oplog_slice = {}
for line in f:
    func_parse_line(line)

for key in dic_sync_slice:
    count_mb_arr = [dic_sync_slice[key], (dic_sync_slice[key] * 10)]
    dic_sync_slice[key] = count_mb_arr

for key in dic_oplog_slice:
    dic_oplog_slice[key][1] = round((dic_oplog_slice[key][1] / 3600.0), 2)

for key in dic_snapshot:
    dic_snapshot[key] = round((dic_snapshot[key] / 1000.0 / 1000.0 / 1000.0), 2)

for key in sorted(dic_sync_slice):
    print("%s => %s%s" % (key, dic_sync_slice[key], " MB"))

for key in sorted(dic_oplog_slice):
    print("%s => %s%s" % (key, dic_oplog_slice[key], " H"))

for key in sorted(dic_snapshot):
    print("%s => %s%s" % (key, dic_snapshot[key], " GB"))

f.close()
