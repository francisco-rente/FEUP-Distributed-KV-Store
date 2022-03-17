import csv
import os


def is_file_empty(file_name): return os.stat(file_name).st_size == 0


def write_headers(file_name, blkSize=-1):
    if(is_file_empty(file_name)):
        with open(file_name, 'w', encoding='UTF8', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if(blkSize == -1):
                writer.writerow(['Size', 'Time'])
            else:
                writer.writerow(['Size', 'BlkSize', 'Time'])


def get_file_name(option):
    dir = 'results/'
    files = ['generic_values.csv', 'line_values.csv', 'block_values.csv']
    try:
        return dir + files[option - 1]
    except:
        raise Exception("Invalid file option")


def write_values(file_name, size, time, blkSize=-1):
    try:
        with open(file_name, 'a', encoding='UTF8', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if(blkSize == -1):
                writer.writerow([size, time])
            else:
                writer.writerow([size, blkSize, time])
    except:
        raise Exception("Couldn't write to file")


def output_to_file(option, size, time, blkSize=-1):
    file = get_file_name(option)

    if(os.path.exists(file) == True):
        write_headers(file, blkSize)
        write_values(file, size, time, blkSize)
    else:
        raise Exception(file + ": file does not exist")
