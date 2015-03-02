#!/usr/bin/python

#title          : main.py
#description    : Processes the flat tax file from tax clearinghouse
#               : and generates a sql file
#author         : Kevin Rabello
#version        : 1.0.2
#date           : September 5, 2014
#usage          : ./process_tax.py
#output file    : yyyymmdd_tax.sql
#==============================================================================


import os.path
import csv
import time
import paramiko
import pycurl
from config import *

global username
global password
username = ''
password = ''
db_user = ''
db_pass = ''


#### Start Definitions ####
def process_message(message):
    print message


def get_csv_file(current_date):
    process_message('Starting Tax Download...')
    c = pycurl.Curl()
    current_tax_filename = str('incoming/tax_' + current_date + '.txt')
    fp = open(current_tax_filename, "w")

    url = 'http://tables.zip2tax.com?usr={usr}&pwd={pwd}&id=1'.format(usr=username, pwd=password)

    print url
    c.setopt(pycurl.URL, url)
    c.setopt(pycurl.HTTPHEADER, ['Accept: application/csv'])
    c.setopt(pycurl.VERBOSE, 0)
    c.setopt(c.FOLLOWLOCATION, 1)
    c.setopt(pycurl.WRITEDATA, fp)
    c.perform()
    # check file size to make sure it was downloaded
    if os.path.getsize(current_tax_filename) == 0:
        exit("Error getting csv")
    return current_tax_filename


def usage(usage_text):
    return usage_text


# Make sure file exists and that the
# file contains contents by checking its file size
def check_input_file(file):
    if os.path.isfile(file):
        if os.stat(file).st_size > 0:
            read_file(file)
        else:
            process_message(emptyFile)
    else:
        global openError
        process_message(openError % file)


def convertChars(val):
    return val.replace(u"'", u"&apos;")

# returns sql query
def build_query(values):
    return "REPLACE INTO `edumart`.`taxes` ( `zip`, `state`, `county`, `city`, `tax_rate` ) VALUES " \
          "('%s', '%s', '%s', '%s', '%s');\n" \
          % (
            str(convertChars(values[1])),
            str(convertChars(values[13])),
            str(convertChars(values[14])),
            str(convertChars(values[11])),
            str(float(convertChars(values[2]))*0.01)
            )


# writes query to file then uploads
def write_output(out):
    global local_file_path
    file = open(local_file_path, 'w')
    file.write(out)
    file.close()


# Read file
def read_file(file):
    file = open(file, 'rb')
    try:
        row_number = 0
        reader = csv.reader(file)
        qry = ''
        for row in reader:
            if row_number == 0:
                process_message('Skipping Header. Processing file')
            else:
                qry += str(build_query(row))
            row_number += 1
        write_output(qry)
    finally:
        file.close()


def upload(hostname, port, user_name, pass_word, source, destination):
    try:
        t = paramiko.Transport((hostname, port))
        try:
            t.connect(username=user_name, password=pass_word)
            try:
                sftp = paramiko.SFTPClient.from_transport(t)
                try:
                    sftp.put(source, destination)
                    process_message(uploadMessage % hostname)
                except Exception, e:
                    print str(e)
                sftp.close()
                t.close()
            except Exception, e:
                print str(e)
        except Exception, e:
            print str(e)
    except Exception, e:
        print str(e)


def main():
    process_message(startProcessMessage)
    created_file = get_csv_file(time.strftime("%Y-%m"))
    check_input_file(created_file)
    #upload file to server
    #w3
    upload(w3Server['ip'],
           w3Server['port'],
           w3Server['uname'],
           w3Server['pword'],
           local_file_path,
           '/tmp/'+filename)
    print("mysql edumart -u {db_user} -p {db_pass} -e 'source /tmp/"+filename+"';")
    process_message(finishProcessMessage)


#### End Definitions ####


#run script
if __name__ == "__main__":
    main()
