#!/usr/bin/env python
from threading import Thread
import subprocess, os, sys
import time
import datetime
from Queue import Queue

num_threads = 75
queue = Queue()
rsync = "\"C:\\Program Files\\DeltaCopy\\rsync.exe\" -v -rlt -z --chmod=a=rw,Da+x --delete"
source = "/path/to/source"
destination = "/path/to/destination"

now = datetime.datetime.now()
logfiledate = now.strftime("%Y-%m-%d %H-%M-%S")
logfile = "D:\\logs\\threads_log" + logfiledate + "_log.txt"

def listdirs(folder):
	return [d for d in os.listdir(folder) if os.path.isdir(os.path.join(folder, d))]
    
directories = listdirs(source)
exclude = []

print "Beginning"
print datetime.datetime.now()
file = open(logfile, 'w')
file.write("Beginning\n")
file.write(str(datetime.datetime.now()) + "\n\n\n")
file.close()

print "pausing in case of debugging"
time.sleep(30)

def syncDirectory(i, queue):
	"""syncsDirectory"""
	#print "running thread", i
	while queue.empty != True:
		dir = queue.get()
		if dir in exclude:
			print "excluding", dir
			queue.task_done()
		else:
			file = open(logfile, 'a')
			file.write("  Beginning"+ dir +"\n")
			file.write("  " + str(datetime.datetime.now()) + "\n")
			file.close()
			
			print "syncing", dir
			#print "  " + source + "\\" + dir
			#print "  " + destination + "\\" + dir
			src = source + "/" + dir + "/"
			dest = destination + "/" + dir + "/"
			command = rsync + " " + src + " " + dest
			print command
			
			now = datetime.datetime.now()
            logfiledate = now.strftime("%Y-%m-%d %H-%M-%S")
			
			ret = subprocess.call(command,
								shell=True,
								stdout=open('D:\\'+ dir + logfiledate + '-log.txt', 'a'),
								stderr=subprocess.STDOUT)
			#print "running dir on", dir
			queue.task_done()
			file = open(logfile, 'a')
			file.write("  Finishing"+ dir +"\n")
			file.write("  " + str(datetime.datetime.now()) + "\n")
			file.close()
			print "done syncing", dir
			time.sleep(1)
			
		queuesize = queue.qsize()
		file = open(logfile, 'a')
		file.write("  " + str(queuesize) + " remaining \n")
		file.close()
		
		print queuesize, "remaining"

for i in range(num_threads):
	worker = Thread(target=syncDirectory, args=(i, queue))
	worker.setDaemon(True)
	worker.start()

for dir in directories:
	queue.put(dir)

print "Main Thread Waiting"
queue.join()
print "Done"
print datetime.datetime.now()
file = open(logfile, 'a')
file.write("Done\n")
file.write(str(datetime.datetime.now()) + "\n")
file.close()
