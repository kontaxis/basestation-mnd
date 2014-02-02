#!/usr/bin/python

import thread
import time
import socket
import string
import copy
import sys
import sqlite3
import signal, os

# -------------------------------------------------- #
# XXX modify feeds array to add/remove producers XXX #
# -------------------------------------------------- #

feeds = [
	{'host': '192.168.110.10', 'port': 30003, 'alias': 'charlie'},
	{'host': '192.168.110.14', 'port': 30003, 'alias': 'tango'  }
]

# ---------------------------------------------- #
# XXX do NOT modify anything below this line XXX #
# ---------------------------------------------- #

# termination condition
should_Terminate = False


def produce_feed (feed):

	sys.stderr.write("[x] [%s] producer started\n" 
		% (feed['alias']))

	# connect to feed
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((feed['host'], feed['port']))
	except socket.error as e:
		sys.stderr.write("[!] [%s] FATAL %s:%d %s\n" 
			% (feed['alias]'], feed['host'], feed['port'], e))
		return

	sys.stderr.write("[x] [%s] connected to feed %s:%d\n" 
		% (feed['alias'], feed['host'], feed['port']))

	# read feed and append data
	while 1:

		try:
			data = s.recv(1024)
			if not data:
				sys.stderr.wiret("[x] [%s] no data. exiting.\n" 
					% (feed['alias']))
				break
		except socket.error as e:
			sys.stderr.write("[x] [%s] %s:%d %s\n e" 
				% (feed['alias'], feed['host'], feed['port'], e))
			continue			

		#sys.stdout.write("[x] [%s] received %d bytes\n" 
		#	% (feed['alias'], len(data)))

		# lock here
		feed['lock'].acquire()

		#
		prev_dataLen = len(feed['data'])

		feed['data'] += data

		current_dataLen = len(feed['data'])

		#sys.stdout.write("[x] [%s] mydataList chunks %d -> %d\n" 
		#	% (feed['alias'], prev_dataLen, current_dataLen))

		# unlock here
		feed['lock'].release()

	sys.stdout.write("[x] [%s] goodbye\n" 
		% (feed['alias']))


# signal handler. graceful termination

def handler(signum, frame):

	global should_Terminate

	sys.stderr.write("[!] caught signal %d\n" % signum)

	if (signum == signal.SIGINT):
		sys.stderr.write("[!] SIGINT has been recorded\n")
		should_Terminate = True


# init

signal.signal(signal.SIGINT, handler)

# temporary data list used by CONSUMER worker
_dataList = []

index = 0

for feed in feeds:

	feed['data'] = ''
	feed['lock'] = thread.allocate_lock()

	_dataList.append({
		"host": feed['host'], "port": feed['port'], 
		"alias": feed['alias'], "index": index, "data": ''
	})

	feed['index'] = index
	index += 1

# start threads

try:

	for feed in feeds:
		thread.start_new_thread(produce_feed, (feed, ))

except:

	sys.stderr.write("[!] ERROR unable to start thread for %s\n" % feed)
	print(sys.exc_info())

# consume feed data

prev_time = 0

while 1:

	if (should_Terminate == True):
		sys.stdout.write("[*] Termination condition met. Goodbye\n")
		break
	
	# if time now - time of last SQLite insert is < T 
	# then continue else sleep (pace your self)

	if (time.time() - prev_time < 5):
		#sys.stdout.write("[*] consumer attempted too fast. sleeping ...\n")
		time.sleep(5)
		continue

	#sys.stdout.write("[*] consumer processing ...\n")

	# XXX make a quick copy of data XXX 

	for feed in feeds:

		if (feed["lock"].acquire(0)):

			#sys.stdout.write("[*] processing feed %s\n" % feed["alias"])

			prev_dataLen = len(_dataList[feed["index"]]["data"])

			_dataList[feed["index"]]["data"] += feed["data"]

			current_dataLen = len(_dataList[feed["index"]]["data"])

			#if (prev_dataLen != current_dataLen):
			#	sys.stdout.write("[*] feed %s grew %d -> %d bytes\n" 
			#		% (feed['alias'], prev_dataLen, current_dataLen))

			feed["data"] = ''

			feed["lock"].release()

	# xxx consume and free

	for _d in _dataList:

		if (len(_d["data"]) == 0):
			continue

		# split by newline
		_d_frags = _d["data"].split("\r\n")

		tokens = None

		# keep complete lines only, push rest back into _dataList

		if (_d["data"][len(_d["data"])-2:] == '\r\n'):
			_d["data"] = ''
			tokens = _d_frags
		else:
			_d["data"] = ''
			_d["data"] += _d_frags[len(_d_frags)-1]
			tokens = _d_frags[0:len(_d_frags)-1]

		# stats info

		sys.stderr.write(
			"[*] [%d] processing %s yielded %d messages and %d residual bytes\n" 
			% (time.time(), _d["alias"], len(tokens), 
					len(_d["data"])))

		# assert

		#if (len(_d_frags) != len(tokens)):
		#	sys.stdout.write(
		#		"[*] --- BEGIN TOKEN ---\n%s\n--- END TOKEN ---\n" % _d_frags)
		#	sys.stdout.write(
		#		"[*] --- BEGIN FINAL TOKEN ---\n%s\n--- END FINAL TOKEN ---\n" 
		#		% tokens)

		#sys.stdout.write(tokens)

		# --------------------
		# commit to sqlite3 db
		# --------------------

		con = None

		try:

			con = sqlite3.connect('flights.sqlite3')

			cur = con.cursor()

			placeholders = '?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?'

			tokens_fragments = []

			for token in tokens:

				if (token == ''):
					continue

				token_fragments = token.split(",")

				if (token_fragments < 10):
					sys.stderr.write(
						"[!] WARNING. %d fragments < min 10. Ignoring record.\n" 
						% len(token_fragments))
					print(token)
					print(token_fragments)
					continue

				if (token_fragments[0] == "MSG"):

					if (len(token_fragments) != 22):
						sys.stderr.write(
							"[!] WARNING. %d fragments != expected 22. Ignoring record.\n" 
							% len(token_fragments))
						print(token)
						print(token_fragments)
						continue

				elif (token_fragments[0] == "SEL" or \
							token_fragments[0] == "ID"  or 
							token_fragments[0] == "AIR" or 
							token_fragments[0] == "STA" or 
							token_fragments[0] == "CLK"):

					while (len(token_fragments) < 22):
						token_fragments.append('')

					if (len(token_fragments) != 22):
						sys.stderr.write(
							"[!] WARNING. %d fragments != expected 22. Ignoring record.\n"
							% len(token_fragments))
						print(token)
						print(token_fragments)

				sys.stderr.write(".")

				tokens_fragments.append(token_fragments)

			#sys.stderr.write("\n")

			query = 'insert into data values (\'%s\', %d, \'%s\', %s)' \
				% (_d["host"] + ':' + str(_d["port"]), time.time(), _d["alias"], 
						placeholders)

			cur.executemany(query, tokens_fragments)

			con.commit()

			sys.stderr.write("+\n")

		except sqlite3.Error as e:

			print("sqliteError: %s" % e.args[0])
			print(query)
			print(tokens_fragments)

		finally:

			if con:
				con.close()

	prev_time = time.time()
