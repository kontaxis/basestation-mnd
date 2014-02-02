#!/bin/bash

sqlite3=`which sqlite3`

if [ "$sqlite3" == "" ]; then 
	echo "[!] unable to find sqlite3 binary. aborting."
	exit
fi

touch flights.sqlite3

if [ "$?" != "0" ]; then
	echo "[!] failed to get write access to flights.sqlite3"
	exit
fi

sqlite3 flights.sqlite3 ".schema"

if [ "$?" != "0" ]; then
	echo "[!] sqlite3 failed to dump schema off flights.sqlite3"
	exit
fi

sqlite3 flights.sqlite3 "drop table data"

if [ "$?" != "0" ]; then
	echo "[!] sqlite3 failed to drop table 'data' from flights.sqlite3"
	exit
fi

sqlite3 flights.sqlite3 "create table data (tstamp integer, feed text, notes text, MessageType text, TransmissionType text, SessionID text, AircraftID text, HexIdent text, FlightID text, DateReceived text, TimeReceived text, DateLogged text, TimeLogged text, Callsign text, Altitude text, GroundSpeed text, Track text, Latitude text, Longitude text, VerticalRate text, Squawk text, Alert text, Emergency text, SPI text, IsOnGround)"

if [ "$?" != "0" ]; then
	echo "[!] sqlite3 failed to create table 'data' in flights.sqlite3"
	exit
fi

sqlite3 flights.sqlite3 ".schema"

if [ "$?" != "0" ]; then
	echo "[!] sqlite3 failed to dump new schema off flights.sqlite3"
	exit
fi

