# ymca_virtuagym_dashboard
Keeps track of gym attendees across branches: updates a .csv file by calling the Virtuagym API , categorizes events into group, swim or individual for analysis

## Overview

This project was made to help Central MA YMCA Management better understand attendance trends across all six branches during the COVID pandemic. Currently, the Central Mass YMCA uses a 3rd party application called Virtuagym to allow members to sign up for specific classes, or come in to work out during preset time blocks (classes and blocks are called 'events' in the Virtuagym system). The attached file is bundled with an .csv of the records of each branches' events, and using Pyinstaller, is run by a one-click .exe program for management to update records for use in a Tableau Dashboard. 

When the program is run, it looks up the last date of the events on the .csv, and makes an API call for each branch from that date to the current one, labeling each event as either 'individual' (general gym use), 'group' (group class) or 'swim'. Since recorded absences (only number of attendees who signed up for an event) aren't sent in the first API call, another call is made for each event. The data is then appended to the orignal .csv file.
