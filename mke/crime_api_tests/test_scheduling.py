#Need to add schedule library to requirements??

import schedule
import time

#Example
def job():
    print("I'm working...")

schedule.every(10).minutes.do(job)
schedule.every().hour.do(job)
schedule.every().day.at("10:30").do(job)
schedule.every().monday.do(job)
schedule.every().wednesday.at("13:15").do(job)
schedule.every().day.at("12:42", "Europe/Amsterdam").do(job)
schedule.every().minute.at(":17").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

##A schedule is set by doing `schedule.every(X).minute.do(job)`. When the schedule needs to execute, it will run the `job()` function.

##The code above polls every second using `time.sleep(1)` to check if the job needs to be run.


#We could use
#Run export and load once a week for new updates
def job1():
    print("I'm working...")

    schedule.every().sunday.do(job)
    #schedule.every().sunday.at("10:30").do(job)


while True:
    schedule.run_pending()
    time.sleep(86400)

#86400 seconds in a day
#We could use
#Run export and load once a day for any changes
def job2():
    print("I'm working...")

    schedule.every().day.do(job)


while True:
    schedule.run_pending()
    time.sleep(86400)

