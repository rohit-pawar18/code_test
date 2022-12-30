import sys
import datetime
import pandas as pd

def loadLogs():
    """ Load and valdate logs file. """

    try:
        log_file = sys.argv[1]
        with open(log_file, 'r') as file:
            return file.read()
    except:
        raise Exception("No file provide in command line or currupted files.")

def validate_time(time_text):
        try:
            datetime.datetime.strptime(time_text, '%H:%M:%S')
            return True
        except ValueError:
            return False

def clean_logs(logs):
    """ Clean logs for wrong values and empty rows."""
    # remove logs without any username, start/end or no time
    cleaned_logs = []
    for splited_logs in logs.split("\n"):
        words = splited_logs.split(" ")
        if len(words) > 2 and words[1].isalnum() and words[2].lower() in ['start','end'] and validate_time(words[0]):
            cleaned_logs.append(words)

    return pd.DataFrame(cleaned_logs, columns = ['time', 'name', 'status'])



def prepare_report(df):
    """ Method to calculate the diffrent in seconds and sessions"""

    df['time'] = pd.to_datetime(df['time'])

    # group by name
    for idx, group in df.groupby('name'):
        _list = []
        seconds = 0
        while not group.empty:
            _dict = {}
            start_found = False
            group = group.sort_values(['time','status'], ascending=True).reset_index(drop=True)
            
            # add end time if not avalible 
            if 'Start' in list(group['status']) and 'End' not in list(group['status']):
                data = [{'time':df.time.max(),'name':group['name'].reset_index(drop=True)[0],'status':'End'}]
                group = group.append(data,ignore_index=True,sort=True)

            # add Start time if not avalible 
            if 'End' in list(group['status']) and 'Start' not in list(group['status']):
                data = [{'time':df[df['status']=='Start']['time'].min(),'name':group['name'].reset_index(drop=True)[0],'status':'Start'}]
                group = group.append(data,ignore_index=True,sort=True)

            # trace and calculate time between start and end.
            for idx1, row in group.iterrows():
                if row['status'] == 'Start' and not start_found:
                    _dict.update({'Start':row['time']})
                    group = group.drop(idx1)
                    start_found = True
                    if 'End' in _dict.keys():
                        seconds += (_dict['End'] - _dict['Start']).total_seconds()
                        break
                if row['status'] == 'End':
                    _dict.update({'End':row['time']})
                    group = group.drop(idx1)
                    _list.append(_dict)
                    if 'Start' in  _dict.keys():
                        seconds += (_dict['End'] - _dict['Start']).total_seconds()
                        break
        # print the diffrence per user.
        print(f"==========={idx}========{len(_list)}============{abs(seconds)}==========")

    return df            

if __name__ == '__main__':
   logs = loadLogs()
   cleand_logs = clean_logs(logs)
   prepare_report(cleand_logs)
