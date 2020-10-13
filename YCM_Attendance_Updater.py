
if __name__ == "__main__":

    import requests
    import datetime as dt
    import time
    import numpy as np
    import pandas as pd
    import random

    #structure is club then API
    auth_dct = {
                'Location1': ('club_key1','api_key1'),
                'Location2': ('club_key2','api_key2'),
    }

    #PRIMARY API EVENT CALL/FORMATTING

    def get_club_events(start_time, end_time, keys):
        #calls virtuagym API and returns JSON result
        api_key = keys[1]
        club_key = keys[0]
        club_id = club_key.split('-')[1]
        api_call = 'https://api.virtuagym.com/api/v1/club/{}/events?api_key={}&club_secret={}&timestamp_start={}&timestamp_end={}'.format(
            club_id,api_key,club_key,start_time,end_time)
        response_lst = []
        
        response = requests.get(api_call).json()
        response_lst += response['result']
        
        
        try:
            remaining = response['status']['results_remaining']

            #create loop to handle pagination limits
            while remaining > 0:
                next_page = response['status']['next_page']
                api_call2 = '{}&{}'.format(api_call,next_page)
                response = requests.get(api_call2).json()
                response_lst += response['result']
                remaining = response['status']['results_remaining']
        except:
            pass
            
        return response_lst

    def gym_stat_cleaner(response, location):
        """gets columns we want from API call"""
        gym_stats = pd.DataFrame(response)
        
        #select relevant columns
        gym_stats= gym_stats[['title','attendees', 'canceled',
        'event_id', 'instructor_id',
        'max_places', 'presence_saved', 'schedule_id',
        'activity_id', 'start', 'end']]
        
        #create/edit time columns
        gym_stats['start'] = gym_stats['start'].astype('datetime64[ns]')
        gym_stats['end'] = gym_stats['end'].astype('datetime64[ns]')
        gym_stats['day'] = gym_stats['start'].dt.strftime('%A')
        gym_stats['block'] = gym_stats['start'].dt.round('60min').dt.time
        
        #create other columns
        gym_stats['location'] = location
        gym_stats['absent_count'] = np.nan
        gym_stats['est_absent_count'] = np.nan
        gym_stats['visitors'] = np.nan
        gym_stats['category'] = 'group'
        
        #remove canceled appts
        gym_stats = gym_stats[gym_stats['canceled'] == False]

        #reorder columns
        correct_order = ['location','title', 'block','day','date','attendees', 'max_places', 'start', 'end',  
                        'event_id', 'instructor_id','presence_saved', 'schedule_id','activity_id', 
                        'category', 'absent_count', 'est_absent_count', 'visitors']
        gym_stats = gym_stats[correct_order]

        return gym_stats


    #PRIMARY EVENT DATA CLEANING/AGGREGATION

    def remove_events(gyms):
        #removes events from df that shouldn't be tracked
        
        #decapitalize all titles
        gyms['title']  = gyms['title'].str.lower()
        notrack_list = ['personal training', 'private lesson', 'holiday', 
                        'family swim', 'childwatch', 'open swim', 'family splash']
        #remove 
        gyms = gyms[~gyms['title'].str.contains('|'.join(notrack_list))]
        gyms = gyms[~gyms['title'].str.startswith('camp')] #avoids removing items like "boot camp"

        return gyms

    def populate_categories(gyms):
        #adds swim and individual exercise categories to dataframe
        
        #inidividual category
        fitness = ['fitness, strength training/bodybuilding','fitness center',
                'wellness center','exercise','wellness: cardio and weights']
        individual_mask = gyms['title'].str.contains('|'.join(fitness),na=False) 
        gyms.loc[individual_mask, 'category'] = 'individual'
        
        #recat group `exercise for all` class 
        exceptions = ['exercise for all']
        group_mask = gyms['title'].str.contains('|'.join(exceptions),na=False)
        gyms.loc[group_mask,'category'] = 'group'
        
        #swim category
        swim_mask = gyms['title'].str.contains('lap swim|lane #',na=False)
        gyms.loc[swim_mask,'category'] = 'swim'
        
        return gyms




    ##API CALL FOR ABSENCE/Visitors Functions

    def get_event_details(df):
        #input: dataframe for a given period of time
        #output: list of two series: # of absences, unique visitors.
        
        
        absent_values = []
        unique_visitors = []
        
        #iterate through events/date in dataframe
        for iterr in df.iterrows():
            absent_count = 0
            uniques = []
            row = iterr[1]
            
            #errorchek for too many API requests 
            try:
                result = get_event_data(row['event_id'],
                                        row['start'].timestamp(),
                                        auth_dct[row['location']])
            except:
                print('too many API requests')

                time.sleep(3601) #wait 1 hour for reset
                result = get_event_data(row['event_id'],
                                        row['start'].timestamp(),
                                        auth_dct[row['location']])
            for member in result:
                #get absences
                if member['present'] == False:
                    absent_count +=1
                
                #get member_ids (of those present)
                else:
                    if member['member_id'] == 0:
                        #sometimes guests don't have user_name fields? idk 
                        try:
                            uniques.append(member['user_name'])
                        except:
                            uniques.append('guest{}'.format(random.randint(1,50000)))
                    else:
                        uniques.append(member['member_id'])

            absent_values.append(absent_count)
            unique_visitors.append(uniques)
            
        return [pd.Series(absent_values,index=df.index),pd.Series(unique_visitors,index=df.index)] 




    def get_event_data(event_id,start_time,keys):
        #used in get_event_details, calls virtuagym API for data regarding a specified event, 
        api_key = keys[1]
        club_key = keys[0]
        club_id = club_key.split('-')[1]
        api_call='https://api.virtuagym.com/api/v1/club/{club_id}/eventparticipants?api_key={api_key}&club_secret={club_secret}&event_id={event_id}&timestamp_start={timestamp_start}'.format(
            club_id=club_id,api_key=api_key,club_secret=club_key,event_id=event_id,timestamp_start=start_time)#,timestamp_end=end_time)
        response = requests.get(api_call).json()
        #error checking
        if response['status']['statuscode'] != 200:
            print('failed API call for {}'.format(event_id))
            
        return response['result']


    ### Estimate non-saved Absences

    def estimate_unsaved_absences(row, gb_mean, gb_limit):
        absents = row['absent_count']
        #set new absent value to old one
        new_absents = absents
        #alter new_absent value if needed
        
        if row['presence_saved'] == False and row['attendees'] > 0:
            
            #get matching mean/limit values
            try:
                mean = gb_mean[row['location']][row['category']][row['weekend']]
                limit = gb_limit[row['location']][row['category']][row['weekend']] #limit is (mean + std) for absences
            #if values don't exist for that specific entry
            except:
                mean = gb_mean[row['location']][row['category']][0]
                limit = gb_limit[row['location']][row['category']][0]
                
            
            #staff didn't mark anyone present
            if absents == row['max_places']:
                new_absents =  mean
            
            #staff only marked a couple people present
            elif absents > limit:
                new_absents = limit + 1

        return int(new_absents)


    def calculate_means(df):
        #calculate mean + std values from 'saved' data
        df['weekend'] = df['day'].str.contains('Saturday|Sunday')
        save_mask = df['presence_saved'] == True
        date_start = df.tail(1)['start'].values[0] - pd.Timedelta(days=60) #only base averages off last two months
        date_mask = df['start'] > date_start
        gb_mean = df[(save_mask) & (date_mask)].groupby(['location','category','weekend'])['absent_count'].agg(np.mean)
        gb_std = df[(save_mask) & (date_mask)].groupby(['location','category','weekend'])['absent_count'].agg(np.std)
        gb_std[gb_std.isnull()] = 0 #incase there isn't enough data for a stdv
        gb_limit = round(gb_mean+gb_std).astype(int)
        return [gb_mean, gb_limit]


    def combine_tricommunity_swim(gyms):
        #aggregate Tri-Community lane entries into single 'lap swim' entry
        
        lanes = gyms.loc[(gyms['location'] == 'Tri-Community') & (gyms['category'] == 'swim')]
        start_times = lanes['start'].value_counts().index
        
        for time in start_times:
            block = lanes[lanes['start'] == time]
            gyms.loc[block.index[0],'attendees'] = block['attendees'].sum()
            gyms.loc[block.index[0],'max_places'] = block['max_places'].sum()
            gyms.loc[block.index[0],'title'] = 'lap swim'
        
        #remove any events w/ lane in it 
        gyms = gyms[~gyms['title'].str.contains('lane #')]
        
        return gyms



    #null handler
    def handle_null_titles(df):
        nulls = df[df['title'].isnull()]
        nullsize = nulls.shape[0]
        if nullsize > 0:
            #check if there are multiple null activities:
            nullacts = list(nulls['activity_id'].value_counts().index)
            
            print('there are {} different titles with null values. Would you like to replace?'.format(len(nullacts)))
            v1 = input('(y/n): ')
            if v1 == 'y':
                for item in nullacts:
                    actmask = nulls['activity_id'] == item
                    nullrep = nulls[actmask].iloc[0]
                    print('location = {}, date = {}, day={}, start = {}, end = {}'.format(nullrep['location'],nullrep['date'],
                                                                                        nullrep['start'].day_name(),
                                                                                        nullrep['start'].time(),
                                                                                        nullrep['end'].time()
                                                                                        ))
                    newtitle = input('Enter new title: ')
                    df.loc[(actmask) & (df['title'].isnull()),'title'] = newtitle
                    print('Thank you. please consider contacting an administrator to fix this events title.')
            else:
                print('NA titles will remain in dataset. contact admin to address')    
        
        return df





    #Final program

    def update_attendance():
        #read in previous dataframe, convert columns so append works
        year_attnd = pd.read_csv('YCM_Attendance_2020.csv')
        year_attnd = year_attnd.drop(columns=['Unnamed: 0'])
        year_attnd['start'] = pd.to_datetime(year_attnd['start'])
        year_attnd['end'] = pd.to_datetime(year_attnd['end'])
        year_attnd = year_attnd.astype({'est_absent_count':'float64'})
        
        #get dates to pull event data for 
        d1 = (year_attnd.tail(1).iloc[0]['start'] + pd.DateOffset(days=1)).replace(hour=0)
        d2 = dt.datetime.now().replace(hour=0)
        print('getting event data for {} to {}'.format(d1.date(),d2.date()))
        d1 = d1.timestamp()
        d2 = d2.timestamp()

        #get event data for each location
        dfs = []
        for location in auth_dct:
            #get new event data
            new_club_events = get_club_events(d1,d2,auth_dct[location])
            df = gym_stat_cleaner(new_club_events, location)
            dfs.append(df)
        new_attnd = pd.concat(dfs).sort_values('start')
        
        #clean df
        new_attnd = remove_events(new_attnd.reset_index(drop=True))
        new_attnd = populate_categories(new_attnd)
        new_attnd = combine_tricommunity_swim(new_attnd)

        #get absences/visitors (note Tri-Community absence data is not accurate here, )
        absents, visitors = get_event_details(new_attnd)
        new_attnd['absent_count'] = absents
        new_attnd['visitors'] = visitors
        
        #calculate mean, mean + std from past two months for estimated absences
        year_attnd.append(new_attnd,sort=False).sort_values('start')
        gb_mean, gb_limit = calculate_means(year_attnd)
        new_attnd['est_absent_count'] = new_attnd.apply(estimate_unsaved_absences, args=(gb_mean,gb_limit),axis=1)
         #set central values to 0:
        c_mask = new_attnd['location'] == 'Central'
        ind_mask = new_attnd['category'] == 'individual'
        new_attnd.iloc[(c_mask) & (ind_mask), 'est_absent_count'] = 0


        #check null values
        new_attnd = handle_null_titles(new_attnd)

        #remove extra columns
        correct_order = ['location', 'title', 'block', 'day', 'date', 'attendees', 'max_places', 'start', 'end', 'event_id', 'instructor_id', 'presence_saved', 'schedule_id', 'activity_id', 'category', 'absent_count', 'est_absent_count','visitors']
        new_attnd= new_attnd[correct_order]
        
        #append to file
        new_attnd.to_csv('YCM_Attendance_2020.csv', header=False,mode='a')
        print('done.')


    update_attendance()
