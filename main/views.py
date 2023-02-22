from django.db.models import F
from django.http.response import JsonResponse
from django.shortcuts import render
from django.shortcuts import get_object_or_404

from loop.settings import MEDIA_ROOT, BASE_DIR

from main.models import Store, BusinessHours, StoreStatus, Report

from datetime import datetime, timedelta
import os
import pandas as pd
import threading

# Create your views here.


def home(request):
    return render(request, 'home.html')

def trigger_report(request):
    r = Report().save()
    t = threading.Thread(target=create_report, args=[r.id])
    t.setDaemon(True)
    t.start()
    return JsonResponse({'id':r.id})

def get_report(request):
    report = get_object_or_404(Report, pk = request.GET.get('report_id'))
    if report.status=='Complete':
        return JsonResponse({'status': report.status, 'file': report.file_name})
    else:
        return JsonResponse({'status': 'Running'})

def get_history_data(history_time, stores):
    data = {}

    ## get the statuses within the busisness hours of the respective stores
    statuses = StoreStatus.objects.select_related().filter(
        timestamp__gte = history_time).filter(
        timestamp__gte = F('store_id__businesshours__start_time'), 
        timestamp__lte = F('store_id__businesshours__end_time'), 
        timestamp__weekday = F('store_id__businesshours__day_of_week')
    ).order_by('store_id', 'timestamp')
    ## get the data for the respective stores
    for store in stores:
        data[store.store_id] = {
            'uptime':timedelta(0),
            'downtime': timedelta(0),
            'last_active': '',
            'last_inactive': ''
        }
        curr_statuses = statuses.filter(store_id = store) # get the status of the current store
        ## next initialize the above declared dictionary for the current store
        if curr_statuses[0].status =='active':
            data[store.store_id]['last_active'] = curr_statuses[0].timestamp
            data[store.store_id]['uptime'] = curr_statuses[0].timestamp - curr_statuses[0].store_id.businesshours.start_time
            data[store.store_id]['last_inactive'] = curr_statuses[0].store_id.businesshours.start_time
        
        else:
            data[store.store_id]['last_active'] = curr_statuses[0].store_id.businesshours.start_time
            data[store.store_id]['last_inactive'] = curr_statuses[0].timestamp
            data[store.store_id]['downtime'] = (data[store.store_id]['last_inactive'] - data[store.store_id]['last_active']).total_seconds()/2
            data[store.store_id]['uptime'] = (data[store.store_id]['last_inactive'] - data[store.store_id]['last_active']).total_seconds()/2
        ## next iterate over the statuses of the current store in order to the last status
        for i in range(1, len(curr_statuses)-1):
            if curr_statuses[i].status=='active':
                ...
                if(data[store.store_id]['last_active'] > data[store.store_id]['last_inactive']): # active to active
                    ...
                    data[store.store_id]['uptime'] += curr_statuses[i].timestamp - data[store.store_id]['last_active']
                    data[store.store_id]['last_active'] = curr_statuses[i].timestamp
                else: # inactive to active
                    ...
                    data[store.store_id]['downtime'] += (curr_statuses[i].timestamp - data[store.store_id]['last_inactive']).total_seconds()/2
                    data[store.store_id]['uptime'] += (curr_statuses[i].timestamp - data[store.store_id]['last_inactive']).total_seconds()/2
                    data[store.store_id]['last_active'] = curr_statuses[i].timestamp

            elif curr_statuses[i].status=='inactive':
                ...
                if(data[store.store_id]['last_active'] > data[store.store_id]['last_inactive']): # active to inactive
                    ...
                    data[store.store_id]['downtime'] += (curr_statuses[i].timestamp - data[store.store_id]['last_active']).total_seconds()/2
                    data[store.store_id]['uptime'] += (curr_statuses[i].timestamp - data[store.store_id]['last_active']).total_seconds()/2
                    data[store.store_id]['last_inactive'] = curr_statuses[i].timestamp
                else: # inactive to inactive
                    ...
                    data[store.store_id]['downtime'] += curr_statuses[i].timestamp - data[store.store_id]['last_inactive']
                    data[store.store_id]['last_inactive'] = curr_statuses[i].timestamp
        
        ## last check the final status and do the needful for the results
        if curr_statuses.last().status =='active': ## last status is active
            if(data[store.store_id]['last_active'] > data[store.store_id]['last_inactive']): # active to active
                ...
                data[store.store_id]['uptime'] += curr_statuses[i].store_id.businesshours.end_time - data[store.store_id]['last_active']
                data[store.store_id]['last_active'] = curr_statuses[i].store_id.businesshours.start_time
            else: # inactive to active
                ...
                data[store.store_id]['downtime'] += (curr_statuses[i].timestamp - data[store.store_id]['last_inactive']).total_seconds()/2
                data[store.store_id]['uptime'] += (curr_statuses[i].timestamp - data[store.store_id]['last_inactive']).total_seconds()/2
                data[store.store_id]['last_active'] = curr_statuses[i].timestamp
        
        else: ## last status is inactive
            if(data[store.store_id]['last_active'] > data[store.store_id]['last_inactive']): # active to inactive
                ...
                data[store.store_id]['downtime'] += (curr_statuses[i].timestamp - data[store.store_id]['last_active']).total_seconds()/2
                data[store.store_id]['uptime'] += (curr_statuses[i].timestamp - data[store.store_id]['last_active']).total_seconds()/2
                data[store.store_id]['last_inactive'] = curr_statuses[i].timestamp
            else: # inactive to inactive
                ...
                data[store.store_id]['downtime'] += curr_statuses[i].store_id.businesshours.end_time - data[store.store_id]['last_inactive']
                data[store.store_id]['last_inactive'] = curr_statuses[i].store_id.businesshours.start_time
    
    return data

def create_report(id):
    r = Report.object.get(pk=id)
    curr = datetime.now()
    last_hour = curr - timedelta(hours=1)
    last_day = curr - timedelta(days=1)
    last_week = curr - timedelta(weeks=1)
    '''
    store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week
    '''
    stores = Store.objects.all()
    # store_status = StoreStatus.objects.filter(timestamp__gte=last_week).order_by('store_id', 'timestamp')
    # last_hour_status = store_status.filter(timestamp__gte=last_hour).order_by('store_id', 'timestamp')
    
    data_last_hour = get_history_data(last_hour, stores)
    data_last_day = get_history_data(last_day, stores)
    data_last_week = get_history_data(last_week, stores)

    results = []
    for store in stores:
        results.append(
            [
                store.store_id,
                data_last_hour[store.store_id]['uptime'] if data_last_hour.get(store.store_id) else '',
                data_last_day[store.store_id]['uptime'] if data_last_day.get(store.store_id) else '',
                data_last_week[store.store_id]['uptime'] if data_last_week.get(store.store_id) else '',
                data_last_hour[store.store_id]['downtime'] if data_last_hour.get(store.store_id) else '',
                data_last_day[store.store_id]['downtime'] if data_last_day.get(store.store_id) else '',
                data_last_week[store.store_id]['downtime'] if data_last_week.get(store.store_id) else '',
            ]
        )
    pd.DataFrame(
        results, 
        columns=[
            'store_id', 'uptime_last_hour', 'uptime_last_day', 
            'uptime_last_week', 'downtime_last_hour', 'downtime_last_day', 
            'downtime_last_week']
    ).to_csv(f'{os.path.join(BASE_DIR, MEDIA_ROOT)}{id}.csv')
    r.file_name = f'{os.path.join(BASE_DIR, MEDIA_ROOT)}{id}.csv'
    r.status='Complete'
    r.save()


    
    
#     for store in stores:
#         bhours = BusinessHours.objects.filter(store_id = store.store_id)
#         bdays = set([bday for bday in bhours.values_list('day_of_week')])
#         curr.weekday
#         statuses = last_hour_status.filter(store_id = store.store_id)
#         data[store.store_id] = {
#             'last_uptime': '',
#             'last_downtime':''
#         }
#         ## talk about last_status
#         bhour = check_within_bhours(bhours, status[0])
#         if bhour

#         for i in range(1, len(statuses)-1):
#             status = statuses[i]
#             bhour = check_within_bhours(bhours, status)
#             if bhour:
#                 if(last_status=='active' and status.status == 'active'):
#                     ...

#                 if(last_status=='active' and status.status == 'inactive'):
#                     ...
#                 if(last_status=='inactive' and status.status == 'active'):
#                     ...
#                 if(last_status=='inactive' and status.status == 'inactive'):
#                     ...



        

#     # Uptime last hour

# def check_within_bhours(bhours, status):
#     return bhours.get(
#         day_of_week = status.timestamp.week_day, # the status should be on the days the store is open
#         start_time__lte=status.timestamp.time(), # the status should have a time after the store is opened on that day
#         end_time__gte=status.timestamp.time() # the status should have a time before the store gets closed on that day
#         )