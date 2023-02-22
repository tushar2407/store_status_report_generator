from django.db.models import F
from django.http.response import JsonResponse
from django.shortcuts import render
from django.shortcuts import get_object_or_404

from loop.settings import MEDIA_ROOT, BASE_DIR

from main.models import Store, BusinessHours, StoreStatus, Report

from datetime import datetime, timedelta
import os
import pandas as pd
import pytz
import threading

# Create your views here.


def home(request):
    return render(request, 'home.html')

def trigger_report(request):
    r = Report()
    r.save()
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
        timestamp__week_day = F('store_id__businesshours__day_of_week')
    ).order_by('store_id', 'timestamp')
    ## get the data for the respective stores
    for store in stores:
        print(store, "STORES")
        data[store.store_id] = {
            'uptime':timedelta(0),
            'downtime': timedelta(0),
            'last_active': '',
            'last_inactive': '',
            # 'last_active_day':'',
            # 'last_inactive_day': ''
        }
        curr_statuses = statuses.filter(store_id = store) # get the status of the current store
        if not curr_statuses.count():
            continue
        ## next initialize the above declared dictionary for the current store
        if curr_statuses[0].status =='active':
            data[store.store_id]['last_active'] = curr_statuses[0].timestamp.time()
            try:
                bh = BusinessHours.objects.filter(store_id = store, day_of_week = curr_statuses[0].timestamp.weekday())[0]
            except:
                continue
            data[store.store_id]['uptime'] = curr_statuses[0].timestamp.replace(tzinfo=None) - datetime.combine(curr_statuses[0].timestamp.date(), bh.start_time)
            data[store.store_id]['last_inactive'] = bh.start_time ## as datetime object needed

        else:
            bh = BusinessHours.objects.filter(store_id = store, day_of_week = curr_statuses[0].timestamp.weekday())[0]
            timediff = (
                datetime.combine(curr_statuses[0].timestamp.date(), data[store.store_id]['last_inactive']) - \
                datetime.combine(curr_statuses[0].timestamp.date(), data[store.store_id]['last_active'])
                ).total_seconds()/2
            data[store.store_id]['last_active'] = bh.start_time
            data[store.store_id]['last_inactive'] = curr_statuses[0].timestamp.time()
            data[store.store_id]['downtime'] = timediff
            data[store.store_id]['uptime'] = timediff

        ## next iterate over the statuses of the current store in order to the last status
        for i in range(1, len(curr_statuses)-1):
            if curr_statuses[i].status=='active':
                ...
                if (data[store.store_id]['last_active'] > data[store.store_id]['last_inactive']): # active to active
                    ...
                    data[store.store_id]['uptime'] += curr_statuses[i].timestamp.replace(tzinfo=None) - datetime.combine(
                        curr_statuses[i].timestamp.date(),
                        data[store.store_id]['last_active']
                        )
                    data[store.store_id]['last_active'] = curr_statuses[i].timestamp.time()
                    # data[store.store_id]['last_active_day'] = curr_statuses[i].timestamp.date()
                else: # inactive to active
                    ...
                    addition_term = timedelta(seconds=(
                        curr_statuses[i].timestamp.replace(tzinfo=None) - datetime.combine(
                            curr_statuses[i].timestamp.date(), 
                            data[store.store_id]['last_inactive']
                            )
                        ).total_seconds()/2)
                    data[store.store_id]['downtime'] += addition_term
                    data[store.store_id]['uptime'] += addition_term
                    data[store.store_id]['last_active'] = curr_statuses[i].timestamp.time()
                    # data[store.store_id]['last_active_day'] = curr_statuses[i].timestamp.date()
                    # data[store.store_id]['last_inactive'] = data[store.store_id]['last_inactive']+addition_term
                    # data[store.store_id]['last_inactive_day'] = (curr_statuses[i].timestamp-data[store.store_id]['last_inactive']).date()

            elif curr_statuses[i].status=='inactive':
                ...
                if(data[store.store_id]['last_active'] > data[store.store_id]['last_inactive']): # active to inactive
                    ...
                    addition_term = timedelta(seconds=(
                        curr_statuses[i].timestamp.replace(tzinfo=None) - datetime.combine(
                            curr_statuses[i].timestamp.date(), 
                            data[store.store_id]['last_active']
                            )
                        ).total_seconds()/2)
                    data[store.store_id]['downtime'] += addition_term
                    data[store.store_id]['uptime'] += addition_term
                    data[store.store_id]['last_inactive'] = curr_statuses[i].timestamp.time()
                    # data[store.store_id]['last_inactive_day'] = curr_statuses[i].timestamp.date()
                    # data[store.store_id]['last_active'] += addition_term ## if exceeds the business time, then should be initialized to start of next day
                    # data[store.store_id]['last_active_day'] = curr_statuses[i].timestamp.date()
                else: # inactive to inactive
                    ...
                    data[store.store_id]['downtime'] += curr_statuses[i].timestamp - datetime.combine(
                        curr_statuses[i].timestamp.date(), 
                        data[store.store_id]['last_inactive']
                    )
                    data[store.store_id]['last_inactive'] = curr_statuses[i].timestamp.time()
                    # data[store.store_id]['last_inactive_day'] = curr_statuses[i].timestamp.date()
        
        ## last check the final status and do the needful for the results
        if curr_statuses.last().status =='active': ## last status is active
            if(data[store.store_id]['last_active'] > data[store.store_id]['last_inactive']): # active to active
                ...
                try:
                    bh = BusinessHours.objects.filter(store_id = store, day_of_week = curr_statuses.last().timestamp.weekday())[0]
                except:
                    continue
                # data[store.store_id]['uptime'] += curr_statuses.last().store_id.businesshours.end_time - data[store.store_id]['last_active']
                temp = datetime.now().date()
                data[store.store_id]['uptime'] += datetime.combine(temp, bh.end_time) - datetime.combine(temp, data[store.store_id]['last_active'])
                data[store.store_id]['last_active'] = bh.end_time
            else: # inactive to active
                ...
                addition_term = timedelta(seconds=(
                    curr_statuses.last().timestamp.replace(tzinfo=None) - datetime.combine(
                        curr_statuses.last().timestamp.date(), 
                        data[store.store_id]['last_inactive']
                        )
                    ).total_seconds()/2)
                data[store.store_id]['downtime'] += addition_term
                data[store.store_id]['uptime'] += addition_term
                data[store.store_id]['last_active'] = curr_statuses.last().timestamp.time()
                # data[store.store_id]['last_inactive'] += addition_term
        
        else: ## last status is inactive
            if(data[store.store_id]['last_active'] > data[store.store_id]['last_inactive']): # active to inactive
                ...
                addition_term = (curr_statuses.last().timestamp.replace(tzinfo=None) - datetime.combine(
                    curr_statuses.last().timestamp.date(), 
                    data[store.store_id]['last_active']
                    )
                ).total_seconds()/2
                data[store.store_id]['downtime'] += addition_term
                data[store.store_id]['uptime'] += addition_term
                data[store.store_id]['last_inactive'] = curr_statuses.last().timestamp.time()
                # data[store.store_id]['last_active'] += addition_term
            else: # inactive to inactive
                ...
                bh = BusinessHours.objects.filter(store_id = store, day_of_week = curr_statuses.last().timestamp.weekday())[0]
                temp = datetime.now().date()
                data[store.store_id]['downtime'] += datetime.combine(temp, bh.end_time) - datetime.combine(temp, data[store.store_id]['last_inactive'])
                data[store.store_id]['last_inactive'] = bh.start_time
    
    return data

def create_report(id):
    r = Report.objects.get(pk=id)
    curr = datetime(2023, 1, 25, tzinfo=pytz.UTC)
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
    r.file_name = f'{os.path.join(BASE_DIR, MEDIA_ROOT)}{id}.csv'
    r.status='Complete'
    r.save()
    pd.DataFrame(
        results, 
        columns=[
            'store_id', 'uptime_last_hour', 'uptime_last_day', 
            'uptime_last_week', 'downtime_last_hour', 'downtime_last_day', 
            'downtime_last_week']
    ).to_csv(f'{MEDIA_ROOT}{id}.csv')
