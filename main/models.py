from django.db import models

# Create your models here.

class Store(models.Model):
    store_id = models.BigIntegerField(primary_key=True)
    timezone = models.CharField(
        max_length=256, 
        default='America/Chicago'
        )

class StoreStatus(models.Model):
    store_id = models.ForeignKey(
        'Store', 
        on_delete=models.CASCADE
        )
    status = models.CharField(
        max_length=256, 
        choices=(
            ('active', 'active'), 
            ('inactive', 'inactive')
            )
        )
    timestamp = models.DateTimeField()

class BusinessHours(models.Model):
    store_id = models.ForeignKey(
        'Store', 
        on_delete=models.CASCADE
        )
    start_time = models.TimeField()
    end_time = models.TimeField()
    day_of_week = models.IntegerField(default=0)

class Report(models.Model):
    file_name = models.CharField(max_length=256)
    status = models.CharField(
        max_length=256, 
        default='Running', 
        choices=(
            ('Running', 'Running'), 
            ('Complete', 'Complete')
            )
        )