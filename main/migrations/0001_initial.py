# Generated by Django 4.1.7 on 2023-02-22 12:45

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Store',
            fields=[
                ('store_id', models.BigIntegerField(primary_key=True, serialize=False)),
                ('timezone', models.CharField(default='America/Chicago', max_length=256)),
            ],
        ),
        migrations.CreateModel(
            name='StoreStatus',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('status', models.CharField(choices=[('active', 'active'), ('inactive', 'inactive')], max_length=256)),
                ('timestamp', models.DateTimeField()),
                ('store_id', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='main.store')),
            ],
        ),
        migrations.CreateModel(
            name='BusinessHours',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('start_time', models.TimeField()),
                ('end_time', models.TimeField()),
                ('day_of_week', models.IntegerField(default=0)),
                ('store_id', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='main.store')),
            ],
        ),
    ]
