# Generated by Django 3.1.3 on 2022-02-15 20:07

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('drf_api_logger', '0003_apilogsmodel_user'),
    ]

    operations = [
        migrations.AddField(
            model_name='apilogsmodel',
            name='response_size',
            field=models.DecimalField(blank=True, decimal_places=4, max_digits=16, null=True),
        ),
    ]
