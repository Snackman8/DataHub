# DataHub

To install, clone the project into /srv

Copy /srv/DataHub/deployment_configs/supervisor_DataHub.conf to /etc/supervisor/conf.d/

sudo mkdir /srv/DataHub_Modules

sudo supervisorctl reload

sudo supervisorctl restart DataHub
