## Installation

The easiest way to install is to clone this repository to your home directory and then pip3 install
```
cd ~
git clone https://github.com/Snackman8/DataHub
cd DataHub
sudo pip3 install .
```

Create the directory where DataHub_Modules will reside
```
sudo mkdir /srv/DataHub_Modules
```

Copy the supervisor conf file and reload supervisor so DataHub will auto start
```
sudo cp ~/DataHub/deployment_configs/supervisor_DataHub.conf /etc/supervisor/conf.d/
sudo supervisorctl reload
sudo supervisorctl restart DataHub
```
