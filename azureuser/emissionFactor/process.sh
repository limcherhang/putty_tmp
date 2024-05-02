sudo mysql -h 20.205.24.18 -u eco -p < emissionFactor.sql # will change hostname, username and password

cd /home/azureuser/emissionFactor/emissionDB && python3 complete_mongoInfo.py
cd /home/azureuser/emissionFactor/emissionDB && python3 complete_emissionAsset.py
cd /home/azureuser/emissionFactor/emissionDB && python3 demov2.py
