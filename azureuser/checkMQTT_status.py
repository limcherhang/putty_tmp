import subprocess
from utils import util

mail_username = 'alin@evercomm.com.sg'
mail_password = 'xhzygviprwmqdutz'
from_email = 'Alerts MQTT Quit'
to_email = 'cherhanglim0227@gmail.com'
Subject = "MQTT Quit Alerts"

shell_command = "ps -ef | grep py"

result = subprocess.run(shell_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

result_stdout = result.stdout

if "python3 MQTT_SG.py --subType=bacnet" not in result_stdout:

    util.alert_line("python3 MQTT_SG.py --subType=bacnet not found")

if "python3 MQTT_SG.py --subType=modbus" not in result_stdout:
    util.alert_line("python3 MQTT_SG.py --subType=modbus not found")

if "python3 MQTT_SG.py --subType=zigbee" not in result_stdout:
    util.alert_line("python3 MQTT_SG.py --subType=zigbee not found")

if "python3 MQTT_SG.py --subType=m2m" not in result_stdout:
    util.alert_line("python3 MQTT_SG.py --subType=m2m not found")

if "python3 MQTT_TW.py --subType=bacnet" not in result_stdout:
    util.alert_line("python3 MQTT_TW.py --subType=bacnet not found")

if "python3 MQTT_TW.py --subType=modbus" not in result_stdout:
    util.alert_line("python3 MQTT_TW.py --subType=modbus not found")

if "python3 MQTT_TW.py --subType=zigbee" not in result_stdout:
    util.alert_line("ppython3 MQTT_TW.py --subType=zigbee not found")

if "python3 MQTT_PPSS.py" not in result_stdout:
    util.alert_line("python3 MQTT_PPSS.py not found not found")

if result.stderr:
    print("错误输出：", result.stderr)