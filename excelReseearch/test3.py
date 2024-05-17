import paramiko

hostname = "20.212.250.116"
port = 22
username = "azureuser"
private_key_path = "./NXMap-Prod_key.pem"

ssh = paramiko.SSHClient()

# 加载私钥
private_key = paramiko.RSAKey.from_private_key_file(private_key_path)

# 连接到服务器
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname, port, username, pkey=private_key)

# 连接成功后，可以执行你想要的操作，比如执行命令
stdin, stdout, stderr = ssh.exec_command('ls -l')

# 输出命令执行结果
print(stdout.read().decode())

# 关闭SSH连接
ssh.close()