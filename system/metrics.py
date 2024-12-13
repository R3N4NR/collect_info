import psutil
import socket
import cpuinfo
from getmac import get_mac_address

def collect_system_metrics():
    discos_info = []
    hostname = socket.gethostname()
    cpu_info = cpuinfo.get_cpu_info()
    cpu_percent = psutil.cpu_percent(interval=1)
    virtual_memory = psutil.virtual_memory()
    memoria_total = virtual_memory.total / (1024 ** 3)
    memoria_livre = virtual_memory.available / (1024 ** 3)
    ram_percent = virtual_memory.percent
    disk_usage = psutil.disk_usage('/')
    local_ip = socket.gethostbyname(socket.gethostname())

    mac = get_mac_address().upper()
    if mac:
        mac = mac.replace(":", "-")

    partitions = psutil.disk_partitions()
    
    for partition in partitions:
        try:
            disk_usage = psutil.disk_usage(partition.mountpoint)

            disk_data = {
                "disco": partition.device,
                "espaco_total_GB": disk_usage.total / (1024 ** 3),
                "espaco_livre_GB": disk_usage.free / (1024 ** 3),
                "uso_percentual": disk_usage.percent
            }

            discos_info.append(disk_data)

        except PermissionError:
            continue

    return {
        'hostname': hostname,
        'ip_local': local_ip,
        'mac': mac,
        'cpu_info': cpu_info['brand_raw'],
        'cpu_percent': cpu_percent,
        'memoria_total': memoria_total,
        'memoria_livre': memoria_livre,
        'ram_percent': ram_percent,
        'partitions': discos_info
    }
