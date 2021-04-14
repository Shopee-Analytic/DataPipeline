from datetime import datetime
from time import sleep

now = datetime.timestamp(datetime.now())
print(now)
sleep(1)
later = datetime.timestamp(datetime.now())
print(later)

print(f"calculated: {later-now}")
