PIP should be configured with the following options:
```ini
[global]
index-url = https://packagemanager/pypi/latest/simple
trusted-host = packagemanager
```
This configuration will need to be in
C:\Users\[Your OPDID]\AppData\Roaming\pip.pip.ini   

Confirm pip config by running ```pip config list``` and (if needed) ```pip config debug```.   

This will configure pip to attempt to download packages from an internal site (packagemanager).   

Should this not work (due to server instability) pip can be overridden to pull packages from external sites:   
```ps1
pip install -r requirements.txt --upgrade pip --index-url https://pypi.org/simple --trusted-host pypi.org --trusted-host files.pythonhosted.org
```   
*WARNING*
The ```--trusted-host``` flag configures pip to not carry out any SSL checks. This is not ideal from a security point of view and exposes the laptop to man-in-the-middle attacks.