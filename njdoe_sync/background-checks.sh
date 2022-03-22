cd ~/datarobot/datagun
./bin/qgtunnel ~/.pyenv/versions/datagun/bin/python ./datagun/extract.py --config ./datagun/config/njdoe.json

cd ~/datarobot/njdoe-sync
~/.pyenv/versions/njdoe-sync/bin/python ./njdoe_sync/background-checks.py
