curl -fsSL https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list
sudo apt update
sudo apt install mongodb-org
sudo systemctl start mongod.service
sudo systemctl enable mongod
mongo
use hashTrend
db.createUser({user:"admin", pwd:"testtesttest", roles:[{role:"root", db:"admin"}]})
exit

git clone https://github.com/fquellec/hash-trend-server.git
cd hash-trend/
sudo apt-get update
sudo apt install redis-server
sudo apt install python3-pip
pip3 install -r requirements.txt

python3 worker.py & python3 api.py &



