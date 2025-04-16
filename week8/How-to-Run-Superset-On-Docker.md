## How to Run Supserset via Docker Compose

This will only 2 containers so it uses far less memory. After making sure the Docker Desktop is up and running, open a terminal (or CMD in the case of Windows) and move to a folder of your choice.

1. Clone the sjsu-data226-SP25 repo to the folder
```
git clone https://github.com/keeyong/sjsu-data226-SP25.git
```
If you don't have git, you can just download it at https://github.com/keeyong/sjsu-data226-SP25/archive/refs/heads/main.zip. After unzipping it, you can follow the steps below

2. Change the current directory to sjsu-data226-SP25/week8
```
cd sjsu-data226-SP25/week8
```

3. Run Superset
```
docker compose up
```

4. Next initialize Superset environments
```
docker exec -it superset superset db upgrade
docker exec -it superset superset init
```

5. Create an admin account for log-in. It will prompt you to enter userid, first name, last name, email, and password . You can set any ID or Password you want (But remember them for the Web UI login). For the rest, you can just enter to take the default values.
```
docker exec -it superset superset fab create-admin
```
Here are examples of the prompts from the above command
```
Username [admin]: 
User first name [admin]: 
User last name [user]: 
Email [admin@fab.org]: 
Password: 
Repeat for confirmation:
```

6. Wait some time, then visit http://localhost:8088 and log in (Use ID:PW you set up previously). Now you can set up database connection first, then add a dataset and create any charts you want
