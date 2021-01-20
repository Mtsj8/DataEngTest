# Data Engineering Test

## Attention

Fork this repository in order to start building your solution. Then when you're done, send us a link to your repository with the solution.

## Requirements

- Docker version >= 19.03
- Docker-compose version >= 1.25
- Python >= 3.7.3

Instruction on how to install [docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/).


## Firing up your Data Base

Fire up your PostgreSQL instance by opening a terminal and running the following command:

```bash
docker-compose up -d 
```

Using your prefered DB Client ([DBeaver](https://dbeaver.io/) for instace), you can connect to your PostgreSQL instance with the following credentials:

```
HOST: localhost
PASSWORD: 5342
USER: kzas
PASSWORD: kzas
DB: kzas
```

---
## The task ahead

The following diagram describe what you should do:

![Diagram](./diagram.jfif)

You have a folder (`raw_data`) that contains:

- JSON data from real estate ads,
- a CSV file with buildings. 

It's required that you write a script to identify for each ad, which building it belongs to. After successfully **identifying that an ad X belongs to building Y, load the enriched ad data into a PostgreSQL table (or set of tables).**
You should also provide the sql statements to create the table(s) into PostgreSQL database.


### Tips and things to keep in mind

- Organize your workflow in a way that you also provide instructions on how to run your scripts.
  #### Requirements
  - pandas: 1.2.0
  - numpy: 1.19.5
  - psycopg2: 2.8.6
  - geopy: 2.1.0
  - tqdm: 4.56.0
  - argparse: 1.1

  
  
  Get data: `bash get_data.sh`
  
  #### Run script:
  
    - If you have pipenv: `pipenv run python ads_buildings_pgsql.py`
    
    - If you don't have pipenv: `python ads_buildings_pgsql.py`
  
  
- Is your solution able to find a building for every ad? **Don't worry if it's 100%**, but can you explain why?
- What would you do **differently** if you had more time?
- Feel free to use any language you're comfortable with, but keep in mind that organization and comments are welcome to explain your train of thought.
