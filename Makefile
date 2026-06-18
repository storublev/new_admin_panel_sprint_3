full-up:
	sudo docker-compose down -v && sudo docker-compose -f docker-compose.yml build --no-cache etl  && sudo docker-compose -f docker-compose.yml up -d

load-data:
	sudo docker-compose exec -it etl sh -c "python etl.py"

stop:
	sudo docker-compose stop

remove:
	sudo docker-compose down

remove-all:
	sudo docker-compose down -v
