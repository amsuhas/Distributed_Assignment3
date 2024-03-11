create_network:
	docker network create my_network

dummy_image:
	docker build -f Dockerfile_dummy -t dummy_image .

server_image:
	docker build -f Dockerfile_server -t server_image .

lb_image:
	docker build -f Dockerfile_lb -t lb_image .

client_image:
	docker build -f Dockerfile_client -t client_image .

run_lb:
	docker run --privileged -dit --name load_balancer --hostname load_balancer -v /var/run/docker.sock:/var/run/docker.sock --network my_network lb_image 

run_client:
	docker run -dit --name client --hostname client --network my_network client_image
	docker exec -it client bash

run_server:
	docker run -dit --name server --hostname server --network my_network server_image

read_logs:
	docker cp load_balancer:/usr/src/app/logs lb_logs

run_dummy:
	docker run -dit --name dummy --hostname server0 --network my_network dummy_image
	

